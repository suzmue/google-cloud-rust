// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicyExt};
use http::header::{HeaderName, HeaderValue};
use prost::Message as _;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Sleep;
use wkt::Timestamp;

use crate::error::PublishError;
use crate::generated::gapic_dataplane::builder::publisher::Publish as PublishRequestBuilder;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::google::pubsub::v1::PubsubClientTelemetry;
use crate::google::pubsub::v1::pubsub_client_telemetry::{self, PublishOperation};
use crate::model::PublishResponse;
use crate::publisher::batch::batch_resolve_publish_futures;
use crate::publisher::token_bucket::TokenBucket;

type BatchSenders = Vec<oneshot::Sender<Result<String, PublishError>>>;

/// Shared state for a single batch across initial and hedged RPC attempts.
pub(crate) struct BatchState {
    pub msgs: Arc<Vec<crate::model::Message>>,
    pub txs: Mutex<Option<BatchSenders>>,
    pub client: GapicPublisher,
    pub topic: String,
    pub token_bucket: Arc<TokenBucket>,
    pub delay: Duration,
    pub total_timeout: Option<Duration>,
    pub start_instant: tokio::time::Instant,
    pub start_time: Option<Timestamp>,
}

impl BatchState {
    pub(crate) fn new(
        msgs: Vec<crate::model::Message>,
        txs: BatchSenders,
        client: GapicPublisher,
        topic: String,
        token_bucket: Arc<TokenBucket>,
        delay: Duration,
        total_timeout: Option<Duration>,
    ) -> Self {
        Self {
            msgs: Arc::new(msgs),
            txs: Mutex::new(Some(txs)),
            client,
            topic,
            token_bucket,
            delay,
            total_timeout,
            start_instant: tokio::time::Instant::now(),
            start_time: wkt::Timestamp::try_from(std::time::SystemTime::now()).ok(),
        }
    }

    /// Returns true if this batch has already been resolved.
    pub(crate) fn is_done(&self) -> bool {
        self.txs.lock().unwrap().is_none()
    }

    /// Sends the initial publish attempt with standard retry policy.
    pub(crate) async fn send_initial(&self) -> crate::Result<()> {
        let request = self
            .client
            .publish()
            .set_topic(self.topic.clone())
            .set_messages((*self.msgs).clone());
        let res = request.send().await;
        self.complete_initial(res)
    }

    /// Completes the initial attempt.
    /// - If Ok, resolves txs and refills token bucket.
    /// - If Err (exhausted retries), resolves txs with Err.
    /// - If already resolved by a hedged attempt, does nothing.
    pub(crate) fn complete_initial(
        &self,
        res: crate::Result<PublishResponse>,
    ) -> crate::Result<()> {
        let mut lock = self.txs.lock().unwrap();
        if let Some(txs) = lock.take() {
            if res.is_ok() {
                self.token_bucket.refill();
            }
            batch_resolve_publish_futures(res, txs)
        } else {
            Ok(())
        }
    }

    /// Sends a hedged publish attempt with `x-goog-pubsub-client-telemetry` header and `NeverRetry`.
    /// The attempt timeout is clamped so that it does not exceed the remaining time
    /// of the initial request (clamped to at most 10 seconds).
    pub(crate) async fn send_hedged_rpc(&self, attempt_count: i32) {
        if self.is_done() {
            return;
        }

        let timeout = if let Some(total_timeout) = self.total_timeout {
            let elapsed = self.start_instant.elapsed();
            let remaining = total_timeout.saturating_sub(elapsed);
            if remaining.is_zero() {
                return;
            }
            remaining.min(Duration::from_secs(10))
        } else {
            Duration::from_secs(10)
        };

        let res = self
            .client
            .publish()
            .set_topic(self.topic.clone())
            .set_messages((*self.msgs).clone())
            .with_retry_policy(NeverRetry.with_time_limit(timeout))
            .set_pubsub_client_telemetry_header(attempt_count, self.start_time)
            .send()
            .await;

        if let Ok(resp) = res {
            let mut lock = self.txs.lock().unwrap();
            if let Some(txs) = lock.take() {
                self.token_bucket.refill();
                let _ = batch_resolve_publish_futures(Ok(resp), txs);
            }
        }
        // All errors from hedged requests are ignored.
    }
}

/// Encodes the internal `x-goog-pubsub-client-telemetry` header value for hedged publish attempts.
///
/// This header is used by Cloud Pub/Sub servers to associate hedged duplicates with their original
/// publish operation and track attempt count and original start time.
pub(crate) fn format_pubsub_client_telemetry_header(
    attempt_count: i32,
    start_time: Option<wkt::Timestamp>,
) -> Option<HeaderValue> {
    use base64::{Engine, prelude::BASE64_STANDARD};
    use gaxi::prost::ToProto;
    let p = PubsubClientTelemetry {
        operation: Some(pubsub_client_telemetry::Operation::PublishOperation(
            PublishOperation {
                hedged_attempt_count: attempt_count,
                publish_start_time: start_time.and_then(|t| t.to_proto().ok()),
            },
        )),
    };

    let encoded_proto = p.encode_to_vec();
    let b64_str = BASE64_STANDARD.encode(&encoded_proto);
    HeaderValue::try_from(b64_str).ok()
}

impl PublishRequestBuilder {
    fn set_pubsub_client_telemetry_header(
        self,
        attempt_count: i32,
        start_time: Option<wkt::Timestamp>,
    ) -> Self {
        if let Some(val) = format_pubsub_client_telemetry_header(attempt_count, start_time) {
            return self.with_custom_header(
                HeaderName::from_static("x-goog-pubsub-client-telemetry"),
                val,
            );
        }
        self
    }
}

/// An entry in the priority queue of scheduled hedges.
struct HedgeItem {
    state: Arc<BatchState>,
    deadline: tokio::time::Instant,
    attempt_count: i32,
}

impl PartialEq for HedgeItem {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Eq for HedgeItem {}

impl PartialOrd for HedgeItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HedgeItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order so BinaryHeap is a min-heap by deadline.
        other.deadline.cmp(&self.deadline)
    }
}

/// Handle held by ConcurrentBatchActor to submit batches to the dedicated scheduler task.
#[derive(Debug)]
pub(crate) struct HedgingSchedulerHandle {
    pub tx: mpsc::UnboundedSender<Arc<BatchState>>,
    pub token_bucket: Arc<TokenBucket>,
    pub delay: Duration,
    pub total_timeout: Option<Duration>,
}

impl HedgingSchedulerHandle {
    /// Dispatches a batch to the network and registers it with the hedging scheduler.
    pub(crate) fn dispatch(
        &self,
        msgs: Vec<crate::model::Message>,
        txs: Vec<oneshot::Sender<Result<String, PublishError>>>,
        client: GapicPublisher,
        topic: String,
        inflight: &mut JoinSet<crate::Result<()>>,
    ) {
        let state = Arc::new(BatchState::new(
            msgs,
            txs,
            client,
            topic,
            self.token_bucket.clone(),
            self.delay,
            self.total_timeout,
        ));
        let state_clone = state.clone();
        inflight.spawn(async move { state_clone.send_initial().await });
        let _ = self.tx.send(state);
    }
}

/// Dedicated background task that manages scheduled hedges using a priority queue.
pub(crate) struct HedgingScheduler {
    rx: mpsc::UnboundedReceiver<Arc<BatchState>>,
}

impl HedgingScheduler {
    pub(crate) fn spawn(
        token_bucket: Arc<TokenBucket>,
        delay: Duration,
        total_timeout: Option<Duration>,
    ) -> (HedgingSchedulerHandle, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let scheduler = Self { rx };
        let handle = tokio::spawn(scheduler.run());
        (
            HedgingSchedulerHandle {
                tx,
                token_bucket,
                delay,
                total_timeout,
            },
            handle,
        )
    }

    pub(crate) async fn run(mut self) {
        let mut queue: BinaryHeap<HedgeItem> = BinaryHeap::new();
        let mut timer: Option<Pin<Box<Sleep>>> = None;
        let mut inflight_hedges = JoinSet::new();

        loop {
            tokio::select! {
                // Clean up finished hedged RPC tasks
                _ = inflight_hedges.join_next(), if !inflight_hedges.is_empty() => {
                    continue;
                }
                // Receive new batches to hedge
                item = self.rx.recv() => {
                    match item {
                        Some(state) => {
                            let deadline = tokio::time::Instant::now() + state.delay;
                            queue.push(HedgeItem { state, deadline, attempt_count: 1 });
                            if timer.as_ref().is_none_or(|t| deadline < t.deadline()) {
                                timer = Some(Box::pin(tokio::time::sleep_until(deadline)));
                            }
                        }
                        None => {
                            // Actor dropped sender. Prune resolved batches immediately.
                            queue.retain(|item| !item.state.is_done());
                            if queue.is_empty() && inflight_hedges.is_empty() {
                                break;
                            }
                            timer = queue.peek().map(|item| Box::pin(tokio::time::sleep_until(item.deadline)));
                        }
                    }
                }
                // Earliest scheduled hedge deadline expired
                _ = async { timer.as_mut().unwrap().await }, if timer.is_some() => {
                    let now = tokio::time::Instant::now();
                    while let Some(item) = queue.peek() {
                        if item.deadline <= now {
                            let item = queue.pop().unwrap();
                            if !item.state.is_done() {
                                if item.state.token_bucket.try_acquire() {
                                    let state = item.state.clone();
                                    let attempt_count = item.attempt_count;
                                    inflight_hedges.spawn(async move {
                                        state.send_hedged_rpc(attempt_count).await;
                                    });
                                }
                                // Reschedule with next deadline in case this attempt is also slow
                                let next_deadline = now + item.state.delay;
                                queue.push(HedgeItem {
                                    state: item.state,
                                    deadline: next_deadline,
                                    attempt_count: item.attempt_count + 1,
                                });
                            }
                        } else {
                            break;
                        }
                    }
                    if self.rx.is_closed() {
                        queue.retain(|item| !item.state.is_done());
                        if queue.is_empty() && inflight_hedges.is_empty() {
                            break;
                        }
                    }
                    timer = queue.peek().map(|item| Box::pin(tokio::time::sleep_until(item.deadline)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Message;
    use google_cloud_gax::options::internal::RequestOptionsExt;
    use google_cloud_test_macros::tokio_test_no_panics;

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: crate::RequestOptions) -> crate::Result<crate::Response<crate::model::PublishResponse>>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisherWithFuture {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisherWithFuture {
            fn publish(&self, req: crate::model::PublishRequest, _options: google_cloud_gax::options::RequestOptions) -> impl Future<Output=google_cloud_gax::Result<google_cloud_gax::response::Response<crate::model::PublishResponse>>> + Send;
        }
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_initial_succeeds_fast() -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, options| {
                let headers = options.get_extension::<http::HeaderMap>();
                let is_hedged = headers
                    .and_then(|h| h.get("x-goog-pubsub-client-telemetry"))
                    .is_some();
                r.topic == "topic" && !is_hedged
            })
            .return_once(|_, _| {
                Ok(crate::Response::from(
                    PublishResponse::new().set_message_ids(["msg-initial".to_string()]),
                ))
            });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket.clone(),
            Duration::from_millis(100),
            Some(Duration::from_secs(60)),
        ));

        state.send_initial().await?;
        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert_eq!(token_bucket.available_scaled_tokens(), 100);
        assert!(state.is_done());

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_hedged_succeeds_when_initial_hangs() -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs indefinitely
        mock.expect_publish()
            .withf(|r, options| {
                let headers = options.get_extension::<http::HeaderMap>();
                let is_hedged = headers
                    .and_then(|h| h.get("x-goog-pubsub-client-telemetry"))
                    .is_some();
                r.topic == "topic" && !is_hedged
            })
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok(crate::Response::from(
                        PublishResponse::new().set_message_ids(["msg-initial".to_string()]),
                    ))
                })
            });

        // Hedged attempt succeeds quickly
        mock.expect_publish()
            .withf(|r, options| {
                let headers = options.get_extension::<http::HeaderMap>();
                let telemetry_valid = headers
                    .and_then(|h| h.get("x-goog-pubsub-client-telemetry"))
                    .is_some_and(|val| {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD
                            .decode(val.as_bytes())
                            .ok()
                            .and_then(|bytes| PubsubClientTelemetry::decode(&bytes[..]).ok())
                            .and_then(|proto| proto.operation)
                            .is_some_and(|op| match op {
                                pubsub_client_telemetry::Operation::PublishOperation(p) => {
                                    p.hedged_attempt_count == 1 && p.publish_start_time.is_some()
                                }
                            })
                    });
                r.topic == "topic" && telemetry_valid
            })
            .returning(|_, _| {
                Box::pin(async {
                    Ok(crate::Response::from(
                        PublishResponse::new().set_message_ids(["msg-hedged".to_string()]),
                    ))
                })
            });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        // Refill 10 times (10 * 100 = 1000 units = 1 full token) so a hedge can be acquired
        for _ in 0..10 {
            token_bucket.refill();
        }
        let (scheduler_handle, _task) = HedgingScheduler::spawn(
            token_bucket.clone(),
            Duration::from_millis(100),
            Some(Duration::from_secs(60)),
        );

        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket.clone(),
            Duration::from_millis(100),
            Some(Duration::from_secs(60)),
        ));

        let state_clone = state.clone();
        tokio::spawn(async move {
            let _ = state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // Advance time by 150ms to trigger hedge
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged");
        assert!(state.is_done());
        // Had 1000 units, acquired 1000 (0), refilled 100 on success -> 100 scaled units
        assert_eq!(token_bucket.available_scaled_tokens(), 100);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_initial_fails_option_1() -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once(|_, _| {
            Err(crate::Error::io(std::io::Error::other(
                "fatal network error",
            )))
        });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket.clone(),
            Duration::from_millis(100),
            Some(Duration::from_secs(60)),
        ));

        let res = state.send_initial().await;
        assert!(res.is_err());
        let publish_res = rx.await?;
        assert!(publish_res.is_err());
        assert!(state.is_done());

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_hedged_rpc_not_sent_when_total_timeout_expired() -> anyhow::Result<()> {
        let (tx, _rx) = oneshot::channel();
        let mock = MockGapicPublisher::new();
        // Expect NO publish call because total_timeout expired before hedge
        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket,
            Duration::from_millis(100),
            Some(Duration::from_millis(50)),
        ));

        // Advance time past total timeout
        tokio::time::advance(Duration::from_millis(100)).await;
        state.send_hedged_rpc(1).await;
        // Batch remains unresolved (no hedged RPC sent)
        assert!(!state.is_done());

        Ok(())
    }

    #[test]
    fn test_format_telemetry_header() {
        use base64::Engine as _;
        use prost::Message as _;

        let start_time = wkt::Timestamp::clamp(1_700_000_000, 500_000_000);
        let header_val = format_pubsub_client_telemetry_header(2, Some(start_time))
            .expect("header value should be generated");

        let decoded_bytes = base64::engine::general_purpose::STANDARD
            .decode(header_val.as_bytes())
            .expect("should be valid base64");

        let telemetry = PubsubClientTelemetry::decode(&decoded_bytes[..])
            .expect("should decode into PubsubClientTelemetry");

        match telemetry.operation {
            Some(pubsub_client_telemetry::Operation::PublishOperation(op)) => {
                assert_eq!(op.hedged_attempt_count, 2);
                let ts = op.publish_start_time.expect("timestamp should be present");
                assert_eq!(ts.seconds, 1_700_000_000);
                assert_eq!(ts.nanos, 500_000_000);
            }
            _ => panic!("unexpected operation in telemetry proto"),
        }
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_scheduler_shuts_down_promptly_when_batch_done() -> anyhow::Result<()> {
        let (tx, _rx) = oneshot::channel();
        let mock = MockGapicPublisher::new();
        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));

        let (scheduler_handle, task) = HedgingScheduler::spawn(
            token_bucket.clone(),
            Duration::from_secs(10),
            Some(Duration::from_secs(60)),
        );

        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket,
            Duration::from_secs(10),
            Some(Duration::from_secs(60)),
        ));

        // Mark batch done immediately
        let _ = state.txs.lock().unwrap().take();
        assert!(state.is_done());

        scheduler_handle.tx.send(state)?;
        // Drop sender to signal shutdown
        drop(scheduler_handle);

        // Task should finish immediately without sleeping for 10s
        tokio::time::timeout(Duration::from_millis(100), task).await??;

        Ok(())
    }
}
