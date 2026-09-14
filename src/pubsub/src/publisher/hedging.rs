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
use tokio_util::sync::CancellationToken;
use wkt::Timestamp;

use super::actor::batch_resolve_publish_futures;
use super::options::HedgingOptions;
use super::token_bucket::TokenBucket;
use crate::error::PublishError;
use crate::generated::gapic_dataplane::{
    builder::publisher::Publish as PublishRequestBuilder, client::Publisher as GapicPublisher,
};
use crate::google::pubsub::v1::{
    PubsubClientTelemetry,
    pubsub_client_telemetry::{Operation, PublishOperation},
};
use crate::model::PublishResponse;

type BatchSenders = Vec<oneshot::Sender<Result<String, PublishError>>>;

/// Shared state for a single batch across initial and hedged RPC attempts.
pub(crate) struct BatchState {
    pub msgs: Arc<Vec<crate::model::Message>>,
    pub txs: Mutex<
        Option<(
            BatchSenders,
            tokio::sync::oneshot::Sender<crate::Result<()>>,
        )>,
    >,
    pub client: GapicPublisher,
    pub topic: String,
    pub token_bucket: Arc<TokenBucket>,
    pub delay: Duration,
    pub start_time: Option<Timestamp>,
    pub cancel_token: CancellationToken,
}

impl BatchState {
    pub(crate) fn new(
        msgs: Vec<crate::model::Message>,
        txs: BatchSenders,
        client: GapicPublisher,
        topic: String,
        token_bucket: Arc<TokenBucket>,
        delay: Duration,
        done_tx: tokio::sync::oneshot::Sender<crate::Result<()>>,
    ) -> Self {
        Self {
            msgs: Arc::new(msgs),
            txs: Mutex::new(Some((txs, done_tx))),
            client,
            topic,
            token_bucket,
            delay,
            start_time: wkt::Timestamp::try_from(std::time::SystemTime::now()).ok(),
            cancel_token: CancellationToken::new(),
        }
    }

    /// Returns true if this batch has already been resolved.
    pub(crate) fn is_done(&self) -> bool {
        self.txs.lock().unwrap().is_none()
    }

    /// Sends the initial publish attempt with standard retry policy.
    pub(crate) async fn send_initial(&self) {
        let request = self
            .client
            .publish()
            .set_topic(self.topic.clone())
            .set_messages((*self.msgs).clone());

        tokio::select! {
            _ = self.cancel_token.cancelled() => {}
            res = request.send() => {
                self.complete(res);
            }
        }
    }

    /// Sends a hedged publish attempt with `x-goog-pubsub-client-telemetry` header and `NeverRetry`.
    pub(crate) async fn send_hedged_rpc(&self, attempt_count: i32) {
        if self.is_done() {
            return;
        }

        // TODO(#6776): clamp the timeout to the remaining time of the initial request.
        let timeout = Duration::from_secs(10);

        let request = self
            .client
            .publish()
            .set_topic(self.topic.clone())
            .set_messages((*self.msgs).clone())
            .with_retry_policy(NeverRetry.with_time_limit(timeout))
            .set_pubsub_client_telemetry_header(attempt_count, self.start_time);

        tokio::select! {
            _ = self.cancel_token.cancelled() => {}
            res = request.send() => {
                if let Ok(resp) = res {
                    self.complete(Ok(resp));
                }
            }
        }
    }

    /// Completes the attempt.
    /// - If Ok, refills token bucket.
    /// - Attempts to resolve the txs with the result.
    /// - If already resolved by another attempt, does nothing.
    fn complete(&self, resp: crate::Result<PublishResponse>) {
        let mut lock = self.txs.lock().unwrap();
        if let Some((txs, done_tx)) = lock.take() {
            self.cancel_token.cancel();
            if resp.is_ok() {
                self.token_bucket.refill();
            }
            let _ = done_tx.send(batch_resolve_publish_futures(resp, txs));
        }
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
        operation: Some(Operation::PublishOperation(PublishOperation {
            hedged_attempt_count: attempt_count,
            publish_start_time: start_time.and_then(|t| t.to_proto().ok()),
        })),
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
#[derive(Debug, Clone)]
pub(crate) struct HedgingSchedulerHandle {
    pub tx: mpsc::UnboundedSender<Arc<BatchState>>,
    pub token_bucket: Arc<TokenBucket>,
    pub delay: Duration,
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
        let (done_tx, done_rx) = oneshot::channel();
        let state = Arc::new(BatchState::new(
            msgs,
            txs,
            client,
            topic,
            self.token_bucket.clone(),
            self.delay,
            done_tx,
        ));
        inflight.spawn(async move {
            let res = done_rx.await.map_err(crate::Error::io)?; // forward errors if the oneshot was dropped.
            res
        });
        // Send the initial RPC immediatiely instead of sending over the channel.
        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        let _ = self.tx.send(state);
    }
}

/// Dedicated background task that manages scheduled hedges using a priority queue.
pub(crate) struct HedgingScheduler {
    rx: mpsc::UnboundedReceiver<Arc<BatchState>>,
}

impl HedgingScheduler {
    pub(crate) fn spawn(opts: HedgingOptions) -> HedgingSchedulerHandle {
        let token_bucket = Arc::new(TokenBucket::new(opts.max_tokens, opts.refill_ratio));
        let (tx, rx) = mpsc::unbounded_channel();
        let scheduler = Self { rx };
        tokio::spawn(scheduler.run());
        HedgingSchedulerHandle {
            tx,
            token_bucket,
            delay: opts.delay,
        }
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
                            // Actor dropped sender.
                            break;
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
                                let next_attempt = if item.state.token_bucket.try_acquire() {
                                    let state = item.state.clone();
                                    let attempt_count = item.attempt_count;
                                    inflight_hedges.spawn(async move {
                                        state.send_hedged_rpc(attempt_count).await;
                                    });
                                    item.attempt_count + 1
                                } else {
                                    item.attempt_count
                                };
                                queue.push(HedgeItem {
                                    deadline: now + item.state.delay,
                                    state: item.state,
                                    attempt_count: next_attempt,
                                });
                            }
                        } else {
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

    fn is_hedged_request(options: &google_cloud_gax::options::RequestOptions) -> bool {
        options
            .get_extension::<http::HeaderMap>()
            .and_then(|h| h.get("x-goog-pubsub-client-telemetry"))
            .is_some()
    }

    fn is_initial_request(options: &google_cloud_gax::options::RequestOptions) -> bool {
        !is_hedged_request(options)
    }

    fn mock_publish_response(msg_id: &str) -> crate::Result<crate::Response<PublishResponse>> {
        Ok(crate::Response::from(
            PublishResponse::new().set_message_ids([msg_id.to_string()]),
        ))
    }

    #[allow(clippy::type_complexity)]
    fn test_batch_state(
        client: GapicPublisher,
        token_bucket: Arc<TokenBucket>,
        delay: Duration,
    ) -> (
        Arc<BatchState>,
        oneshot::Receiver<Result<String, PublishError>>,
        oneshot::Receiver<crate::Result<()>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket,
            delay,
            done_tx,
        ));
        (state, rx, done_rx)
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_initial_succeeds_fast() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .return_once(|_, _| mock_publish_response("msg-initial"));

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

        state.send_initial().await;
        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_hedged_succeeds_when_initial_hangs() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs indefinitely
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    mock_publish_response("msg-initial")
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
                                Operation::PublishOperation(p) => {
                                    p.hedged_attempt_count == 1 && p.publish_start_time.is_some()
                                }
                            })
                    });
                r.topic == "topic" && telemetry_valid
            })
            .returning(|_, _| Box::pin(async { mock_publish_response("msg-hedged") }));

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        // Refill 10 times (10 * 100 = 1000 units = 1 full token) so a hedge can be acquired
        for _ in 0..10 {
            token_bucket.refill();
        }

        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

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
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_initial_fails_option_1() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once(|_, _| {
            Err(crate::Error::io(std::io::Error::other(
                "fatal network error",
            )))
        });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

        state.send_initial().await;
        let publish_res = rx.await?;
        assert!(publish_res.is_err());
        assert!(state.is_done());
        let done_res = done_rx.await?;
        assert!(done_res.is_err());

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
            Some(Operation::PublishOperation(op)) => {
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
        let mock = MockGapicPublisher::new();
        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (tx, rx) = mpsc::unbounded_channel();
        let scheduler = HedgingScheduler { rx };
        let task = tokio::spawn(scheduler.run());

        let (state, _rx, done_rx) = test_batch_state(client, token_bucket, Duration::from_secs(10));

        // Mark batch done immediately
        state.complete(Ok(
            PublishResponse::new().set_message_ids(["msg".to_string()])
        ));
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        tx.send(state)?;
        // Drop sender to signal shutdown
        drop(tx);

        // Task should finish immediately without sleeping for 10s
        tokio::time::timeout(Duration::from_millis(100), task).await??;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_cancellation_when_initial_finishes_first() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial succeeds after 50ms
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    mock_publish_response("msg-initial")
                })
            });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

        assert!(!state.cancel_token.is_cancelled());
        state.send_initial().await;
        assert!(state.cancel_token.is_cancelled());

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_cancellation_when_hedged_finishes_first() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs for 10s
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    mock_publish_response("msg-initial")
                })
            });

        // Hedged succeeds after 20ms
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_hedged_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    mock_publish_response("msg-hedged")
                })
            });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

        let initial_handle = tokio::spawn({
            let state = state.clone();
            async move {
                state.send_initial().await;
            }
        });

        let hedged_handle = tokio::spawn({
            let state = state.clone();
            async move {
                state.send_hedged_rpc(1).await;
            }
        });

        let _ = hedged_handle.await;
        // Upon hedged completion, cancel_token was cancelled so initial finishes quickly
        assert!(state.cancel_token.is_cancelled());
        initial_handle.await?;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged");
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_multiple_hedged_attempts() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs for 10s
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    mock_publish_response("msg-initial")
                })
            });

        // 1st hedged attempt (attempt 1) at t = 100ms hangs
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
                                Operation::PublishOperation(p) => p.hedged_attempt_count == 1,
                            })
                    });
                r.topic == "topic" && telemetry_valid
            })
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    mock_publish_response("msg-hedged-1")
                })
            });

        // 2nd hedged attempt (attempt 2) at t = 200ms succeeds immediately
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
                                Operation::PublishOperation(p) => p.hedged_attempt_count == 2,
                            })
                    });
                r.topic == "topic" && telemetry_valid
            })
            .returning(|_, _| Box::pin(async { mock_publish_response("msg-hedged-2") }));

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        // Refill 20 times (2 tokens) so two hedges can be acquired
        for _ in 0..20 {
            token_bucket.refill();
        }

        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // Advance time to 250ms to trigger both hedges
        tokio::time::advance(Duration::from_millis(250)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged-2");
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_throttled_hedge_reschedules_and_maintains_attempt_count() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs for 10s
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    mock_publish_response("msg-initial")
                })
            });

        // Hedged attempt dispatched with attempt count 1
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
                                Operation::PublishOperation(p) => p.hedged_attempt_count == 1,
                            })
                    });
                r.topic == "topic" && telemetry_valid
            })
            .returning(|_, _| Box::pin(async { mock_publish_response("msg-hedged-1") }));

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        // Token bucket starts empty (0 tokens)

        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket.clone(), Duration::from_millis(100));

        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // At t = 100ms: scheduler ticks, throttled (no tokens), rescheduled for t = 200ms
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(!state.is_done());

        // At t = 150ms: refill token bucket (10 refills of 0.1 ratio = 1 token)
        tokio::time::advance(Duration::from_millis(50)).await;
        for _ in 0..10 {
            token_bucket.refill();
        }

        // Advance to t = 250ms (second tick at 200ms fires and hedged RPC completes)
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged-1");
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_failed_hedged_attempt_ignored() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial attempt returns success after 200ms
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_initial_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    mock_publish_response("msg-initial")
                })
            });

        // Hedged attempt at t = 100ms fails immediately
        mock.expect_publish()
            .withf(|r, options| r.topic == "topic" && is_hedged_request(options))
            .returning(|_, _| {
                Box::pin(async {
                    Err(crate::Error::io(std::io::Error::other("hedged rpc failed")))
                })
            });

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        for _ in 0..10 {
            token_bucket.refill();
        }

        let (state, rx, done_rx) =
            test_batch_state(client, token_bucket, Duration::from_millis(100));

        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // Advance time to 250ms (hedged fails at 100ms, initial succeeds at 200ms)
        tokio::time::advance(Duration::from_millis(250)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.is_done());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }
}
