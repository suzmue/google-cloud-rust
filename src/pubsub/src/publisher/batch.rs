// Copyright 2025 Google LLC
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

use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicyExt};
use tokio::task::JoinSet;

use super::options::{BatchingOptions, HedgingOptions};
use super::token_bucket::TokenBucket;
use crate::error::PublishError;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::model::PublishResponse;
use crate::publisher::actor::BundledMessage;
use google_cloud_gax::options::RequestOptionsBuilder;
use http::header::{HeaderName, HeaderValue};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Default)]
pub(crate) struct Batch {
    messages: Vec<BundledMessage>,
    initial_size: u32,
    messages_byte_size: u32,
    batching_options: BatchingOptions,
}

impl Batch {
    pub(crate) fn new(initial_size: u32, batching_options: BatchingOptions) -> Self {
        Batch {
            initial_size,
            messages_byte_size: initial_size,
            batching_options,
            ..Batch::default()
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.messages.len()
    }

    pub(crate) fn size(&self) -> u32 {
        self.messages_byte_size
    }

    pub(crate) fn push(&mut self, msg: BundledMessage) {
        self.messages_byte_size += Self::message_size(&msg.msg) as u32;
        self.messages.push(msg);
    }

    fn message_size(msg: &crate::model::Message) -> usize {
        // This is only an estimate and not the wire length.
        // TODO(#3963): If we move on to use protobuf crate, then it may be
        // possible to use compute_size to find the wire length.
        msg.attributes
            .iter()
            .fold(msg.data.len() + msg.ordering_key.len(), |acc, (k, v)| {
                acc + k.len() + v.len()
            })
    }

    pub(crate) fn at_threshold(&mut self) -> bool {
        self.len() as u32 >= self.batching_options.message_count_threshold
            || self.size() >= self.batching_options.byte_threshold
    }

    // Return true if adding the next message is within the byte threshold.
    pub(crate) fn can_add(&mut self, next: &BundledMessage) -> bool {
        self.size() + Self::message_size(&next.msg) as u32 <= self.batching_options.byte_threshold
    }

    /// Drains the batch and spawns a task to send the messages.
    ///
    /// This method mutably drains the messages from the current batch, leaving it
    /// empty, and returns a `JoinHandle` for the spawned send operation. This allows
    /// the `Worker` to immediately begin creating a new batch while the old one is
    /// being sent in the background.
    pub(crate) fn flush(
        &mut self,
        client: GapicPublisher,
        topic: String,
        inflight: &mut JoinSet<crate::Result<()>>,
        hedging: Option<(HedgingOptions, Arc<TokenBucket>)>,
    ) {
        let batch_to_send = Self {
            initial_size: self.initial_size,
            messages: std::mem::take(&mut self.messages),
            messages_byte_size: self.messages_byte_size,
            batching_options: self.batching_options.clone(),
        };
        self.messages_byte_size = self.initial_size;
        if let Some((hedging_options, token_bucket)) = hedging {
            inflight.spawn(batch_to_send.send_hedged(client, topic, hedging_options, token_bucket));
        } else {
            inflight.spawn(batch_to_send.send(client, topic));
        }
    }

    /// Send the batch to the service and process the results.
    async fn send(self, client: GapicPublisher, topic: String) -> crate::Result<()> {
        let (msgs, txs): (Vec<_>, Vec<_>) = self
            .messages
            .into_iter()
            .map(|msg| (msg.msg, msg.tx))
            .unzip();
        let request = client.publish().set_topic(topic).set_messages(msgs);

        // Handle the response by extracting the message ID on success.
        handle_response(request.send().await, txs).await
    }

    /// Send the batch to the service with hedging and process the results.
    async fn send_hedged(
        self,
        client: GapicPublisher,
        topic: String,
        hedging_options: HedgingOptions,
        token_bucket: Arc<TokenBucket>,
    ) -> crate::Result<()> {
        let (msgs, txs): (Vec<_>, Vec<_>) = self
            .messages
            .into_iter()
            .map(|msg| (msg.msg, msg.tx))
            .unzip();

        // Loop:
        // 1. send request
        // 2. wait for delay or a request completion
        // 3. process request completion
        // - (if success, cancel others)
        // - (if hedged and non-transient, discard)
        // - (if original and error (after all retries), return the error)
        // 4. If delay passed, hedge request
        // - hedged requests are not retried
        // - timeout of hedged requests is total_timeout - now (capped at 10s)

        let delay = tokio::time::sleep(hedging_options.delay);
        tokio::pin!(delay);

        let client_clone = client.clone();
        let topic_clone = topic.clone();
        let msgs_clone = msgs.clone();
        let initial_attempt = async move {
            client_clone
                .publish()
                .set_topic(topic_clone)
                .set_messages(msgs_clone)
                .send()
                .await
        };
        tokio::pin!(initial_attempt);

        let mut hedged: JoinSet<crate::Result<PublishResponse>> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                initial = &mut initial_attempt => {
                    if initial.is_ok() {
                        token_bucket.refill();
                    }
                    return handle_response(initial, txs).await;
                },
                hedged = hedged.join_next(), if !hedged.is_empty() => {
                    if let Some(Ok(Ok(resp))) = hedged {
                        token_bucket.refill();
                        return handle_response(Ok(resp), txs).await
                    }
                    // Discard all other errors and continue.
                },
                _ = &mut delay => {
                    // Delay expired before any attempts completed.
                    let new_deadline = tokio::time::Instant::now() + hedging_options.delay;
                    delay.as_mut().reset(new_deadline);

                    if !token_bucket.try_acquire() {
                        continue;
                    }

                    // TODO: set delay
                    // TODO: add serialized metrics to the headers.
                    let client_clone = client.clone();
                    let topic_clone = topic.clone();
                    let msgs_clone = msgs.clone();
                    hedged.spawn(async move {
                        client_clone
                            .publish()
                            .set_topic(topic_clone)
                            .set_messages(msgs_clone)
                            .with_custom_header(
                                HeaderName::from_static("x-goog-pubsub-hedged"),
                                HeaderValue::from_static("true"),
                            )
                            .with_retry_policy(NeverRetry.with_time_limit(Duration::from_secs(10)))
                            .send()
                            .await
                    });
                }
            }
        }
    }
}

async fn handle_response(
    resp: crate::Result<PublishResponse>,
    txs: Vec<tokio::sync::oneshot::Sender<Result<String, PublishError>>>,
) -> crate::Result<()> {
    match resp {
        Err(e) => {
            // TODO(#4013): To support message ordering retry, we need to correctly handle
            // the send error here with either retry or propagate to the user.
            let e = Arc::new(e);
            for tx in txs {
                // The user may have dropped the handle, so it is ok if this fails.
                let _ = tx.send(Err(PublishError::Rpc(e.clone())));
            }
            Err(crate::Error::io(e))
        }
        Ok(result) => {
            txs.into_iter()
                .zip(result.message_ids)
                .for_each(|(tx, result)| {
                    // The user may have dropped the handle, so it is ok if this fails.
                    let _ = tx.send(Ok(result));
                });
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{Message, PublishResponse},
        publisher::actor::BundledMessage,
        publisher::batch::{Batch, BatchingOptions},
    };
    use google_cloud_test_macros::tokio_test_no_panics;
    use tokio::task::JoinSet;

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: crate::RequestOptions) -> crate::Result<crate::Response<crate::model::PublishResponse>>;
        }
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_push_and_flush_batch() -> anyhow::Result<()> {
        let mut batch = Batch::new("topic".len() as u32, BatchingOptions::default());
        assert!(batch.is_empty());

        let (message_a, _rx_a) = create_bundled_message_from_bytes("hello");
        batch.push(message_a);
        assert_eq!(batch.len(), 1);

        let (message_b, _rx_b) = create_bundled_message_from_bytes(", ");
        batch.push(message_b);
        assert_eq!(batch.len(), 2);

        let (message_c, _rx_c) = create_bundled_message_from_bytes("world");
        batch.push(message_c);
        assert_eq!(batch.len(), 3);

        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| r.topic == "topic" && r.messages.len() == 3)
            .return_once(|_, _| Ok(crate::Response::from(PublishResponse::new())));
        let client = GapicPublisher::from_stub(mock);
        let mut inflight = JoinSet::new();
        batch.flush(client, "topic".to_string(), &mut inflight, None);
        assert_eq!(batch.len(), 0);
        inflight.join_all().await;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_size() -> anyhow::Result<()> {
        use std::collections::HashMap;

        let topic = "topic";
        let mut batch: Batch = Batch::new(topic.len() as u32, BatchingOptions::default());
        let mut expected_encoded_len = topic.len();
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (message_data_only, _rx) = create_bundled_message_from_bytes("message_data_only");
        expected_encoded_len += message_data_only.msg.data.len();
        batch.push(message_data_only);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let (message_with_ordering, _rx) = create_bundled_message_from_pubsub_message(
            Message::new().set_ordering_key("ordering_key"),
        );
        expected_encoded_len += message_with_ordering.msg.ordering_key.len();
        batch.push(message_with_ordering);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let attributes = HashMap::from([("k1", "v1"), ("key2", "value2")]);
        let (message_with_attributes, _rx) =
            create_bundled_message_from_pubsub_message(Message::new().set_attributes(attributes));
        expected_encoded_len += 14;
        batch.push(message_with_attributes);
        assert_eq!(batch.size(), expected_encoded_len as u32);

        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| r.topic == "topic" && r.messages.len() == 3)
            .return_once(|_, _| Ok(crate::Response::from(PublishResponse::new())));
        let client = GapicPublisher::from_stub(mock);
        let mut inflight = JoinSet::new();
        batch.flush(client, "topic".to_string(), &mut inflight, None);
        assert_eq!(batch.size(), "topic".len() as u32);
        inflight.join_all().await;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_hedged_flush_attempt0_succeeds_fast() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;
        use crate::publisher::token_bucket::TokenBucket;
        use google_cloud_gax::options::internal::RequestOptionsExt;
        use std::sync::Arc;
        use std::time::Duration;

        let mut batch = Batch::new("topic".len() as u32, BatchingOptions::default());
        let (message, rx) = create_bundled_message_from_bytes("fast");
        batch.push(message);

        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, options| {
                let headers = options.get_extension::<http::HeaderMap>();
                let is_hedged = headers
                    .and_then(|h| h.get("x-goog-pubsub-hedged"))
                    .is_some();
                r.topic == "topic" && !is_hedged
            })
            .return_once(|_, _| {
                Ok(crate::Response::from(
                    PublishResponse::new().set_message_ids(["msg-1".to_string()]),
                ))
            });

        let client = GapicPublisher::from_stub(mock);
        let mut inflight = JoinSet::new();
        let hedging_opts = HedgingOptions::new().set_delay(Duration::from_millis(500));
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));

        batch.flush(
            client,
            "topic".to_string(),
            &mut inflight,
            Some((hedging_opts, token_bucket.clone())),
        );
        inflight.join_all().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-1");
        // Refill on success: 100 scaled units
        assert_eq!(token_bucket.available_scaled_tokens(), 100);

        Ok(())
    }

    fn create_bundled_message_from_bytes<T: Into<::bytes::Bytes>>(
        data: T,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<std::result::Result<String, crate::error::PublishError>>,
    ) {
        create_bundled_message_from_pubsub_message(Message::new().set_data(data.into()))
    }

    fn create_bundled_message_from_pubsub_message(
        msg: Message,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<std::result::Result<String, crate::error::PublishError>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (BundledMessage { tx, msg }, rx)
    }
}
