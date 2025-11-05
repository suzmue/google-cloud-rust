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

use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::options::BatchingOptions;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Object that is passed to the worker task over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishHandle].
#[derive(Debug)]
pub(crate) struct BundledMessage {
    pub msg: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<crate::Result<String>>,
}

/// The worker is spawned in a background task and handles
/// batching and publishing all messages that are sent to the publisher.
#[derive(Debug)]
pub(crate) struct Worker {
    topic_name: String,
    client: GapicPublisher,
    #[allow(dead_code)]
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<BundledMessage>,
}

impl Worker {
    pub(crate) fn new(
        topic_name: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<BundledMessage>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
            batching_options,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut batch = Batch::new();
        let delay = self.batching_options.delay_threshold;
        let message_limit = self.batching_options.message_count_threshold;

        let timer = tokio::time::sleep(delay);
        // Pin the timer to the stack.
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // Handle timer events.
                // This branch will only be checked when there is a non-empty batch,
                // so this will not fire continuously.
                _ = &mut timer, if !batch.is_empty() => {
                    let batch_to_send = std::mem::take(&mut batch);
                    tokio::spawn(batch_to_send.send(self.client.clone(), self.topic_name.clone()));
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(msg) => {
                            // Reset the timer if this is the first message to be added to the batch.
                            if batch.is_empty() {
                                timer.as_mut().reset(tokio::time::Instant::now() + delay);
                            }
                            batch.push(msg);
                            if batch.len() as u32 >= message_limit {
                                let batch_to_send = std::mem::take(&mut batch);
                                // In the future, we may also want to keep track of JoinHandles in order to
                                // flush the results.
                                let _handle =
                                    tokio::spawn(batch_to_send.send(self.client.clone(), self.topic_name.clone()));
                            }
                        },
                        None => {
                            // The sender has been dropped send batch and stop running.
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishHandles for the batch and the program ends.
                            if !batch.is_empty() {
                                let _handle =
                                        tokio::spawn(batch.send(self.client.clone(), self.topic_name.clone()));
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Batch {
    // TODO(#3686): A batch should also keep track of its total size
    // for improved performance.
    batch: Vec<BundledMessage>,
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

impl Batch {
    fn new() -> Self {
        Batch { batch: Vec::new() }
    }

    fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    fn len(&self) -> usize {
        self.batch.len()
    }

    fn push(&mut self, msg: BundledMessage) {
        self.batch.push(msg);
    }

    /// Send the batch to the service and process the results.
    async fn send(self, client: GapicPublisher, topic: String) {
        let (msgs, txs): (Vec<_>, Vec<_>) =
            self.batch.into_iter().map(|msg| (msg.msg, msg.tx)).unzip();
        let request = client.publish().set_topic(topic).set_messages(msgs);

        // Handle the response by extracting the message ID on success.
        match request.send().await {
            Err(e) => {
                let e = Arc::new(e);
                txs.into_iter().for_each(move |tx| {
                    // The user may have dropped the handle, so it is ok if this fails.
                    // TODO(#3689): The error type for this is incorrect, will need to handle
                    // this error propagation more fully.
                    let _ = tx.send(Err(gax::error::Error::io(e.clone())));
                });
            }
            Ok(result) => {
                txs.into_iter()
                    .zip(result.message_ids.into_iter())
                    .for_each(|(tx, result)| {
                        // The user may have dropped the handle, so it is ok if this fails.
                        let _ = tx.send(Ok(result));
                    });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_bundled_message_helper(
        data: String,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<crate::Result<String>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            BundledMessage {
                tx,
                msg: PubsubMessage::new().set_data(data),
            },
            rx,
        )
    }

    #[tokio::test]
    async fn test_push_batch() {
        let mut batch = Batch::new();
        assert!(batch.is_empty());

        let (message_a, _rx_a) = create_bundled_message_helper("hello".to_string());
        batch.push(message_a);
        assert_eq!(batch.len(), 1);

        let (message_b, _rx_b) = create_bundled_message_helper(", ".to_string());
        batch.push(message_b);
        assert_eq!(batch.len(), 2);

        let (message_c, _rx_c) = create_bundled_message_helper("world".to_string());
        batch.push(message_c);
        assert_eq!(batch.len(), 3);
    }
}
