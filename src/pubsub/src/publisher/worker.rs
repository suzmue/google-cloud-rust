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

use super::client::ToWorker;
use crate::generated::gapic_dataplane;
use tokio::sync::mpsc;

pub(crate) struct Worker {
    topic_name: String,
    client: gapic_dataplane::client::Publisher,
    rx: mpsc::Receiver<ToWorker>,
}

impl Worker {
    pub(crate) fn new(
        topic_name: String,
        client: gapic_dataplane::client::Publisher,
        rx: mpsc::Receiver<ToWorker>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            let request = self
                .client
                .publish()
                .set_topic(self.topic_name.clone())
                .set_messages(vec![msg.message]);

            let result = request.send().await.map(|response| {
                // For now, assume one message response. Batching will handle multiple.
                response.message_ids.get(0).cloned().unwrap_or_default()
            });

            // The user may have dropped the handle, so we don't care if this fails.
            let _ = msg.tx.send(result);
        }
    }
}
