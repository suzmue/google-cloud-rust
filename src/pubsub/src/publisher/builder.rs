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

use super::client::Publisher;
use super::worker::Worker;
use crate::publisher::client::client_builder;

/// A builder for [Publisher].
pub struct PublisherBuilder {
    topic_name: Option<String>,
    // We will add batching/flow control settings here later.
    inner: gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>,
}

impl PublisherBuilder {
    pub(crate) fn new(
        inner: gax::client_builder::ClientBuilder<
            client_builder::Factory,
            gaxi::options::Credentials,
        >,
    ) -> Self {
        Self {
            topic_name: None,
            inner,
        }
    }

    /// Sets the full name of the topic to publish to.
    /// This is a required setting.
    /// e.g., `projects/my-project/topics/my-topic`.
    pub fn with_topic(mut self, topic_name: impl Into<String>) -> Self {
        self.topic_name = Some(topic_name.into());
        self
    }

    // TODO: Delegate other methods like with_credentials, with_endpoint, etc.

    /// Asynchronously builds a `Publisher`.
    pub async fn build(self) -> Result<Publisher, crate::Error> {
        let topic_name = self
            .topic_name
            .ok_or_else(|| crate::Error::service(Default::default()))?;

        // The inner builder will create a raw publisher client.
        let client = match self.inner.build().await {
            Ok(c) => c,
            Err(e) => return Err(crate::Error::io(e)),
        };

        // Create the channel for communicating with the background worker.
        let (tx, rx) = tokio::sync::mpsc::channel(100); // TODO: make buffer size configurable

        // Spawn the background worker.
        // TODO: spawn multiple workers based on configuration.
        let worker = Worker::new(topic_name, client, rx);
        tokio::spawn(worker.run());

        Ok(Publisher { sender: tx })
    }
}
