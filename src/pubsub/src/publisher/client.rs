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

use crate::publisher::{builder::PublisherBuilder, handle::PublishHandle};
use tokio::sync::{mpsc, oneshot};

// A message to be sent to the background worker.
#[derive(Debug)]
pub(crate) struct ToWorker {
    pub message: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<Result<String, crate::Error>>,
}

/// Client for publishing messages to Pub/Sub topics.
#[derive(Clone, Debug)]
pub struct Publisher {
    pub(crate) sender: mpsc::Sender<ToWorker>,
}

pub(crate) mod client_builder {
    use crate::generated::gapic_dataplane;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = gapic_dataplane::client::Publisher;
        type Credentials = gaxi::options::Credentials;
        #[allow(unused_mut)]
        async fn build(
            self,
            mut config: gaxi::options::ClientConfig,
        ) -> gax::client_builder::Result<Self::Client> {
            // TODO(#3019): Pubsub default retry policy goes here.
            Self::Client::new(config).await
        }
    }
}

impl Publisher {
    /// Returns a builder for [Publisher].
    pub fn builder() -> PublisherBuilder {
        PublisherBuilder::new(gax::client_builder::internal::new_builder(
            client_builder::Factory,
        ))
    }

    /// Queues a message for publishing and returns a handle to the operation.
    pub async fn publish(
        &self,
        message: crate::model::PubsubMessage,
    ) -> Result<PublishHandle, crate::Error> {
        let (tx, rx) = oneshot::channel();
        let msg = ToWorker { message, tx };

        // TODO: handle backpressure properly instead of just returning an error.
        self.sender
            .send(msg)
            .await
            .map_err(|_e| crate::Error::service(Default::default()))?;

        Ok(PublishHandle { rx })
    }
}

#[cfg(test)]
mod tests {
    use super::Publisher;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let _publisher = Publisher::builder()
            .with_topic("projects/p/topics/t")
            //.with_credentials(auth::credentials::anonymous::Builder::new().build())
            .build()
            .await?;
        Ok(())
    }
}
