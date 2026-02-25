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

use super::builder::StreamingPull;
use super::client_builder::ClientBuilder;
use super::handler::AckResult;
use super::lease_state::NewMessage;
use super::transport::Transport;
use crate::ClientBuilderResult as BuilderResult;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub(super) struct SharedLease {
    pub message_tx: UnboundedSender<NewMessage>,
    pub ack_tx: UnboundedSender<AckResult>,
    pub handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// A Subscriber client for the [Cloud Pub/Sub] API.
// ... (rest of doc comment)
#[derive(Clone, Debug)]
pub struct Subscriber {
    inner: Arc<Transport>,
    client_id: String,
    grpc_subchannel_count: usize,
    pub(super) shared_lease: Arc<Mutex<Option<Weak<SharedLease>>>>,
}

impl Subscriber {
    /// Returns a builder for [Subscriber].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Subscriber::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Receive messages from a [subscription].
    ///
    /// The `subscription` is the full name, in the format of
    /// `projects/*/subscriptions/*`.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample(client: Subscriber) -> anyhow::Result<()> {
    /// let mut stream = client
    ///     .stream("projects/my-project/subscriptions/my-subscription")
    ///     .build();
    /// while let Some((m, h)) = stream.next().await.transpose()? {
    ///     println!("Received message m={m:?}");
    ///     h.ack();
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [subscription]: https://docs.cloud.google.com/pubsub/docs/subscription-overview
    pub fn stream<T>(&self, subscription: T) -> StreamingPull
    where
        T: Into<String>,
    {
        StreamingPull::new(
            self.inner.clone(),
            subscription.into(),
            self.client_id.clone(),
            self.grpc_subchannel_count,
            self.shared_lease.clone(),
        )
    }

    pub(super) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let grpc_subchannel_count =
            std::cmp::max(1, builder.config.grpc_subchannel_count.unwrap_or(1));
        let transport = Transport::new(builder.config).await?;
        Ok(Self {
            inner: Arc::new(transport),
            client_id: uuid::Uuid::new_v4().to_string(),
            grpc_subchannel_count,
            shared_lease: Arc::new(Mutex::new(None)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaxi::grpc::tonic::Status as TonicStatus;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use pubsub_grpc_mock::{MockSubscriber, start};

    #[tokio::test]
    async fn basic() -> anyhow::Result<()> {
        let _ = Subscriber::builder().build().await?;
        Ok(())
    }

    #[tokio::test]
    async fn streaming_pull() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = Subscriber::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let err = client
            .stream("projects/p/subscriptions/s")
            .build()
            .next()
            .await
            .expect("stream should not be empty")
            .expect_err("the first streamed item should be an error");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        Ok(())
    }

    #[tokio::test]
    async fn grpc_subchannel_count() -> anyhow::Result<()> {
        let client = Subscriber::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        assert_eq!(client.grpc_subchannel_count, 1);

        let client = Subscriber::builder()
            .with_credentials(Anonymous::new().build())
            .with_grpc_subchannel_count(0)
            .build()
            .await?;
        assert_eq!(client.grpc_subchannel_count, 1);

        let client = Subscriber::builder()
            .with_credentials(Anonymous::new().build())
            .with_grpc_subchannel_count(8)
            .build()
            .await?;
        assert_eq!(client.grpc_subchannel_count, 8);

        let builder = client.stream("projects/p/subscriptions/s");
        assert_eq!(builder.grpc_subchannel_count, 8);

        Ok(())
    }
}
