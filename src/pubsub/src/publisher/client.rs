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

use std::future::Future;
use std::marker::PhantomData;

pub use crate::strategy;
pub use crate::traits;

use strategy::{
    FlowControlEnabled, FlowControlIgnored, FlowControlStrategy, Ordered, PublishingStrategy,
    Unordered,
};
use traits::{
    BoxedOrderedPublishPermit, BoxedPublishPermit, FlowControlledPublisher,
    OrderedFlowControlPublisher, OrderedPublishPermitApi, PublishPermitApi, SimpleOrderedPublisher,
    SimplePublisher,
};

/// Client for publishing messages to Pub/Sub topics.
#[derive(Clone, Debug)]
pub struct PublisherClient {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
}

/// A builder for [PublisherClient].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::PublisherClient;
/// let builder : ClientBuilder = PublisherClient::builder();
/// let client = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// let publisher_a = client.publisher("projects/my-project/topics/topic-a");
/// let publisher_b = client.publisher("projects/my-project/topics/topic-b");
/// # Ok(()) }
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::PublisherClient;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = PublisherClient;
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

impl PublisherClient {
    /// Returns a builder for [PublisherClient].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_pubsub::client::PublisherClient;
    /// let client = PublisherClient::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Creates a new Pub/Sub publisher client with the given configuration.
    pub(crate) async fn new(
        config: gaxi::options::ClientConfig,
    ) -> Result<Self, gax::client_builder::Error> {
        let inner = crate::generated::gapic_dataplane::client::Publisher::new(config).await?;
        std::result::Result::Ok(Self { inner })
    }

    /// Creates a new `Publisher` for a given topic.
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::*;
    /// # use builder::publisher::ClientBuilder;
    /// # use client::PublisherClient;
    /// # use model::PubsubMessage;
    /// let client = PublisherClient::builder()
    ///     .with_endpoint("https://pubsub.googleapis.com")
    ///     .build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic");
    /// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    pub fn publisher<T>(&self, topic: T) -> PublisherBuilder<Unordered, FlowControlIgnored>
    where
        T: Into<String>,
    {
        unimplemented!()
    }
}

/// A publisher for a specific topic.
///
/// This struct is generic over a `PublishingStrategy` (Ordered vs. Unordered)
/// and a `FlowControlStrategy` (Ignored vs. Enabled). The available methods
/// will change depending on its generic parameters.
#[derive(Clone, Debug)]
pub struct Publisher<S: PublishingStrategy, F: FlowControlStrategy> {
    // topic: String,
    // settings: PublisherSettings,
    _strategy: PhantomData<(S, F)>,
}

/// A builder for creating and configuring a `Publisher`.
#[derive(Clone, Debug)]
pub struct PublisherBuilder<S: PublishingStrategy, F: FlowControlStrategy> {
    // settings: PublisherSettings,
    _strategy: PhantomData<(S, F)>,
}

// --- Builder Implementation ---

// Generic implementation for methods that don't change the builder's state.
impl<S: PublishingStrategy, F: FlowControlStrategy> PublisherBuilder<S, F> {
    pub fn set_batch_delay_threshold(self, duration: std::time::Duration) -> Self {
        unimplemented!()
    }
    // ... other common settings ...
}

// Implementation block for the default "Unordered" builder.
impl<F: FlowControlStrategy> PublisherBuilder<Unordered, F> {
    /// Enables message ordering, returning a builder for an ordered publisher.
    pub fn enable_message_ordering(self) -> PublisherBuilder<Ordered, F> {
        unimplemented!()
    }
}

// Implementation block for the default "FlowControlIgnored" builder.
impl<S: PublishingStrategy> PublisherBuilder<S, FlowControlIgnored> {
    /// Enables permit-based flow control.
    /// This transforms the builder, unlocking the `acquire()` and `try_acquire()` methods
    /// on the final publisher, while disabling the simple `publish()` method.
    pub fn with_flow_control(
        self,
        settings: FlowControlSettings,
    ) -> PublisherBuilder<S, FlowControlEnabled> {
        unimplemented!()
    }
}

// Final `build` methods for each concrete builder type.

impl PublisherBuilder<Unordered, FlowControlIgnored> {
    pub fn build(self) -> Publisher<Unordered, FlowControlIgnored> {
        unimplemented!()
    }
}

impl PublisherBuilder<Ordered, FlowControlIgnored> {
    pub fn build(self) -> Publisher<Ordered, FlowControlIgnored> {
        unimplemented!()
    }
}

impl PublisherBuilder<Unordered, FlowControlEnabled> {
    pub fn build(self) -> Publisher<Unordered, FlowControlEnabled> {
        unimplemented!()
    }
}

impl PublisherBuilder<Ordered, FlowControlEnabled> {
    pub fn build(self) -> Publisher<Ordered, FlowControlEnabled> {
        unimplemented!()
    }
}

// --- Publisher API Implementation ---

// --- Trait Impls for "Simple" Publishers (FlowControlIgnored) ---

impl SimplePublisher for Publisher<Unordered, FlowControlIgnored> {
    fn publish(&self, msg: Message) -> PublishHandle {
        unimplemented!()
    }
}

impl SimplePublisher for Publisher<Ordered, FlowControlIgnored> {
    fn publish(&self, msg: Message) -> PublishHandle {
        unimplemented!()
    }
}

impl SimpleOrderedPublisher for Publisher<Ordered, FlowControlIgnored> {
    fn publish_ordered(&self, msg: OrderedMessage) -> PublishHandle {
        unimplemented!()
    }
}

// --- Trait Impls for "Flow Controlled" Publishers (FlowControlEnabled) ---

// A permit that grants the right to publish a single message.
pub struct PublishPermit<S: PublishingStrategy> {
    _strategy: PhantomData<S>,
}

impl OrderedFlowControlPublisher for Publisher<Ordered, FlowControlEnabled> {
    async fn acquire(&self, message_size: u32) -> BoxedOrderedPublishPermit {
        // The concrete implementation would await a real permit...
        let permit: PublishPermit<Ordered> = PublishPermit {
            _strategy: PhantomData,
        };
        // ...and then return it as a boxed trait object.
        Box::new(permit)
    }

    fn try_acquire(&self, message_size: u32) -> Result<BoxedOrderedPublishPermit, ()> {
        // The concrete implementation would await a real permit...
        let permit: PublishPermit<Ordered> = PublishPermit {
            _strategy: PhantomData,
        };
        // ...and then return it as a boxed trait object.
        Ok(Box::new(permit))
    }
}

impl FlowControlledPublisher for Publisher<Unordered, FlowControlEnabled> {
    async fn acquire(&self, message_size: u32) -> BoxedPublishPermit {
        // The concrete implementation would await a real permit...
        let permit: PublishPermit<Unordered> = PublishPermit {
            _strategy: PhantomData,
        };
        // ...and then return it as a boxed trait object.
        Box::new(permit)
    }

    fn try_acquire(&self, message_size: u32) -> Result<BoxedPublishPermit, ()> {
        // The concrete implementation would await a real permit...
        let permit: PublishPermit<Unordered> = PublishPermit {
            _strategy: PhantomData,
        };
        // ...and then return it as a boxed trait object.
        Ok(Box::new(permit))
    }
}

// --- Trait Impls for PublishPermits ---

impl PublishPermitApi for PublishPermit<Unordered> {
    fn publish(self: Box<Self>, msg: Message) -> PublishHandle {
        unimplemented!()
    }
}

impl OrderedPublishPermitApi for PublishPermit<Ordered> {
    fn publish(self: Box<Self>, msg: Message) -> PublishHandle {
        unimplemented!()
    }

    fn publish_ordered(self: Box<Self>, msg: OrderedMessage) -> PublishHandle {
        unimplemented!()
    }
}

// Other structs (e.g., PublishHandle, Message, etc.) would be defined here or in other modules.
pub struct PublishHandle;
pub struct Message;

impl Message {
    pub fn new(msg: String) -> Self {
        Self
    }
}
pub struct OrderedMessage;

impl OrderedMessage {
    pub fn new(msg: String, key: String) -> Self {
        Self
    }
}

pub struct FlowControlSettings;
