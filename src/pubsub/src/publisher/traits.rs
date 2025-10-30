//! Traits for mocking and generic programming with the Pub/Sub publisher.

use crate::publisher::client::{Message, OrderedMessage, PublishHandle};
use std::future::Future;

// --- Trait for the "Simple" API ---

/// A publisher that uses the simple, fire-and-forget API.
///
/// This trait is implemented by `Publisher<_, FlowControlIgnored>`.
pub trait SimplePublisher {
    /// Publishes a standard, unordered message.
    fn publish(&self, msg: Message) -> PublishHandle;
}

/// Extends the simple publisher with the ability to publish ordered messages.
///
/// This trait is implemented by `Publisher<Ordered, FlowControlIgnored>`.
pub trait SimpleOrderedPublisher: SimplePublisher {
    /// Publishes a message with an ordering key.
    fn publish_ordered(&self, msg: OrderedMessage) -> PublishHandle;
}

// --- Trait for the "Flow Control" API ---

/// The API of a `PublishPermit`, allowing it to be mocked.
///
/// The `Box<Self>` receiver is used to make the trait object-safe.
pub trait OrderedPublishPermitApi {
    /// Consumes the permit to publish a standard message.
    fn publish(self: Box<Self>, msg: Message) -> PublishHandle;

    /// Consumes the permit to publish an ordered message.
    fn publish_ordered(self: Box<Self>, msg: OrderedMessage) -> PublishHandle;
}

pub trait PublishPermitApi {
    /// Consumes the permit to publish a standard message.
    fn publish(self: Box<Self>, msg: Message) -> PublishHandle;
}

/// A mockable, type-erased permit returned by `acquire()` in the trait.
pub type BoxedPublishPermit = Box<dyn PublishPermitApi + Send + Sync>;

/// A mockable, type-erased permit returned by `acquire()` in the trait.
pub type BoxedOrderedPublishPermit = Box<dyn OrderedPublishPermitApi + Send + Sync>;

/// A publisher that uses the permit-based, flow-controlled API.
///
/// This trait is implemented by `Publisher<_, FlowControlEnabled>`.
pub trait FlowControlledPublisher {
    /// Asynchronously acquires a permit to publish.
    ///
    /// The returned future resolves to a `BoxedPublishPermit`, a type-erased
    /// trait object that can be used for mocking.
    fn acquire(&self, message_size: u32) -> impl Future<Output = BoxedPublishPermit>; // Simplified Error for now

    fn try_acquire(&self, message_size: u32) -> Result<BoxedPublishPermit, ()>;
}

pub trait OrderedFlowControlPublisher {
    /// Asynchronously acquires a permit to publish.
    ///
    /// The returned future resolves to a `BoxedPublishPermit`, a type-erased
    /// trait object that can be used for mocking.
    fn acquire(&self, message_size: u32) -> impl Future<Output = BoxedOrderedPublishPermit>; // Simplified Error for now

    fn try_acquire(&self, message_size: u32) -> Result<BoxedOrderedPublishPermit, ()>;
}
