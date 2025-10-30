//! Traits for mocking and generic programming with the Pub/Sub publisher.

use crate::publisher::client::{Message, OrderedMessage, PublishHandle};
use std::future::Future;

// --- Core Message Trait ---

/// A trait for types that can be published to Pub/Sub.
pub trait Publishable {
    fn ordering_key(&self) -> Option<&str>;
    // ... other methods for data, attributes ...
}

/// A marker trait indicating that a publisher `P` can publish a message of type `M`.
pub trait CanPublish<M: Publishable> {}

// --- Publisher API Traits ---

/// The API of a permit that can publish messages.
pub trait PermitApi {
    /// Publishes a message, consuming the permit.
    fn publish<M: Publishable>(self: Box<Self>, msg: M) -> PublishHandle
    where
        Self: CanPublish<M>;
}

/// A publisher that uses the simple, fire-and-forget API.
pub trait SimplePublisher: CanPublish<Message> {
    /// Publishes a message.
    fn publish<M: Publishable>(&self, msg: M) -> PublishHandle
    where
        Self: CanPublish<M>;
}

// --- Traits for the "Flow Control" API ---

/// The API of a permit that can publish unordered messages.
pub trait UnorderedPermitApi {
    fn publish(self: Box<Self>, msg: Message) -> PublishHandle;
}

/// The API of a permit that can also publish ordered messages.
pub trait OrderedPermitApi: UnorderedPermitApi {
    fn publish_ordered(self: Box<Self>, msg: OrderedMessage) -> PublishHandle;
}

/// A mockable, type-erased permit for unordered messages.
pub type BoxedUnorderedPermit = Box<dyn UnorderedPermitApi + Send + Sync>;
/// A mockable, type-erased permit for ordered messages.
pub type BoxedOrderedPermit = Box<dyn OrderedPermitApi + Send + Sync>;

/// A publisher that uses the permit-based API for unordered messages.
pub trait FlowControlledPublisher {
    /// The trait object type for the permit this publisher acquires.
    type Permit: UnorderedPermitApi + ?Sized;

    fn acquire(&self, size: u32) -> impl Future<Output = Box<Self::Permit>>;
}

/// A publisher that uses the permit-based API and supports ordered messages.
pub trait OrderedFlowControlledPublisher: FlowControlledPublisher
where
    Self::Permit: OrderedPermitApi,
{
    // This trait provides no new methods, only a type constraint.
}
