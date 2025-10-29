//! Marker types for the generic publisher API.

use std::marker::PhantomData;

// A sealed trait pattern prevents downstream users from implementing these
// traits, ensuring that only the states defined in this library are possible.
mod private {
    pub trait Sealed {}
}

// --- Publishing Strategy (Unordered vs. Ordered) ---

/// A trait representing a publishing strategy, sealed to prevent external implementations.
pub trait PublishingStrategy: private::Sealed {}

/// A marker type for a publisher that sends messages without ordering.
#[derive(Debug, Clone)]
pub struct Unordered;
impl private::Sealed for Unordered {}
impl PublishingStrategy for Unordered {}

/// A marker type for a publisher that can send messages with an ordering key.
#[derive(Debug, Clone)]
pub struct Ordered;
impl private::Sealed for Ordered {}
impl PublishingStrategy for Ordered {}

// --- Flow Control Strategy (Ignored vs. Enabled) ---

/// A trait representing a flow control strategy, sealed to prevent external implementations.
pub trait FlowControlStrategy: private::Sealed {}

/// A marker for a publisher that uses the simple "fire-and-forget" publish method.
/// This corresponds to the "Ignore" flow control strategy.
#[derive(Debug, Clone)]
pub struct FlowControlIgnored;
impl private::Sealed for FlowControlIgnored {}
impl FlowControlStrategy for FlowControlIgnored {}

/// A marker for a publisher that uses the permit-based flow control API.
/// This corresponds to the "Block" and "SignalError" strategies.
#[derive(Debug, Clone)]
pub struct FlowControlEnabled;
impl private::Sealed for FlowControlEnabled {}
impl FlowControlStrategy for FlowControlEnabled {}
