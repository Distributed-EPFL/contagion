#![deny(missing_docs)]

//! Contagion, implements the probabilistic reliable broadcast abstraction.
//! This abstraction is strictly stronger than probabilistic consistent
//! broadcast, as it additionally guarantees e-totality.  Despite a Byzantine
//! sender, either none or every correct process delivers the broadcast message.

/// The batched version of contagion that supports mulitple sender and provides optimizations for
/// latency
pub mod batched;

/// The classic version of contagion that only supports either sender or receiver mode and can only
/// broadcast/receive one message per instance
pub mod classic;
