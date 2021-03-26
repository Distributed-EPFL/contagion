#![deny(missing_docs)]

//! [`Contagion`], implements the probabilistic reliable broadcast abstraction.
//! This abstraction is strictly stronger than probabilistic consistent
//! broadcast, as it additionally guarantees e-totality.  Despite a Byzantine
//! sender, either none or every correct process delivers the broadcasted message.
//!
//! [`Contagion`]: crate::classic::Contagion

/// This module provides [`BatchedContagion`] which is an improved version of `Contagion` that supports multiple
/// senders and provides optimizations for latency and reduced network IO.
///
/// [`BatchedContagion`]: crate::batched::BatchedContagion
pub mod batched;

/// This module provides the classic version of [`Contagion`] that only supports either sender or receiver mode and
/// can only
/// broadcast/receive one message per instance and produces a lot of unnecessary traffic
///
/// [`Contagion`]: crate::classic::Contagion
pub mod classic;
