#![deny(missing_docs)]

//! [`Contagion`], implements the probabilistic reliable broadcast abstraction.
//! This abstraction is strictly stronger than probabilistic consistent
//! broadcast, as it additionally guarantees e-totality.  Despite a Byzantine
//! sender, either none or every correct process delivers the broadcasted message.
//!
//! [`Contagion`]: crate::Contagion
mod batched;
pub use batched::*;
