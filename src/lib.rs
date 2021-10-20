#![deny(missing_docs)]

//! [`Contagion`], implements the probabilistic reliable broadcast abstraction.
//! This abstraction is strictly stronger than probabilistic consistent
//! broadcast, as it additionally guarantees e-totality.  Despite a Byzantine
//! sender, either none or every correct process delivers the broadcasted message.
//!
//! [`Contagion`]: self::Contagion

mod config;
pub use config::{ContagionConfig, ContagionConfigBuilder};

#[cfg(feature = "system")]
mod system;
#[cfg(feature = "system")]
pub use system::*;
