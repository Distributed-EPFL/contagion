use derive_builder::Builder;

use serde::{Deserialize, Serialize};

use sieve::batched::BatchedSieveConfig;

#[derive(Builder, Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(structopt::StructOpt))]
/// A struct holding all necessary information required to configure a [`BatchedContagion`] instance
///
/// [`BatchedContagion`]: super::BatchedContagion
pub struct BatchedContagionConfig {
    #[cfg_attr(feature = "cli", structopt(short, long))]
    #[doc = "the threshold  of ready messages required to mark payload as ready"]
    pub ready_threshold: usize,

    #[cfg_attr(feature = "cli", structopt(short, long))]
    #[doc = "threshold of echoes necessary for delivery"]
    pub delivery_threshold: usize,

    #[cfg_attr(feature = "cli", structopt(short, long))]
    #[doc = "the expected size of the gossip set"]
    pub sample_size: usize,

    #[cfg_attr(feature = "cli", structopt(flatten))]
    #[doc = "configuration for the underlying sieve algorithm"]
    pub sieve: BatchedSieveConfig,
}

impl BatchedContagionConfig {
    /// Get the ready threshold of messages for this configuration
    pub fn ready_threshold(&self) -> usize {
        self.ready_threshold
    }

    /// Get the expected sample size
    pub fn sample_size(&self) -> usize {
        self.sample_size
    }

    /// Get the inner `BatchedSieveConfig`
    pub fn sieve(&self) -> &BatchedSieveConfig {
        &self.sieve
    }

    /// Check if the ready threshold has been reached
    pub fn ready_threshold_cmp(&self, v: isize) -> bool {
        Self::threshold_cmp(v, self.ready_threshold)
    }

    /// Check if the delivery threshold has been reached
    pub fn delivery_threshold_cmp(&self, v: isize) -> bool {
        Self::threshold_cmp(v, self.delivery_threshold)
    }

    fn threshold_cmp(v: isize, threshold: usize) -> bool {
        v > 0 && v as usize >= threshold
    }
}