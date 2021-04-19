use derive_builder::Builder;

use serde::{Deserialize, Serialize};

use sieve::SieveConfig;

#[derive(Builder, Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(structopt::StructOpt))]
/// A struct holding all necessary information required to configure a [`BatchedContagion`] instance
///
/// [`BatchedContagion`]: super::BatchedContagion
pub struct ContagionConfig {
    #[cfg_attr(feature = "cli", structopt(short, long))]
    #[doc = "the threshold  of ready messages required to mark payload as ready"]
    pub ready_threshold: usize,

    #[cfg_attr(feature = "cli", structopt(long))]
    #[doc = "the expected size of the gossip set"]
    pub contagion_sample_size: usize,

    #[cfg_attr(feature = "cli", structopt(flatten))]
    #[doc = "configuration for the underlying sieve algorithm"]
    pub sieve: SieveConfig,
}

impl ContagionConfig {
    /// Check if the ready threshold has been reached
    pub fn ready_threshold_cmp(&self, v: i32) -> bool {
        Self::threshold_cmp(v, self.ready_threshold)
    }

    /// Get the configuration channel capacity
    pub fn channel_cap(&self) -> usize {
        self.sieve.murmur.channel_cap
    }

    fn threshold_cmp(v: i32, threshold: usize) -> bool {
        v > 0 && v as usize >= threshold
    }
}

impl Default for ContagionConfig {
    fn default() -> Self {
        Self {
            ready_threshold: 10,
            contagion_sample_size: 10,
            sieve: SieveConfig::default(),
        }
    }
}
