use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use super::Sequence;

use tokio::sync::RwLock;

#[derive(Default)]
/// Convenient management of seen and delivered sequence
pub struct PendingSequences {
    set: RwLock<BTreeMap<Sequence, State>>,
}

impl PendingSequences {
    /// Create a new empty `PendingSequences`
    pub fn new() -> Self {
        Self {
            set: Default::default(),
        }
    }

    /// Register a new set of sequences as pending
    ///
    /// # Returns
    /// A `Vec` of sequences that are newly pending and were not previoulsy delivered
    pub async fn register_pending(&self, seqs: impl Iterator<Item = Sequence>) -> Vec<Sequence> {
        let mut guard = self.set.write().await;

        seqs.filter(|seq| match guard.entry(*seq) {
            Entry::Vacant(e) => {
                e.insert(State::Pending);
                true
            }
            _ => false,
        })
        .collect()
    }

    /// Register a new set of sequences as delivered
    ///
    /// # Returns
    /// A `Vec` of sequences that have been marked as delivered
    pub async fn register_delivered(&self, seqs: impl Iterator<Item = Sequence>) -> Vec<Sequence> {
        let mut guard = self.set.write().await;

        seqs.filter(|seq| match guard.entry(*seq) {
            Entry::Occupied(mut e) => e.get_mut().set_delivered(),
            _ => false,
        })
        .collect()
    }
}

#[derive(Copy, Clone, Debug)]
enum State {
    Pending,
    Delivered,
}

impl State {
    fn set_delivered(&mut self) -> bool {
        if let Self::Pending = self {
            *self = Self::Delivered;
            true
        } else {
            false
        }
    }
}
