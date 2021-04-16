use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use super::{Digest, Sequence};

use futures::future::FutureExt;
use futures::stream::{self, Stream, StreamExt};

use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::{self, JoinHandle};

use tokio_stream::wrappers::ReceiverStream;

#[derive(Copy, Clone, Debug)]
enum State {
    Seen,
    Delivered,
}

impl State {
    fn set_delivered(&mut self) -> bool {
        if let Self::Seen = self {
            *self = Self::Delivered;
            true
        } else {
            false
        }
    }
}

type Channel = oneshot::Sender<Sequence>;

/// Handle for the agent that keeps track of which payloads have been seen and/or delivered for each known batch
pub struct SeenHandle {
    senders: RwLock<HashMap<Digest, mpsc::Sender<Command>>>,
    capacity: usize,
}

impl SeenHandle {
    /// Create a new `PendingHandle` using a given channel capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            senders: Default::default(),
            capacity,
        }
    }

    /// Register a `Stream` of sequences as seen and returns the ones that weren't already seen or delivered
    pub async fn register_seen(
        &self,
        digest: Digest,
        sequences: impl Stream<Item = Sequence>,
    ) -> impl Stream<Item = Sequence> {
        let tx = self.get_agent_or_insert(digest).await;

        sequences
            .zip(stream::repeat(tx))
            .filter_map(|(x, tx)| async move {
                let (resp, rx) = oneshot::channel();

                let _ = tx.send(Command::Seen(x, resp)).await;

                rx.await.ok()
            })
    }

    /// Register a `Stream` of sequences as delivered and returns a stream of sequences
    /// that were previously seen but not already delivered
    pub async fn register_delivered(
        &self,
        digest: Digest,
        sequences: impl Stream<Item = Sequence>,
    ) -> impl Stream<Item = Sequence> {
        let tx = self.get_agent_or_insert(digest).await;

        sequences
            .zip(stream::repeat(tx))
            .filter_map(|(x, tx)| async move {
                let (resp, rx) = oneshot::channel();

                let _ = tx.send(Command::Delivered(x, resp)).await;

                rx.await.ok()
            })
    }

    /// Get all seen or delivered sequences from the specified batch
    pub async fn get_seen(&self, digest: Digest) -> Option<impl Stream<Item = Sequence>> {
        let tx = self.get_agent(digest).await?;

        Self::get_seen_internal(tx, self.capacity).await
    }

    async fn get_seen_internal(
        tx: mpsc::Sender<Command>,
        capacity: usize,
    ) -> Option<impl Stream<Item = Sequence>> {
        let (resp, rx) = mpsc::channel(capacity);

        let _ = tx.send(Command::GetSeen(resp)).await;

        Some(ReceiverStream::new(rx))
    }

    /// Get all seen sequences for every known batch
    #[allow(clippy::needless_collect)]
    pub async fn get_known_batches(
        &self,
    ) -> impl Stream<Item = (Digest, impl Stream<Item = Sequence>)> + '_ {
        let agents = self
            .senders
            .read()
            .await
            .iter()
            .map(|(digest, sender)| (*digest, sender.clone()))
            .collect::<Vec<_>>();

        let capacity = self.capacity;

        stream::iter(agents.into_iter()).filter_map(move |(digest, sender)| {
            Self::get_seen_internal(sender, capacity).map(move |s| s.map(|s| (digest, s)))
        })
    }

    /// Remove all existing tracking information for the specified batch.
    /// All outstanding request for information from this batch will be processed
    /// before the agent is actually stopped
    pub async fn purge(&self, digest: Digest) {
        self.senders.write().await.remove(&digest);
    }

    /// Get the agent channel for some batch without inserting it if it doesn't exist
    async fn get_agent(&self, digest: Digest) -> Option<mpsc::Sender<Command>> {
        self.senders.read().await.get(&digest).map(Clone::clone)
    }

    async fn get_agent_or_insert(&self, digest: Digest) -> mpsc::Sender<Command> {
        self.senders
            .write()
            .await
            .entry(digest)
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(self.capacity);

                PendingAgent::new(rx).spawn();

                tx
            })
            .clone()
    }
}

/// A `PendingAgent` handles seen sequence numbers from one batch. It keeps
/// track of which sequence number has been seen (and not delivered) and which ones have
/// already been delivered
struct PendingAgent {
    set: BTreeMap<Sequence, State>,
    receiver: mpsc::Receiver<Command>,
}

impl PendingAgent {
    fn new(receiver: mpsc::Receiver<Command>) -> Self {
        Self {
            set: Default::default(),
            receiver,
        }
    }

    fn spawn(mut self) -> JoinHandle<Self> {
        task::spawn(async move {
            while let Some(cmd) = self.receiver.recv().await {
                match cmd {
                    Command::Seen(sequence, resp) => match self.set.entry(sequence) {
                        Entry::Vacant(e) => {
                            e.insert(State::Seen);
                            let _ = resp.send(sequence);
                        }
                        _ => continue,
                    },
                    Command::Delivered(sequence, resp) => match self.set.entry(sequence) {
                        Entry::Occupied(mut e) => {
                            if e.get_mut().set_delivered() {
                                let _ = resp.send(sequence);
                            }
                        }
                        _ => continue,
                    },
                    Command::GetSeen(channel) => {
                        stream::iter(self.set.keys().copied())
                            .zip(stream::repeat(channel))
                            .for_each(|(seq, channel)| async move {
                                let _ = channel.send(seq).await;
                            })
                            .await;
                    }
                }
            }

            self
        })
    }
}

enum Command {
    Seen(Sequence, Channel),
    Delivered(Sequence, Channel),
    GetSeen(mpsc::Sender<Sequence>),
}

#[cfg(test)]
mod test {
    use super::*;

    use futures::{future, stream};

    use sieve::test::generate_batch;

    const SIZE: usize = 10;

    #[tokio::test]
    async fn purging() {
        let batch = generate_batch(SIZE);
        let digest = *batch.info().digest();
        let handle = SeenHandle::new(32);

        handle
            .register_seen(digest, stream::iter(0..batch.len()))
            .await
            .enumerate()
            .for_each(|(exp, seq)| async move {
                assert_eq!(exp as Sequence, seq, "incorrect sequence ordering");
            })
            .await;

        handle.purge(digest).await;

        let result = handle.get_seen(digest).await;

        assert!(result.is_none(), "information was not purged");
    }

    #[tokio::test]
    async fn seen() {
        let batch = generate_batch(SIZE);
        let digest = *batch.info().digest();
        let handle = SeenHandle::new(32);
        let seen = stream::iter((0..batch.len()).step_by(2));

        handle
            .register_seen(digest, seen.clone())
            .await
            .enumerate()
            .for_each(|(curr, seq)| async move {
                assert_eq!(curr as Sequence * 2, seq);
            })
            .await;

        handle
            .get_seen(digest)
            .await
            .expect("no data for batch")
            .zip(seen)
            .for_each(|(actual, exp)| async move {
                assert_eq!(actual, exp);
            })
            .await;
    }

    #[tokio::test]
    async fn seen_then_delivered() {
        let batch = generate_batch(SIZE);
        let digest = *batch.info().digest();
        let handle = SeenHandle::new(32);
        let range = 0..batch.len();

        handle
            .register_seen(digest, stream::iter(range.clone()))
            .await
            .enumerate()
            .for_each(|(exp, actual)| async move {
                assert_eq!(exp as Sequence, actual);
            })
            .await;

        let new_seen = handle
            .register_seen(digest, stream::once(future::ready(0)))
            .await
            .collect::<Vec<_>>()
            .await;

        assert!(new_seen.is_empty(), "could see sequences twice");

        handle
            .register_delivered(digest, stream::iter(range))
            .await
            .enumerate()
            .for_each(|(exp, actual)| async move {
                assert_eq!(exp as Sequence, actual);
            })
            .await;
    }

    #[tokio::test]
    async fn delivered_then_seen() {
        let batch = generate_batch(SIZE);
        let digest = *batch.info().digest();
        let handle = SeenHandle::new(32);
        let range = 0..batch.len();

        let delivered = handle
            .register_delivered(digest, stream::iter(range.clone()))
            .await
            .collect::<Vec<_>>()
            .await;

        assert!(delivered.is_empty(), "could deliver without seeing first");

        handle
            .register_seen(digest, stream::iter(range))
            .await
            .enumerate()
            .for_each(|(exp, actual)| async move {
                assert_eq!(exp as Sequence, actual);
            })
            .await;
    }
}
