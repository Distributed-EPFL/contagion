use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use super::{Digest, Sequence};

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
        let (resp, rx) = mpsc::channel(self.capacity);

        let _ = tx.send(Command::GetSeen(resp)).await;

        Some(ReceiverStream::new(rx))
    }

    /// Remove all existing tracking information for the specified batch.
    /// All outstanding request for information from this batch will be processed
    /// before the agent is actually stopped
    pub fn purge(&self, digest: Digest) {
        self.senders.write().await.remove(&digest);
    }

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
                            e.insert(State::Delivered);
                            resp.send(sequence);
                        }
                        _ => continue,
                    },
                    Command::Delivered(sequence, resp) => match self.set.entry(sequence) {
                        Entry::Occupied(e) => {
                            if e.get_mut().set_delivered() {
                                resp.send(sequence);
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
