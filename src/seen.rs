use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use super::{BatchInfo, Sequence};

use futures::future::FutureExt;
use futures::stream::{self, Stream, StreamExt};

use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::{self, JoinHandle};

use tokio_stream::wrappers::ReceiverStream;

use tracing::{debug, debug_span, error, info, trace, warn};
use tracing_futures::Instrument;

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
    senders: RwLock<HashMap<BatchInfo, mpsc::Sender<Command>>>,
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

    async fn register(
        &self,
        info: BatchInfo,
        sequences: impl Stream<Item = Sequence>,
        maker: impl Fn(Sequence, oneshot::Sender<Sequence>) -> Command + Copy,
    ) -> impl Stream<Item = Sequence> {
        let tx = self.get_agent_or_insert(info).await;

        sequences
            .zip(stream::repeat(tx))
            .filter_map(move |(seq, tx)| async move {
                let (resp, rx) = oneshot::channel();
                let command = (maker)(seq, resp);

                if tx.send(command).await.is_err() {
                    error!("agent for batch {} stopped running", info.digest());
                }

                rx.await.ok()
            })
    }

    /// Register a `Stream` of sequences as seen and returns the ones that weren't already seen or delivered
    pub async fn register_seen(
        &self,
        info: BatchInfo,
        sequences: impl Stream<Item = Sequence>,
    ) -> impl Stream<Item = Sequence> {
        self.register(info, sequences, Command::Seen).await
    }

    /// Register a `Stream` of sequences as delivered and returns a stream of sequences
    /// that were previously seen but not already delivered
    pub async fn register_delivered(
        &self,
        info: BatchInfo,
        sequences: impl Stream<Item = Sequence>,
    ) -> impl Stream<Item = Sequence> {
        self.register(info, sequences, Command::Delivered).await
    }

    /// Register delivery for an Iterator of sequences and returns a Stream of sequences that were not already delivered
    pub async fn register_delivered_iter(
        &self,
        info: BatchInfo,
        seqs: impl IntoIterator<Item = Sequence>,
    ) -> impl Stream<Item = Sequence> {
        self.register_delivered(info, stream::iter(seqs.into_iter()))
            .await
    }

    /// Get all seen or delivered sequences from the specified batch
    pub async fn get_seen(&self, info: BatchInfo) -> Option<impl Stream<Item = Sequence>> {
        trace!("getting all seen sequences from batch {}", info.digest());

        let tx = self.get_agent(info).await?;

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
    ) -> impl Stream<Item = (BatchInfo, impl Stream<Item = Sequence>)> {
        let agents = self
            .senders
            .read()
            .await
            .iter()
            .map(|(digest, sender)| (*digest, sender.clone()))
            .collect::<Vec<_>>();

        let capacity = self.capacity;

        stream::iter(agents.into_iter()).filter_map(move |(info, sender)| {
            Self::get_seen_internal(sender, capacity).map(move |s| s.map(|s| (info, s)))
        })
    }

    /// Remove all existing tracking information for the specified batch.
    /// All outstanding request for information from this batch will be processed
    /// before the agent is actually stopped
    pub async fn purge(&self, info: &BatchInfo) {
        self.senders.write().await.remove(info);
    }

    /// Get the agent channel for some batch without inserting it if it doesn't exist
    async fn get_agent(&self, info: BatchInfo) -> Option<mpsc::Sender<Command>> {
        self.senders.read().await.get(&info).map(Clone::clone)
    }

    async fn get_agent_or_insert(&self, info: BatchInfo) -> mpsc::Sender<Command> {
        self.senders
            .write()
            .await
            .entry(info)
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(self.capacity);

                PendingAgent::new(rx).spawn(info);

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

    fn spawn(mut self, info: BatchInfo) -> JoinHandle<Self> {
        task::spawn(
            async move {
                debug!("started agent");

                while let Some(cmd) = self.receiver.recv().await {
                    match cmd {
                        Command::Seen(sequence, resp) => match self.set.entry(sequence) {
                            Entry::Vacant(e) => {
                                debug!("newly seen sequence {}", sequence);
                                e.insert(State::Seen);
                                let _ = resp.send(sequence);
                            }
                            _ => continue,
                        },
                        Command::Delivered(sequence, resp) => {
                            debug!("checking if {} is already delivered", sequence);

                            match self.set.entry(sequence) {
                                Entry::Occupied(mut e) => {
                                    if e.get_mut().set_delivered() {
                                        debug!("newly delivered sequence {}", sequence);

                                        if resp.send(sequence).is_err() {
                                            warn!("did not wait for response to delivery status");
                                        }
                                    }
                                }
                                _ => continue,
                            }
                        }
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

                info!("monitoring agent exiting");

                self
            }
            .instrument(debug_span!("seen_manager", batch=%info.digest())),
        )
    }
}

#[derive(Debug)]
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
        let info = *batch.info();
        let handle = SeenHandle::new(32);

        handle
            .register_seen(info, stream::iter(0..batch.len()))
            .await
            .enumerate()
            .for_each(|(exp, seq)| async move {
                assert_eq!(exp as Sequence, seq, "incorrect sequence ordering");
            })
            .await;

        handle.purge(&info).await;

        let result = handle.get_seen(info).await;

        assert!(result.is_none(), "information was not purged");
    }

    #[tokio::test]
    async fn seen() {
        let batch = generate_batch(SIZE);
        let info = *batch.info();
        let handle = SeenHandle::new(32);
        let seen = stream::iter((0..batch.len()).step_by(2));

        handle
            .register_seen(info, seen.clone())
            .await
            .enumerate()
            .for_each(|(curr, seq)| async move {
                assert_eq!(curr as Sequence * 2, seq);
            })
            .await;

        handle
            .get_seen(info)
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
        let info = *batch.info();
        let handle = SeenHandle::new(32);
        let range = 0..batch.len();

        handle
            .register_seen(info, stream::iter(range.clone()))
            .await
            .enumerate()
            .for_each(|(exp, actual)| async move {
                assert_eq!(exp as Sequence, actual);
            })
            .await;

        let new_seen = handle
            .register_seen(info, stream::once(future::ready(0)))
            .await
            .collect::<Vec<_>>()
            .await;

        assert!(new_seen.is_empty(), "could see sequences twice");

        handle
            .register_delivered(info, stream::iter(range))
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
        let info = *batch.info();
        let handle = SeenHandle::new(32);
        let range = 0..batch.len();

        let delivered = handle
            .register_delivered(info, stream::iter(range.clone()))
            .await
            .collect::<Vec<_>>()
            .await;

        assert!(delivered.is_empty(), "could deliver without seeing first");

        handle
            .register_seen(info, stream::iter(range))
            .await
            .enumerate()
            .for_each(|(exp, actual)| async move {
                assert_eq!(exp as Sequence, actual);
            })
            .await;
    }
}
