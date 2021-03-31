//! <p>This module provides  the batched version of [`Contagion`].</p>
//!
//! <p>[`BatchedContagion`] provides the same safety guarantees as regular [`Contagion`] but also
//! makes effort to reduce unnecessary network IO and latency by batching blocks of transaction
//! together.</p>
//!
//! [`Contagion`]: crate::classic::Contagion
//! [`BatchedContagion`]: crate::batched::BatchedContagion
//! [`RdvPolicy`]: crate::batched::RdvPolicy

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use drop::async_trait;
use drop::crypto::hash::Digest;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::KeyPair;
use drop::system::manager::Handle;
use drop::system::sender::{ConvertSender, SenderError};
use drop::system::{message, Message, Processor, Sampler, Sender};

use futures::stream::{self, StreamExt};

use sieve::batched::{
    BatchedSieve, BatchedSieveError, BatchedSieveHandle, BatchedSieveMessage, EchoHandle,
};
pub use sieve::batched::{FilteredBatch, RdvPolicy, Sequence};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};

use tracing::debug;

mod config;
pub use config::{BatchedContagionConfig, BatchedContagionConfigBuilder};

mod seen;
pub use seen::SeenHandle;

#[derive(Debug, Snafu)]
/// Errors encountered by the `BatchedContagion` algorithm
pub enum BatchedContagionError {
    #[snafu(display("network error when {}: {}", when, source))]
    /// Network error with accompanying information
    Network {
        /// Actual cause of error
        source: SenderError,
        /// Action that failed due to this error
        when: &'static str,
    },

    #[snafu(display("sieve processing error: {}", source))]
    /// Errors encountered when processing messages from the underlying sieve instance
    SieveError {
        /// Inner error cause
        source: BatchedSieveError,
    },

    #[snafu(display("channel is closed, delivery impossible"))]
    /// A channel was closed when attempting to send or receive a message
    Channel,

    /// The `BatchedContagion` instance was not setup before running.
    /// This may happen when `BatchedContagion::output` isn't called before starting
    /// the `Processor`
    #[snafu(display("contagion was not setup properly"))]
    NotSetup,
}

#[message]
/// The messages exchanged by the `BatchedContagion` algorithm
pub enum BatchedContagionMessage<M>
where
    M: Message,
{
    /// Echo a set of sequences from a given `Batch`
    Ready(Digest, Vec<u32>),
    #[serde(bound(deserialize = "M: Message"))]
    /// Encapsulated message meant for underlying `BatchedSieve`
    Sieve(BatchedSieveMessage<M>),
    /// Request to be added to this peer's gossip set
    Subscribe,
}

impl<M> From<BatchedSieveMessage<M>> for BatchedContagionMessage<M>
where
    M: Message,
{
    fn from(sieve: BatchedSieveMessage<M>) -> Self {
        Self::Sieve(sieve)
    }
}

type SieveSender<S, M> = ConvertSender<BatchedSieveMessage<M>, BatchedContagionMessage<M>, S>;

/// A batched version of the contagion algorithm
pub struct BatchedContagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<BatchedContagionMessage<M>>,
    R: RdvPolicy,
{
    config: BatchedContagionConfig,
    sieve: BatchedSieve<M, SieveSender<S, M>, R>,
    subscribers: RwLock<HashSet<PublicKey>>,
    handle: Option<Arc<Mutex<BatchedSieveHandle<M>>>>,
    delivery: Option<mpsc::Sender<FilteredBatch<M>>>,

    seen: SeenHandle,
    batches: RwLock<HashMap<Digest, FilteredBatch<M>>>,

    ready_set: RwLock<HashSet<PublicKey>>,

    ready_agent: EchoHandle,
}

impl<M, S, R> BatchedContagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<BatchedContagionMessage<M>>,
    R: RdvPolicy,
{
    /// Create new `BatchedContagion` from the given configuration
    pub fn new(keypair: KeyPair, config: BatchedContagionConfig, rdv: R) -> Self {
        let sieve = BatchedSieve::new(keypair, rdv, config.sieve);

        Self {
            config,
            sieve,
            subscribers: Default::default(),
            handle: Default::default(),
            delivery: Default::default(),

            ready_set: Default::default(),

            ready_agent: Default::default(),

            seen: SeenHandle::new(config.channel_cap()),
            batches: Default::default(),
        }
    }

    async fn deliver_from_sieve(&self) -> Option<FilteredBatch<M>> {
        self.handle
            .as_ref()?
            .lock()
            .await
            .try_deliver()
            .await
            .ok()
            .flatten()
    }

    async fn deliverable(
        &self,
        digest: Digest,
        from: PublicKey,
        seqs: impl Iterator<Item = Sequence>,
    ) -> Option<FilteredBatch<M>> {
        let correct = self
            .ready_agent
            .send_many(digest, from, seqs)
            .await
            .filter_map(|(seq, count)| async move {
                if self.config.ready_threshold_cmp(count) {
                    Some(seq)
                } else {
                    None
                }
            });

        let delivery = self.seen.register_delivered(digest, correct).await;

        self.batches
            .read()
            .await
            .get(&digest)
            .map(|batch| batch.include(delivery))
            .filter(|batch| !batch.is_empty())
    }
}

#[async_trait]
impl<M, S, R> Processor<BatchedContagionMessage<M>, M, FilteredBatch<M>, S>
    for BatchedContagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<BatchedContagionMessage<M>> + 'static,
    R: RdvPolicy + 'static,
{
    type Handle = ContagionHandle<M, BatchedSieveHandle<M>>;

    type Error = BatchedContagionError;

    async fn process(
        &self,
        message: Arc<BatchedContagionMessage<M>>,
        from: PublicKey,
        sender: Arc<S>,
    ) -> Result<(), Self::Error> {
        match &*message {
            BatchedContagionMessage::Ready(digest, including)
                if self.ready_set.read().await.contains(&from) =>
            {
                if let Some(batch) = self
                    .deliverable(*digest, from, including.iter().copied())
                    .await
                {
                    self.delivery
                        .as_ref()
                        .context(NotSetup)?
                        .send(batch)
                        .await
                        .map_err(|_| snafu::NoneError)
                        .context(Channel)?;
                }
            }

            BatchedContagionMessage::Sieve(sieve) => {
                let sieve_sender = Arc::new(ConvertSender::new(sender.clone()));
                self.sieve
                    .process(Arc::new(sieve.clone()), from, sieve_sender)
                    .await
                    .context(SieveError)?;

                // FIXME: there should be a check for sequences that are newly seen but already have enough echoes

                if let Some(delivered) = self.deliver_from_sieve().await {
                    let digest = *delivered.digest();

                    let new_seqs = {
                        let mut guard = self.seen.write().await;
                        let batch = guard.entry(*delivered.digest()).or_default();

                        batch.register_pending(delivered.included()).await
                    };

                    if !new_seqs.is_empty() {
                        debug!(
                            "announcing new payloads from batch {} to subscribers",
                            digest
                        );

                        sender
                            .send_many(
                                Arc::new(BatchedContagionMessage::Ready(digest, new_seqs)),
                                self.subscribers.read().await.iter(),
                            )
                            .await
                            .context(Network {
                                when: "sending echoes",
                            })?;
                    }
                }
            }

            BatchedContagionMessage::Subscribe if self.subscribers.write().await.insert(from) => {
                let guard = self.seen.read().await;
                let echoes = stream::iter(guard.iter()).then(|(digest, seqs)| async move {
                    let acks = seqs.get_known().await;

                    Arc::new(BatchedContagionMessage::Ready(*digest, acks))
                });

                sender
                    .send_many_to_one_stream(echoes, &from)
                    .await
                    .context(Network {
                        when: "replying to subscribe request",
                    })?;
            }

            e => debug!("ignored {:?} from {}", e, from),
        }

        Ok(())
    }

    async fn output<SA>(&mut self, sampler: Arc<SA>, sender: Arc<S>) -> Self::Handle
    where
        SA: Sampler,
    {
        let sieve_sender = Arc::new(ConvertSender::new(sender.clone()));
        let handle = self.sieve.output(sampler.clone(), sieve_sender).await;

        let handle = Arc::new(Mutex::new(handle));
        let keys = sender.keys().await;

        let sample = sampler
            .sample(keys.iter().copied(), self.config.ready_threshold)
            .await
            .expect("sampling failed");

        sender
            .send_many(Arc::new(BatchedContagionMessage::Subscribe), sample.iter())
            .await
            .expect("subscription failed");

        self.ready_set.write().await.extend(sample);

        self.handle.replace(handle.clone());

        let (tx, rx) = mpsc::channel(self.config.sieve.murmur.channel_cap());

        self.delivery.replace(tx);

        Self::Handle::new(handle, rx)
    }
}

/// [`Handle`] for the [`BatchedContagion`] algorithm
///
/// [`Handle`]: drop::system::manager::Handle
/// [`BatchedContagion`]: self::BatchedContagion
pub struct ContagionHandle<M, H>
where
    M: Message,
    H: Handle<M, FilteredBatch<M>, Error = BatchedSieveError>,
{
    handle: Arc<Mutex<H>>,
    receiver: mpsc::Receiver<FilteredBatch<M>>,
}

impl<M, H> ContagionHandle<M, H>
where
    M: Message,
    H: Handle<M, FilteredBatch<M>, Error = BatchedSieveError>,
{
    fn new(handle: Arc<Mutex<H>>, receiver: mpsc::Receiver<FilteredBatch<M>>) -> Self {
        Self { handle, receiver }
    }
}

#[async_trait]
impl<M, H> Handle<M, FilteredBatch<M>> for ContagionHandle<M, H>
where
    M: Message,
    H: Handle<M, FilteredBatch<M>, Error = BatchedSieveError>,
{
    type Error = BatchedContagionError;

    async fn deliver(&mut self) -> Result<FilteredBatch<M>, Self::Error> {
        self.receiver.recv().await.ok_or_else(|| Channel.build())
    }

    async fn try_deliver(&mut self) -> Result<Option<FilteredBatch<M>>, Self::Error> {
        use futures::future::{ready, select, Either};

        match select(self.deliver(), ready(None::<FilteredBatch<M>>)).await {
            Either::Left((Ok(msg), _)) => Ok(Some(msg)),
            Either::Left((Err(_), _)) => Channel.fail(),
            Either::Right((_, _)) => Ok(None),
        }
    }

    async fn broadcast(&mut self, payload: &M) -> Result<(), Self::Error> {
        self.handle
            .lock()
            .await
            .broadcast(payload)
            .await
            .context(SieveError)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use drop::test::DummyManager;

    use sieve::batched::test::{generate_batch, generate_sieve_sequence};
    use sieve::batched::Fixed;

    /// Generate a sequence of `Ready` Message with specified conflicts
    pub fn generate_ready_echoes<M>(
        digest: Digest,
        sequences: impl Iterator<Item = Sequence> + Clone,
        count: usize,
    ) -> impl Iterator<Item = BatchedContagionMessage<u32>>
    where
        M: Message,
    {
        std::iter::repeat(sequences)
            .zip((0..count).map(|x| x as u32))
            .map(move |(seqs, x)| BatchedContagionMessage::Ready(digest, seqs.collect()))
    }

    #[tokio::test]
    async fn correct_delivery_with_conflicts() {
        drop::test::init_logger();

        const PEER_COUNT: usize = 10;
        const BATCH_SIZE: usize = 20;
        const CONFLICT_RANGE: std::ops::Range<usize> = 0..BATCH_SIZE / 2;

        let config = BatchedContagionConfig::default();
        let contagion = BatchedContagion::new(KeyPair::random(), config, Fixed::new_local());
        let messages = generate_sieve_sequence(PEER_COUNT, BATCH_SIZE, CONFLICT_RANGE)
            .map(Into::into)
            .chain(generate_ready_echoes());
        let mut manager = DummyManager::new(messages, PEER_COUNT);

        let mut handle = manager.run(contagion).await;

        let batch = handle.deliver().await.expect("deliver failed");

        assert_eq!(batch.digest(), digest, "wrong batch delivered");
    }
}
