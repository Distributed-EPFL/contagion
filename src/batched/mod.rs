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

use futures::future::{FutureExt, OptionFuture};
use futures::stream::{Stream, StreamExt};

use postage::dispatch;
use postage::sink::Sink as _;
use postage::stream::Stream as _;

use sieve::{EchoHandle, Sieve, SieveError, SieveHandle, SieveMessage};
pub use sieve::{FilteredBatch, Fixed, Payload, RdvPolicy, RoundRobin, Sequence};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{Mutex, RwLock};

use tracing::debug;

mod config;
pub use config::{ContagionConfig, ContagionConfigBuilder};

mod seen;
pub use seen::SeenHandle;

#[derive(Debug, Snafu)]
/// Errors encountered by the `Contagion` algorithm
pub enum ContagionError {
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
    SieveFail {
        /// Inner error cause
        source: SieveError,
    },

    #[snafu(display("channel is closed, delivery impossible"))]
    /// A channel was closed when attempting to send or receive a message
    Channel,

    /// The `Contagion` instance was not setup before running.
    /// This may happen when `BatchedContagion::output` isn't called before starting
    /// the `Processor`
    #[snafu(display("contagion was not setup properly"))]
    NotSetup,
}

#[message]
/// The messages exchanged by the `BatchedContagion` algorithm
pub enum ContagionMessage<M>
where
    M: Message,
{
    /// Echo all sequences in a batch with a set of exceptions
    Ready(Digest, Vec<Sequence>),
    /// Echo only sequence in a batch
    ReadyOne(Digest, Sequence),
    #[serde(bound(deserialize = "M: Message"))]
    /// Encapsulated message meant for underlying `BatchedSieve`
    Sieve(SieveMessage<M>),
    /// Request to be added to this peer's gossip set
    Subscribe,
}

impl<M> From<Payload<M>> for ContagionMessage<M>
where
    M: Message,
{
    fn from(payload: Payload<M>) -> Self {
        Self::Sieve(payload.into())
    }
}

impl<M> From<SieveMessage<M>> for ContagionMessage<M>
where
    M: Message,
{
    fn from(sieve: SieveMessage<M>) -> Self {
        Self::Sieve(sieve)
    }
}

type SieveSender<S, M> = ConvertSender<SieveMessage<M>, ContagionMessage<M>, S>;
type ConcreteSieveHandle<M, S, R> = SieveHandle<M, SieveSender<S, M>, R>;

/// A batched version of the contagion algorithm
pub struct Contagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    config: ContagionConfig,
    sieve: Sieve<M, SieveSender<S, M>, R>,
    subscribers: RwLock<HashSet<PublicKey>>,
    handle: Option<Mutex<ConcreteSieveHandle<M, S, R>>>,
    delivery: Option<dispatch::Sender<FilteredBatch<M>>>,

    seen: SeenHandle,
    batches: RwLock<HashMap<Digest, FilteredBatch<M>>>,

    ready_set: RwLock<HashSet<PublicKey>>,

    ready_agent: EchoHandle,
}

impl<M, S, R> Contagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    /// Create new `BatchedContagion` from the given configuration
    pub fn new(keypair: KeyPair, config: ContagionConfig, rdv: R) -> Self {
        let sieve = Sieve::new(keypair, rdv, config.sieve);

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

    async fn size_from_digest(&self, digest: &Digest) -> Option<Sequence> {
        self.batches
            .read()
            .await
            .get(digest)
            .map(|batch| batch.sequence())
    }

    async fn deliver(&self, batch: FilteredBatch<M>) -> Result<(), ContagionError> {
        let mut delivery = self.delivery.as_ref().map(Clone::clone).context(NotSetup)?;

        debug!(
            "delivering {} sequences from {}",
            batch.len(),
            batch.digest(),
        );

        delivery
            .send(batch)
            .await
            .map_err(|_| snafu::NoneError)
            .context(Channel)
    }

    /// Register echoes for the given set of sequences
    async fn register_echoes<'a>(
        &'a self,
        digest: Digest,
        from: PublicKey,
        included: impl Iterator<Item = Sequence> + 'a,
    ) -> impl Stream<Item = Sequence> + 'a {
        let config = self.config;

        self.ready_agent
            .send_many(digest, from, included)
            .await
            .inspect(move |(seq, count)| {
                debug!(
                    "have {}/{} acks for seq {} of batch {}",
                    count, config.ready_threshold, seq, digest
                );
            })
            .filter_map(move |(seq, count)| async move {
                if config.ready_threshold_cmp(count) {
                    Some(seq)
                } else {
                    None
                }
            })
            .inspect(move |seq| debug!("ready to deliver {} from {}", seq, digest))
    }

    /// Check which sequences can be delivered after receiving n-acks for the given
    /// sequences
    async fn deliverable(
        &self,
        digest: Digest,
        from: PublicKey,
        excluded: impl IntoIterator<Item = Sequence>,
    ) -> Option<FilteredBatch<M>> {
        let size = self.size_from_digest(&digest).await?;
        let seqs = excluded.into_iter().collect::<BTreeSet<_>>();

        let included = (0..size).filter(|x| seqs.contains(&x));

        let correct = self.register_echoes(digest, from, included).await;

        let delivery = self.seen.register_delivered(digest, correct).await;
        OptionFuture::from(
            self.batches
                .read()
                .await
                .get(&digest)
                .map(move |batch| batch.include_stream(delivery)),
        )
        .await
        .filter(|batch| !batch.is_empty())
    }
}

#[async_trait]
impl<M, S, R> Processor<ContagionMessage<M>, Payload<M>, FilteredBatch<M>, S> for Contagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>> + 'static,
    R: RdvPolicy + 'static,
{
    type Handle = ContagionHandle<M, S, R>;

    type Error = ContagionError;

    async fn process(
        &self,
        message: Arc<ContagionMessage<M>>,
        from: PublicKey,
        sender: Arc<S>,
    ) -> Result<(), Self::Error> {
        match &*message {
            ContagionMessage::Ready(digest, ref excluding)
                if self.ready_set.read().await.contains(&from) =>
            {
                let mut excluding = excluding.clone(); // FIXME: no need to clone once the messages aren't in Arc

                debug!(
                    "got acknowledgements for {} excluding {}",
                    digest,
                    excluding.iter().fold(String::new(), |acc, curr| {
                        acc + format!("{}, ", curr).as_str()
                    })
                );

                excluding.sort_unstable();

                if let Some(batch) = self.deliverable(*digest, from, excluding).await {
                    self.delivery
                        .as_ref()
                        .context(NotSetup)?
                        .clone()
                        .send(batch)
                        .await
                        .map_err(|_| snafu::NoneError)
                        .context(Channel)?;
                }
            }

            ContagionMessage::ReadyOne(_digest, _sequence) => {
                todo!()
            }

            ContagionMessage::Sieve(sieve) => {
                let sieve_sender = Arc::new(ConvertSender::new(sender.clone()));

                self.sieve
                    .process(Arc::new(sieve.clone()), from, sieve_sender)
                    .await
                    .context(SieveFail)?;

                if let Some(delivered) = self.deliver_from_sieve().await {
                    let digest = *delivered.digest();

                    debug!(
                        "new delivery from sieve for {} containing {} sequences",
                        digest,
                        delivered.len() - delivered.excluded_len()
                    );

                    if let Some(deliverable) =
                        self.deliverable(digest, from, delivered.excluded()).await
                    {
                        debug!(
                            "delivering new batch with {} sequences included",
                            deliverable.len()
                        );
                        self.deliver(deliverable).await?;
                    }
                }
            }

            ContagionMessage::Subscribe if self.subscribers.write().await.insert(from) => {
                let echoes = self.seen.get_known_batches().await.then(|(digest, seqs)| {
                    seqs.collect::<Vec<_>>()
                        .map(move |acks| ContagionMessage::Ready(digest, acks))
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

    async fn setup<SA>(&mut self, sampler: Arc<SA>, sender: Arc<S>) -> Self::Handle
    where
        SA: Sampler,
    {
        let sieve_sender = Arc::new(ConvertSender::new(sender.clone()));
        let handle = self.sieve.setup(sampler.clone(), sieve_sender).await;

        let handle = handle;
        let keys = sender.keys().await;

        let sample = sampler
            .sample(keys.iter().copied(), self.config.ready_threshold)
            .await
            .expect("sampling failed");

        sender
            .send_many(ContagionMessage::Subscribe, sample.iter())
            .await
            .expect("subscription failed");

        self.ready_set.write().await.extend(sample);

        self.handle.replace(Mutex::new(handle.clone()));

        let (tx, rx) = dispatch::channel(self.config.sieve.murmur.channel_cap);

        self.delivery.replace(tx);

        Self::Handle::new(handle, rx)
    }

    async fn garbage_collection(&self) {
        todo!()
    }

    async fn disconnect<SA: Sampler>(&self, _: PublicKey, _: Arc<S>, _: Arc<SA>) {
        todo!()
    }
}

impl<M, S> Default for Contagion<M, S, sieve::Fixed>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
{
    fn default() -> Self {
        Self::new(
            KeyPair::random(),
            ContagionConfig::default(),
            Fixed::new_local(),
        )
    }
}

/// [`Handle`] for the [`Contagion`] algorithm
///
/// [`Handle`]: drop::system::manager::Handle
/// [`Contagion`]: self::Contagion
pub struct ContagionHandle<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    handle: SieveHandle<M, SieveSender<S, M>, R>,
    receiver: dispatch::Receiver<FilteredBatch<M>>,
}

impl<M, S, R> ContagionHandle<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    fn new(
        handle: SieveHandle<M, SieveSender<S, M>, R>,
        receiver: dispatch::Receiver<FilteredBatch<M>>,
    ) -> Self {
        Self { handle, receiver }
    }
}

#[async_trait]
impl<M, S, R> Handle<Payload<M>, FilteredBatch<M>> for ContagionHandle<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    type Error = ContagionError;

    async fn deliver(&mut self) -> Result<FilteredBatch<M>, Self::Error> {
        self.receiver.recv().await.ok_or_else(|| Channel.build())
    }

    async fn try_deliver(&mut self) -> Result<Option<FilteredBatch<M>>, Self::Error> {
        todo!()
    }

    async fn broadcast(&mut self, payload: &Payload<M>) -> Result<(), Self::Error> {
        self.handle.broadcast(payload).await.context(SieveFail)
    }
}

impl<M, S, R> Clone for ContagionHandle<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub mod test {
    use super::*;

    use drop::test::DummyManager;

    use sieve::test::{generate_batch, generate_sieve_sequence};
    use sieve::Fixed;

    /// Generate a sequence of `Ready` Message with specified conflicts
    pub fn generate_ready_echoes(
        digest: Digest,
        conflicts: impl Iterator<Item = Sequence> + Clone,
        count: usize,
    ) -> impl Iterator<Item = ContagionMessage<u32>> {
        std::iter::repeat(conflicts)
            .map(move |seqs| ContagionMessage::Ready(digest, seqs.collect()))
            .take(count)
    }

    pub fn generate_single_echo(
        digest: Digest,
        seq: Sequence,
        count: usize,
    ) -> impl Iterator<Item = ContagionMessage<u32>> {
        std::iter::repeat(seq)
            .map(move |seq| ContagionMessage::ReadyOne(digest, seq))
            .take(count)
    }

    /// Generate the required sequence of messages in order for contagion to deliver a batch
    /// with a specified set of conflicts
    pub fn generate_contagion_sequence(
        batch_size: usize,
        peer_count: usize,
        sieve_conflicts: impl Iterator<Item = Sequence> + Clone,
        contagion_conflicts: impl Iterator<Item = Sequence> + Clone,
    ) -> impl Iterator<Item = ContagionMessage<u32>> {
        let batch = generate_batch(batch_size);
        let digest = *batch.info().digest();
        let contagion = generate_ready_echoes(digest, contagion_conflicts, peer_count);

        generate_sieve_sequence(peer_count, batch, sieve_conflicts)
            .map(Into::into)
            .chain(contagion)
    }

    async fn do_test<C>(size: usize, peers: usize, sieve_conflicts: C, contagion_conflicts: C)
    where
        C: ExactSizeIterator<Item = Sequence> + Clone,
    {
        let batch = generate_batch(size);
        let digest = *batch.info().digest();

        let sieve_conflicts = sieve_conflicts.collect::<Vec<_>>();
        let contagion_conflicts = contagion_conflicts.collect::<Vec<_>>();

        let conflicts = sieve_conflicts
            .iter()
            .copied()
            .chain(contagion_conflicts.iter().copied())
            .collect::<HashSet<_>>();

        let config = ContagionConfig::default();
        let contagion = Contagion::new(KeyPair::random(), config, Fixed::new_local());

        let messages = generate_contagion_sequence(
            size,
            peers,
            sieve_conflicts.iter().copied(),
            contagion_conflicts.iter().copied(),
        );

        let mut manager = DummyManager::new(messages, peers);

        let mut handle = manager.run(contagion).await;

        let delivery = handle.deliver().await.expect("delivery failed");

        assert_eq!(delivery.digest(), &digest, "wrong batch digest");
        assert_eq!(
            delivery.excluded_len(),
            conflicts.len(),
            "wrong number of conflicts"
        );

        batch
            .iter()
            .enumerate()
            .filter_map(|(pos, payload)| {
                if !conflicts.contains(&(pos as Sequence)) {
                    Some(payload)
                } else {
                    None
                }
            })
            .zip(delivery.iter())
            .for_each(|(expected, actual)| {
                assert_eq!(expected, actual, "bad payload delivered");
            });
    }

    #[tokio::test]
    async fn deliver_with_same_conflicts() {
        drop::test::init_logger();

        const BATCH_SIZE: usize = 40;
        const PEER_COUNT: usize = 10;
        const CONFLICTS: std::ops::Range<Sequence> = 0..CONFLICT_END as Sequence;
        const CONFLICT_END: usize = 20;

        do_test(BATCH_SIZE, PEER_COUNT, CONFLICTS, CONFLICTS).await;
    }

    #[tokio::test]
    async fn deliver_no_conflicts() {
        use std::iter;

        drop::test::init_logger();

        const BATCH_SIZE: usize = 40;
        const PEER_COUNT: usize = 20;

        do_test(BATCH_SIZE, PEER_COUNT, iter::empty(), iter::empty()).await;
    }
}
