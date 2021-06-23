#![deny(missing_docs)]

//! [`Contagion`], implements the probabilistic reliable broadcast abstraction.
//! This abstraction is strictly stronger than probabilistic consistent
//! broadcast, as it additionally guarantees e-totality.  Despite a Byzantine
//! sender, either none or every correct process delivers the broadcasted message.
//!
//! [`Contagion`]: self::Contagion

use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;

use drop::system::{
    message, ConvertSender, Handle, Message, Processor, Sampler, Sender, SenderError,
};

use futures::future;
use futures::stream::{self, Stream, StreamExt};

use postage::dispatch;
use postage::sink::Sink as _;
use postage::stream::Stream as _;

use sieve::{BatchInfo, EchoHandle, SeenHandle, Sieve, SieveError, SieveHandle, SieveMessage};
pub use sieve::{FilteredBatch, Fixed, Payload, RdvPolicy, RoundRobin, Sequence};

use serde::{Deserialize, Serialize};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{Mutex, RwLock};

use tracing::{debug, error, info, trace};

mod config;
pub use config::{ContagionConfig, ContagionConfigBuilder};

/// A `FilteredBatch` that has an expiration time
#[derive(Clone)]
struct ExpiringBatch<M>
where
    M: Message,
{
    batch: FilteredBatch<M>,
    time: Instant,
}

impl<M> ExpiringBatch<M>
where
    M: Message,
{
    /// Check if this `ExpiringBatch` is expired
    pub fn expired(&self, duration: Duration) -> bool {
        self.time.elapsed() >= duration
    }
}

impl<M> From<FilteredBatch<M>> for ExpiringBatch<M>
where
    M: Message,
{
    fn from(batch: FilteredBatch<M>) -> Self {
        Self {
            batch,
            time: Instant::now(),
        }
    }
}

impl<M> From<&FilteredBatch<M>> for ExpiringBatch<M>
where
    M: Message,
{
    fn from(batch: &FilteredBatch<M>) -> Self {
        Self {
            batch: batch.clone(),
            time: Instant::now(),
        }
    }
}

impl<M> From<&ExpiringBatch<M>> for FilteredBatch<M>
where
    M: Message,
{
    fn from(v: &ExpiringBatch<M>) -> Self {
        v.batch.clone()
    }
}

impl<M> std::ops::Deref for ExpiringBatch<M>
where
    M: Message,
{
    type Target = FilteredBatch<M>;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}
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
    /// This may happen when `BatchedContagion::setup` isn't called before starting
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
    Ready(BatchInfo, Vec<Sequence>),
    /// Echo only sequence in a batch
    ReadyOne(BatchInfo, Sequence),
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
    batches: RwLock<HashMap<BatchInfo, ExpiringBatch<M>>>,

    ready_set: RwLock<HashSet<PublicKey>>,

    echoes: EchoHandle,
}

impl<M, S, R> Contagion<M, S, R>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
    R: RdvPolicy,
{
    /// Create new `BatchedContagion` from the given configuration
    pub fn new(rdv: R, config: ContagionConfig) -> Self {
        let sieve = Sieve::new(rdv, config.sieve);

        Self {
            config,
            sieve,
            subscribers: Default::default(),
            handle: Default::default(),
            delivery: Default::default(),

            ready_set: Default::default(),

            echoes: EchoHandle::new(config.channel_cap(), "contagion"),

            seen: SeenHandle::new(config.channel_cap(), "contagion"),
            batches: Default::default(),
        }
    }

    async fn deliver_from_sieve(&self) -> Option<FilteredBatch<M>> {
        debug!("checking if sieve has any new batch to deliver");

        self.handle
            .as_ref()?
            .lock()
            .await
            .try_deliver()
            .await
            .ok()
            .flatten()
    }

    async fn deliver(&self, batch: FilteredBatch<M>) -> Result<(), ContagionError> {
        self.delivery
            .as_ref()
            .map(Clone::clone)
            .context(NotSetup)?
            .send(batch)
            .await
            .ok()
            .context(Channel)
    }

    /// Register echoes for the given set of sequences and returns a stream of sequences
    /// Process echoes for a set of excluded `Sequence`s
    /// # Returns
    /// A `Stream` containing the `Sequence`s that have reached the threshold of echoes
    async fn process_exceptions<'a>(
        &'a self,
        info: &BatchInfo,
        from: PublicKey,
        sequences: &'a [Sequence],
    ) -> impl Stream<Item = Sequence> + 'a {
        let acked = (0..info.sequence()).filter(move |x| !sequences.contains(&x));
        let echoes = self.echoes.send_many(*info.digest(), from, acked).await;
        let digest = *info.digest();

        self.echoes
            .many_conflicts(*info.digest(), from, sequences.iter().copied())
            .await;

        echoes.filter_map(move |(seq, x)| async move {
            if self.config.ready_threshold_cmp(x) {
                debug!(
                    "reached threshold to deliver payload {} of batch {}",
                    seq, digest
                );
                Some(seq)
            } else {
                debug!(
                    "only have {}/{} acks to deliver payload {} of batch {}",
                    x, self.config.ready_threshold, seq, digest
                );
                None
            }
        })
    }

    /// Check a `Stream`  of sequences to see which ones have already been delivered
    ///
    /// # Returns
    /// `Some` if at least one `Sequence` in the `Stream` has not been delivered yet,
    /// `None` otherwise
    async fn deliverable_unseen(
        &self,
        info: &BatchInfo,
        sequences: impl Stream<Item = Sequence>,
    ) -> Option<FilteredBatch<M>> {
        let digest = *info.digest();
        let echoes = self
            .echoes
            .get_many_echoes_stream(digest, sequences)
            .filter_map(|(seq, count)| {
                future::ready(if self.config.ready_threshold_cmp(count) {
                    trace!("enough echoes for {} of {}", seq, digest);
                    Some(seq)
                } else {
                    None
                })
            });

        let seen = self
            .seen
            .register_delivered(digest, echoes)
            .await
            .inspect(|seq| {
                trace!("ready to deliver {} from {}", seq, digest);
            });

        let new = seen.collect::<BTreeSet<_>>().await;
        let length = new.len();

        if new.is_empty() {
            trace!("no new sequence deliverable for {}", digest);
            None
        } else {
            debug!("new deliverable sequences from {} are {:?}", digest, new);

            self.batches
                .read()
                .await
                .get(info)
                .map(|batch| batch.into())
                .map(|batch: FilteredBatch<M>| batch.include(new))
                .map(|batch| {
                    debug_assert_eq!(batch.len(), length, "invalid final batch length");
                    batch
                })
        }
    }

    async fn deliverable_seen(
        &self,
        info: &BatchInfo,
        sequences: impl IntoIterator<Item = Sequence>,
    ) -> Option<FilteredBatch<M>> {
        let digest = *info.digest();
        let newly_seen = self
            .seen
            .register_seen(
                digest,
                stream::iter(sequences)
                    .inspect(|seq| debug!("checking delivery status for {}", seq)),
            )
            .await
            .inspect(|seq| trace!("newly seen sequence {} from {}", seq, digest));

        self.deliverable_unseen(info, newly_seen).await
    }

    async fn subscribe_to<'a, I>(&self, peers: I, sender: &S) -> Result<(), ContagionError>
    where
        I: IntoIterator<Item = &'a PublicKey>,
        I::IntoIter: Send,
    {
        sender
            .send_many(ContagionMessage::Subscribe, peers.into_iter())
            .await
            .context(Network {
                when: "subscribing to peer",
            })
    }

    async fn acknowledge_batch(
        &self,
        info: &BatchInfo,
        exclusions: impl Iterator<Item = Sequence>,
        sender: &S,
    ) -> Result<(), ContagionError> {
        debug!("send ready message for batch {}", info.digest());

        sender
            .send_many(
                ContagionMessage::Ready(*info, exclusions.collect()),
                self.subscribers.read().await.iter(),
            )
            .await
            .context(Network {
                when: "acknowledging batch",
            })
    }

    async fn purge(&self) {
        let expiration = self.config.sieve.expiration_delay();
        let mut batches = self.batches.write().await;

        // FIXME: use `HashMap::drain_filter` once stable

        let expired = batches
            .iter()
            .filter(|(_, b)| b.expired(expiration))
            .map(|(k, _)| k)
            .copied()
            .collect::<Vec<_>>();

        for info in expired {
            batches.remove(&info);
            self.seen.purge(info.digest()).await;
            self.echoes.purge(*info.digest()).await;
        }
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
        message: ContagionMessage<M>,
        from: PublicKey,
        sender: Arc<S>,
    ) -> Result<(), Self::Error> {
        match message {
            ContagionMessage::Ready(ref info, mut excluding)
                if self.ready_set.read().await.contains(&from) =>
            {
                let digest = *info.digest();

                debug!(
                    "got acknowledgements for {} excluding {:?}",
                    digest, excluding
                );

                excluding.sort_unstable();

                let ready = self.process_exceptions(info, from, &excluding).await;

                if let Some(batch) = self.deliverable_unseen(info, ready).await {
                    debug!(
                        "delivering sequences {} from {}",
                        batch.included().count(),
                        info.digest()
                    );
                    self.deliver(batch).await?;
                }
            }

            ContagionMessage::ReadyOne(ref info, sequence)
                if self.ready_set.read().await.contains(&from) =>
            {
                let digest = *info.digest();

                let (seq, echoes) = self.echoes.send(digest, from, sequence).await;

                debug!("now have {} p-acks for {} of {}", echoes, seq, digest);

                if self.config.ready_threshold_cmp(echoes) {
                    debug!(
                        "reached threshold for payload {} of batch {}",
                        sequence, digest
                    );

                    if let Some(batch) = self
                        .deliverable_unseen(info, stream::once(async move { seq }))
                        .await
                    {
                        debug!("ready to deliver payload {} from {}", sequence, digest);

                        self.deliver(batch).await?;
                    }
                }
            }

            ContagionMessage::Sieve(sieve) => {
                let sieve_sender = Arc::new(ConvertSender::new(sender.clone()));

                self.sieve
                    .process(sieve, from, sieve_sender)
                    .await
                    .context(SieveFail)?;

                if let Some(ref delivered) = self.deliver_from_sieve().await {
                    let info = *delivered.info();

                    debug!("new delivery from sieve {:?}", delivered);

                    self.batches
                        .write()
                        .await
                        .insert(*delivered.info(), delivered.into());

                    self.acknowledge_batch(delivered.info(), delivered.excluded(), &sender)
                        .await?;

                    let delivered = delivered.included().collect::<Vec<_>>();

                    debug!("received {:?} from sieve", delivered);

                    if let Some(batch) = self.deliverable_seen(&info, delivered).await {
                        debug!("delivering late sequence from batch {}", info.digest());
                        self.deliver(batch).await?;
                    }
                }
            }

            ContagionMessage::Subscribe if self.subscribers.write().await.insert(from) => {
                info!("new subscription from {}", from);

                let batches = self.batches.read().await;
                let echoes =
                    stream::iter(batches.keys())
                        .filter_map(|info| async move {
                            if let Some(seqs) = self.seen.get_seen(*info.digest()).await {
                                let seqs = seqs.collect::<Vec<_>>().await;

                                debug!(
                                    "advertising {} sequences from {} to new subscriber",
                                    seqs.len(),
                                    info.digest()
                                );

                                Some((info, seqs))
                            } else {
                                None
                            }
                        })
                        .then(|(info, exclusion)| async move {
                            ContagionMessage::Ready(*info, exclusion)
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

        self.subscribe_to(sample.iter(), &sender)
            .await
            .expect("failed to subscribe");

        self.ready_set.write().await.extend(sample);

        self.handle.replace(Mutex::new(handle.clone()));

        let (tx, rx) = dispatch::channel(self.config.channel_cap());

        self.delivery.replace(tx);

        Self::Handle::new(handle, rx)
    }

    async fn garbage_collection(&self) {
        self.purge().await;

        self.sieve.garbage_collection().await;
    }

    async fn disconnect<SA: Sampler>(&self, peer: PublicKey, sender: Arc<S>, sampler: Arc<SA>) {
        let mut ready_set = self.ready_set.write().await;

        if ready_set.remove(&peer) {
            error!("peer {} from our gossip set disconnected", peer);

            let not_gossip = sender
                .keys()
                .await
                .into_iter()
                .filter(|x| !ready_set.contains(&x));

            match sampler.sample(not_gossip, 1).await {
                Ok(new) => {
                    info!("resampled for {} new peers", new.len());

                    ready_set.extend(new.iter().copied());

                    mem::drop(ready_set);

                    if let Err(e) = self.subscribe_to(new.iter(), &sender).await {
                        error!("Failed to resubscribe after disconnect: {}", e);
                    }
                }
                Err(e) => {
                    error!("failed to resample following disconnection: {}", e);
                }
            }
        }

        let sender = Arc::new(ConvertSender::new(sender));

        self.sieve.disconnect(peer, sender, sampler).await;
    }
}

impl<M, S> Default for Contagion<M, S, sieve::Fixed>
where
    M: Message + 'static,
    S: Sender<ContagionMessage<M>>,
{
    fn default() -> Self {
        Self::new(Fixed::new_local(), ContagionConfig::default())
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
        use postage::stream::TryRecvError;

        match self.receiver.try_recv() {
            Ok(message) => Ok(Some(message)),
            Err(TryRecvError::Pending) => Ok(None),
            _ => Channel.fail(),
        }
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

    use std::iter;

    use drop::test::DummyManager;

    use sieve::test::{generate_batch, generate_sieve_sequence};
    use sieve::{Batch, Fixed};

    /// Generate a sequence of `Ready` Message with specified conflicts
    pub fn generate_ready_echoes(
        info: BatchInfo,
        conflicts: impl Iterator<Item = Sequence> + Clone,
        count: usize,
    ) -> impl Iterator<Item = ContagionMessage<u32>> {
        std::iter::repeat(conflicts)
            .map(move |seqs| ContagionMessage::Ready(info, seqs.collect()))
            .take(count)
    }

    pub fn generate_single_echo(
        info: BatchInfo,
        seq: Sequence,
        count: usize,
    ) -> impl Iterator<Item = ContagionMessage<u32>> {
        std::iter::repeat(seq)
            .map(move |seq| ContagionMessage::ReadyOne(info, seq))
            .take(count)
    }

    /// Generate the required sequence of messages in order for contagion to deliver a batch
    /// with a specified set of conflicts
    pub fn generate_contagion_sequence(
        batch: Batch<u32>,
        peer_count: usize,
        sieve_conflicts: impl Iterator<Item = Sequence> + Clone,
        contagion_conflicts: impl Iterator<Item = Sequence> + Clone,
    ) -> impl Iterator<Item = ContagionMessage<u32>> {
        let contagion = generate_ready_echoes(*batch.info(), contagion_conflicts, peer_count);

        generate_sieve_sequence(peer_count, batch, sieve_conflicts)
            .map(Into::into)
            .chain(contagion)
    }

    /// Perform a test with specified batch size, number of peers and set of conflicts for both contagion
    /// and sieve
    pub async fn do_test<C>(size: usize, peers: usize, sieve_conflicts: C, contagion_conflicts: C)
    where
        C: ExactSizeIterator<Item = Sequence> + Clone,
    {
        let batch = generate_batch(size, size);
        let digest = *batch.info().digest();

        let sieve_conflicts = sieve_conflicts.collect::<Vec<_>>();
        let contagion_conflicts = contagion_conflicts.collect::<Vec<_>>();

        let conflicts = sieve_conflicts
            .iter()
            .copied()
            .chain(contagion_conflicts.iter().copied())
            .collect::<BTreeSet<_>>();

        let config = ContagionConfig::default();
        let contagion = Contagion::new(Fixed::new_local(), config);

        let messages = generate_contagion_sequence(
            batch.clone(),
            peers,
            sieve_conflicts.into_iter(),
            contagion_conflicts.into_iter(),
        );

        let mut manager = DummyManager::new(messages, peers);

        let mut handle = manager.run(contagion).await;

        let batch = Arc::new(batch);

        if conflicts.len() == batch.info().size() {
            handle.deliver().await.expect_err("invalid delivery");
        } else {
            let mut empty = FilteredBatch::empty(batch.clone());

            while let Ok(delivery) = handle.deliver().await {
                empty.merge(delivery);
            }

            assert_eq!(
                empty.iter().count(),
                batch.info().size() - conflicts.len(),
                "iterator does not contains all payloads"
            );

            assert_eq!(empty.info().digest(), &digest, "wrong batch digest");
            assert_eq!(
                empty.excluded_len(),
                conflicts.len(),
                "wrong number of conflicts"
            );

            empty
                .included()
                .for_each(|seq| assert!(!conflicts.contains(&seq)));

            batch
                .iter()
                .filter(|payload| !conflicts.contains(&payload.sequence()))
                .zip(empty.iter())
                .for_each(|(expected, actual)| {
                    assert_eq!(expected, actual, "bad payload delivered");
                });
        }
    }

    #[tokio::test]
    async fn no_delivery_conflicts() {
        drop::test::init_logger();

        const BATCH_SIZE: usize = 40;
        const PEER_COUNT: usize = 20;
        const CONFLICTS_CONTAGION: std::ops::Range<Sequence> = 0..CONFLICT_END as Sequence;
        const CONFLICTS_SIEVE: std::ops::Range<Sequence> =
            (CONFLICT_END as Sequence)..(BATCH_SIZE * BATCH_SIZE) as Sequence;
        const CONFLICT_END: usize = 1000;

        do_test(BATCH_SIZE, PEER_COUNT, CONFLICTS_SIEVE, CONFLICTS_CONTAGION).await;
    }

    #[tokio::test]
    async fn deliver_with_same_conflicts() {
        drop::test::init_logger();

        const BATCH_SIZE: usize = 5;
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

    #[tokio::test]
    async fn single_payload_delivery() {
        use std::iter;

        drop::test::init_logger();

        const PEER_COUNT: usize = 10;
        const BLOCK_COUNT: usize = 80;
        const BLOCK_SIZE: usize = 100;
        const LATE_PAYLOAD: Sequence = 400;
        const CONFLICTS: std::ops::Range<Sequence> = 0..(BLOCK_COUNT * BLOCK_SIZE) as Sequence;

        let batch = generate_batch(BLOCK_COUNT, BLOCK_SIZE);
        let initial_conflicts =
            generate_contagion_sequence(batch.clone(), PEER_COUNT, iter::empty(), CONFLICTS);
        let late_acks = generate_single_echo(*batch.info(), LATE_PAYLOAD, PEER_COUNT);
        let messages = initial_conflicts.chain(late_acks);

        let contagion = Contagion::default();

        let mut manager = DummyManager::new(messages, PEER_COUNT);

        let mut handle = manager.run(contagion).await;

        let delivery: FilteredBatch<u32> = handle.deliver().await.expect("delivery failed");

        debug!("batch is {:?}", delivery);

        assert_eq!(delivery.len(), 1, "too many payloads delivered");
        assert_eq!(
            delivery.included().next().unwrap(),
            LATE_PAYLOAD,
            "wrong sequence delivered"
        );

        let payload = delivery.iter().next().unwrap();

        assert_eq!(payload, batch.iter().nth(LATE_PAYLOAD as usize).unwrap());
    }

    #[tokio::test]
    async fn empty_subscription() {
        const PEER_COUNT: usize = 10;

        let contagion = Contagion::default();
        let mut manager =
            DummyManager::new(iter::once(ContagionMessage::<u32>::Subscribe), PEER_COUNT);

        let mut handle = manager.run(contagion).await;

        let messages = manager.sender().messages().await;

        messages.iter().for_each(|msg| {
            assert!(matches!(
                msg.1,
                ContagionMessage::Subscribe | ContagionMessage::Sieve(_)
            ))
        });

        // there one subscription per peer per algorithm so 3 with murmur, sieve and contagion
        assert_eq!(messages.len(), PEER_COUNT * 3, "too many messages sent");

        handle.deliver().await.expect_err("delivered invalid batch");
    }

    #[tokio::test]
    async fn handle_subscription() {
        const PEER_COUNT: usize = 10;
        const BLOCK_SIZE: usize = 10;
        const BLOCK_COUNT: usize = 10;

        drop::test::init_logger();

        let contagion = Contagion::default();
        let batch = generate_batch(BLOCK_COUNT, BLOCK_SIZE);
        let info = *batch.info();

        let messages =
            generate_contagion_sequence(batch, PEER_COUNT + 1, iter::empty(), iter::empty())
                .chain(iter::repeat(ContagionMessage::Subscribe).take(PEER_COUNT));

        let mut manager = DummyManager::new(messages, PEER_COUNT);

        let mut handle = manager.run(contagion).await;

        handle.deliver().await.expect("delivery failed");

        let sent_announce = manager
            .sender()
            .messages()
            .await
            .into_iter()
            .filter(|x| matches!(x.1, ContagionMessage::Ready(_, _)))
            .collect::<Vec<_>>();

        assert_eq!(
            sent_announce.len(),
            PEER_COUNT,
            "did not announce to every subscriber"
        );

        sent_announce.iter().for_each(|(_, m)| {
            if let ContagionMessage::Ready(inf, excl) = m {
                assert_eq!(info, *inf, "wrong batch acked");
                assert!(excl.is_empty(), "wrong set of conflict announced");
            }
        })
    }
}
