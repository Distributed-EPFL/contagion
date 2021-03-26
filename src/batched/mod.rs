//! <p>This module provides  the batched version of [`Contagion`].</p>
//!
//! <p>[`BatchedContagion`] provides the same safety guarantees as regular [`Contagion`] but also
//! makes effort to reduce unnecessary network IO and latency but batching blocks of transaction
//! together.</p>
//!
//! [`Contagion`]: crate::classic::Contagion
//! [`BatchedContagion`]: crate::batched::BatchedContagion
//! [`RdvPolicy`]: crate::batched::RdvPolicy

use std::collections::HashSet;
use std::sync::Arc;

use drop::async_trait;
use drop::crypto::hash::Digest;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::KeyPair;
use drop::system::manager::Handle;
use drop::system::sender::{ConvertSender, SenderError};
use drop::system::{message, Message, Processor, Sampler, Sender};

use sieve::batched::{
    BatchedSieve, BatchedSieveError, BatchedSieveHandle, BatchedSieveMessage, EchoHandle,
};
pub use sieve::batched::{FilteredBatch, RdvPolicy, Sequence};

use serde::{Deserialize, Serialize};

use snafu::{ResultExt, Snafu};

use tokio::sync::{mpsc, Mutex, RwLock};

mod config;
pub use config::{BatchedContagionConfig, BatchedContagionConfigBuilder};

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
    gossip: RwLock<HashSet<PublicKey>>,
    handle: Option<Arc<Mutex<BatchedSieveHandle<M>>>>,
    delivery: Option<mpsc::Sender<FilteredBatch<M>>>,

    ready_agent: EchoHandle,
    deliver_agent: EchoHandle,
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
            gossip: Default::default(),
            handle: Default::default(),
            delivery: Default::default(),

            ready_agent: Default::default(),
            deliver_agent: Default::default(),
        }
    }

    async fn try_deliver(&self) -> Option<FilteredBatch<M>> {
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
        seqs: impl Iterator<Item = &Sequence>,
    ) -> Option<FilteredBatch<M>> {
        todo!(
            "register echoes for {}: {}",
            digest,
            seqs.fold("".to_string(), |mut acc, curr| {
                acc.push_str(format!("{}", curr).as_str());
                acc.push(',');
                acc
            })
        )
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
            BatchedContagionMessage::Ready(digest, except) => {
                if let Some(delivery) = self.deliverable(*digest, except.iter()).await {
                    todo!("deliver {}", delivery)
                }
            }
            BatchedContagionMessage::Sieve(sieve) => {
                let sender = Arc::new(ConvertSender::new(sender));
                self.sieve
                    .process(Arc::new(sieve.clone()), from, sender)
                    .await
                    .context(SieveError)?;

                if let Some(delivered) = self.try_deliver().await {
                    todo!("start processing second echo round for {}", delivered);
                }
            }
            BatchedContagionMessage::Subscribe => {
                if self.gossip.write().await.insert(from) {
                    todo!("broadcast everything state info required");
                }
            }
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
