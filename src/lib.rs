#![deny(missing_docs)]

//! Contagion, implements the probabilistic reliable broadcast abstraction.
//! This abstraction is strictly stronger than probabilistic consistent
//! broadcast, as it additionally guarantees e-totality.  Despite a Byzantine
//! sender, either none or every correct process delivers the broadcast message.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::ops::Deref;
use std::sync::Arc;

use drop::async_trait;
use drop::crypto::key::exchange::PublicKey;
use drop::crypto::sign::{self, KeyPair, Signature, Signer};
use drop::system::manager::Handle;
use drop::system::sender::ConvertSender;
use drop::system::{message, Message, Processor, Sampler, Sender, SenderError};

use futures::future;

use serde::{Deserialize, Serialize};

use sieve::classic::{Sieve, SieveError, SieveHandle, SieveMessage, SieveProcessingError};

use snafu::{OptionExt, ResultExt, Snafu};

use tokio::sync::{oneshot, Mutex, RwLock};

use tracing::{debug, error};

#[message]
pub(crate) enum ContagionMessage<M: Message> {
    #[serde(bound = "M: Message")]
    /// A message intended for the underlying `Sieve` instance
    Sieve(SieveMessage<M>),
    #[serde(bound = "M: Message")]
    /// Ready echo message
    Ready(M),
    /// Subscription request
    ReadySubscribe,
}

impl<M: Message> From<SieveMessage<(M, Signature)>> for ContagionMessage<(M, Signature)> {
    fn from(v: SieveMessage<(M, Signature)>) -> Self {
        ContagionMessage::Sieve(v)
    }
}

#[derive(Debug, Snafu)]
/// Error type returned by `Contagion::process`
pub enum ContagionProcessingError<M: Message + 'static> {
    #[snafu(display("bad signature from {}", from))]
    /// Signature is invalid for the message processed
    BadSignature {
        /// Source of the invalid message
        from: PublicKey,
    },
    #[snafu(display("network error: {}", source))]
    /// Errors were encountered when sending messages
    Network {
        /// Cause of the network error
        source: SenderError,
    },
    #[snafu(display("sieve error: {}", source))]
    /// Sieve error encountered
    SieveProcess {
        /// Sieve error content
        source: SieveProcessingError<(M, Signature)>,
    },
}

type MaybeHandle<M> = Arc<Mutex<Option<SieveHandle<(M, Signature)>>>>;

#[derive(Debug, Snafu)]
/// Errors encountered by the `Contagion` broadcast primitive
pub enum ContagionError {
    #[snafu(display("failed to broadcast message: {}", source))]
    /// Error encountered when using the sieve primitive to initially broadcast
    Broadcast {
        /// The underlying `SieveError`
        source: SieveError,
    },
    #[snafu(display("handle already used to broadcast"))]
    /// Error encountered when a `ContagionHandle` has already been used to
    /// broadcast or deliver
    AlreadyUsed,
    #[snafu(display("unable to sign message"))]
    /// The message being broadcasted could not be signed
    Sign,
    #[snafu(display("handle is not a sender"))]
    /// The `ContagionHandle` instance was not created with
    /// `Contagion::new_sender`
    NotASender,

    #[snafu(display("no message could be delivered"))]
    /// Error returned when no message could be delivered from `Handle`
    NoMessage,
}

/// A byzantine reliable probabilistic broadcast algorithm
pub struct Contagion<M: Message + 'static> {
    sender: sign::PublicKey,
    keypair: Arc<KeyPair>,

    delivery_threshold: usize,
    ready_threshold: usize,

    sieve: Arc<Sieve<(M, Signature)>>,

    handle: MaybeHandle<M>,
    delivered: Mutex<Option<(M, Signature)>>,
    deliverer: Mutex<Option<oneshot::Sender<M>>>,

    /// Set of peers who subscribed to us
    subscribers: RwLock<HashSet<PublicKey>>,

    /// The set of `Ready` messages we received from other peers
    ready: RwLock<HashSet<(M, Signature)>>,
    ready_replies: RwLock<HashMap<PublicKey, HashSet<(M, Signature)>>>,
    ready_set: RwLock<HashSet<PublicKey>>,

    delivery_set: RwLock<HashSet<PublicKey>>,
    delivery_replies: RwLock<HashMap<PublicKey, HashSet<(M, Signature)>>>,
}

impl<M: Message + 'static> Contagion<M> {
    /// Create a new `Contagion` that can only be used for delivering one
    /// message from a designated sender
    pub fn new_receiver(
        sender: sign::PublicKey,
        keypair: Arc<sign::KeyPair>,
        ready_threshold: usize,
        delivery_threshold: usize,
        echo_threshold: usize,
        sample_size: usize,
    ) -> Self {
        let sieve = Sieve::new_receiver(sender, keypair.clone(), echo_threshold, sample_size);

        Self::common_setup(sender, keypair, sieve, delivery_threshold, ready_threshold)
    }

    /// Create a `Contagion` instance that can only be used to receive one
    /// message
    pub fn new_sender(
        sender: sign::PublicKey,
        keypair: Arc<KeyPair>,
        ready_threshold: usize,
        delivery_threshold: usize,
        echo_threshold: usize,
        sample_size: usize,
    ) -> Self {
        let sieve = Sieve::new_sender(keypair.clone(), echo_threshold, sample_size);

        Self::common_setup(sender, keypair, sieve, delivery_threshold, ready_threshold)
    }

    #[inline]
    fn common_setup(
        sender: sign::PublicKey,
        keypair: Arc<KeyPair>,
        sieve: Sieve<(M, Signature)>,
        delivery_threshold: usize,
        ready_threshold: usize,
    ) -> Self {
        Self {
            sender,
            keypair,
            sieve: Arc::new(sieve),
            deliverer: Mutex::new(None),

            ready_threshold,
            delivery_threshold,

            subscribers: RwLock::new(HashSet::new()),

            ready: RwLock::new(HashSet::new()),
            ready_set: RwLock::new(HashSet::new()),
            ready_replies: RwLock::new(HashMap::with_capacity(ready_threshold)),

            delivery_set: RwLock::new(HashSet::new()),
            delivery_replies: RwLock::new(HashMap::with_capacity(delivery_threshold)),

            handle: Arc::new(Mutex::new(None)),
            delivered: Mutex::new(None),
        }
    }

    async fn check_set(
        key_set: &RwLock<HashSet<PublicKey>>,
        m_set: &RwLock<HashMap<PublicKey, HashSet<(M, Signature)>>>,
        message: &M,
        signature: Signature,
        from: PublicKey,
        threshold: usize,
    ) -> bool {
        if key_set.read().await.contains(&from) {
            if Self::update_set(m_set, message, signature, from).await {
                let count = m_set
                    .read()
                    .await
                    .values()
                    .filter(|hashset| {
                        hashset.iter().any(|(stored_message, stored_signature)| {
                            stored_message == message && stored_signature == &signature
                        })
                    })
                    .count();

                count >= threshold
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Updates a given reply set with the given message and signature.
    /// Returns `true` if the set was updated `false` otherwise
    async fn update_set(
        set: &RwLock<HashMap<PublicKey, HashSet<(M, Signature)>>>,
        message: &M,
        signature: Signature,
        from: PublicKey,
    ) -> bool {
        match set.write().await.entry(from) {
            Entry::Occupied(mut e) => e.get_mut().insert((message.clone(), signature)),
            Entry::Vacant(e) => {
                let mut new_set = HashSet::with_capacity(1);
                new_set.insert((message.clone(), signature));
                e.insert(new_set);
                true
            }
        }
    }

    /// Check if we are ready to transition to ready-to-deliver state
    async fn is_ready(&self, message: &M, signature: Signature, from: PublicKey) -> bool {
        debug!("verifying ready state");

        let ready = Self::check_set(
            &self.ready_set,
            &self.ready_replies,
            message,
            signature,
            from,
            self.ready_threshold,
        )
        .await;

        if ready {
            debug!("transitioning to ready state");
        }

        ready
    }

    async fn can_deliver(&self, message: &M, signature: Signature, from: PublicKey) -> bool {
        debug!("verifying deliver state");
        let deliver_ready = Self::check_set(
            &self.delivery_set,
            &self.delivery_replies,
            message,
            signature,
            from,
            self.delivery_threshold,
        )
        .await;

        if deliver_ready {
            let mut delivered = self.delivered.lock().await;

            match delivered.deref() {
                Some(_) => false,
                None => {
                    *delivered = Some((message.clone(), signature));
                    true
                }
            }
        } else {
            false
        }
    }

    async fn transmit_ready_set<
        'a,
        I: Iterator<Item = &'a PublicKey>,
        S: Sender<ContagionMessage<(M, Signature)>>,
    >(
        &self,
        dest: I,
        sender: Arc<S>,
    ) -> Result<(), ContagionProcessingError<M>> {
        let messages = self
            .ready
            .read()
            .await
            .iter()
            .cloned()
            .map(|x| Arc::new(ContagionMessage::Ready(x)))
            .collect::<Vec<_>>();
        let messages = Arc::new(messages);

        future::join_all(dest.zip(iter::repeat((sender, messages))).map(
            move |(dest, (sender, messages))| {
                future::join_all(
                    messages
                        .iter()
                        .cloned()
                        .zip(iter::repeat(sender))
                        .map(|(msg, sender)| async move { sender.send(msg, &dest).await }),
                )
            },
        ))
        .await;

        Ok(())
    }
}

#[async_trait]
impl<M, S> Processor<ContagionMessage<(M, Signature)>, M, M, S> for Contagion<M>
where
    S: Sender<ContagionMessage<(M, Signature)>> + 'static,
    M: Message + 'static,
{
    type Handle = ContagionHandle<M>;

    type Error = ContagionProcessingError<M>;

    async fn process(
        self: Arc<Self>,
        message: Arc<ContagionMessage<(M, Signature)>>,
        from: PublicKey,
        sender: Arc<S>,
    ) -> Result<(), Self::Error> {
        match message.deref() {
            ContagionMessage::Ready((inner, signature)) => {
                let mut signer = Signer::new(self.keypair.deref().clone());

                signer
                    .verify(signature, &self.sender, inner)
                    .map_err(|_| snafu::NoneError)
                    .context(BadSignature { from })?;

                self.ready.write().await.insert((inner.clone(), *signature));

                if self.is_ready(inner, *signature, from).await {
                    debug!("now ready to deliver");
                    sender
                        .send_many(message.clone(), self.ready_set.read().await.iter())
                        .await
                        .context(Network)?;
                }
                if self.can_deliver(inner, *signature, from).await {
                    let mut lock = self.deliverer.lock().await;
                    if let Some(chan) = lock.take() {
                        if let Err(e) = chan.send(inner.clone()) {
                            error!("error delivering message: {:?}", e);
                        }
                    } else {
                        debug!("already delivered a message");
                    }
                }
            }
            ContagionMessage::Sieve(msg) => {
                let sieve_sender = Arc::new(ConvertSender::new(sender.clone()));
                self.sieve
                    .clone()
                    .process(Arc::new(msg.clone()), from, sieve_sender)
                    .await
                    .context(SieveProcess)?;

                match self.handle.lock().await.take() {
                    Some(mut handle) => {
                        if let Ok(Some((message, signature))) = handle.try_deliver().await {
                            let mut signer = Signer::new(self.keypair.deref().clone());

                            if signer.verify(&signature, &self.sender, &message).is_ok()
                                && self.ready.write().await.insert((message, signature))
                            {
                                self.transmit_ready_set(
                                    self.subscribers.read().await.iter(),
                                    sender,
                                )
                                .await?;
                            }
                        }
                    }
                    None => {
                        debug!("already delivered message");
                    }
                }
            }
            ContagionMessage::ReadySubscribe => {
                if self.subscribers.write().await.insert(from) {
                    debug!("adding {} to subscriber set", from);
                    self.transmit_ready_set(iter::once(&from), sender).await?;
                }
            }
        }

        Ok(())
    }

    async fn output<SA: Sampler>(&mut self, sampler: Arc<SA>, sender: Arc<S>) -> Self::Handle {
        let sieve_sender = ConvertSender::new(sender.clone());
        let handle = Arc::get_mut(&mut self.sieve)
            .expect("double setup detected")
            .output(sampler.clone(), Arc::new(sieve_sender))
            .await;

        *self.handle.lock().await = Some(handle);

        let keys = sender.keys().await;

        let mut ready = self.ready_set.write().await;
        ready.extend(
            sampler
                .sample(keys.iter().copied(), self.ready_threshold)
                .await
                .expect("sampling failed"),
        );
        let mut deliver = self.delivery_set.write().await;
        deliver.extend(
            sampler
                .sample(keys.iter().copied(), self.delivery_threshold)
                .await
                .expect("sampling failed"),
        );

        let (deliver_tx, deliver_rx) = oneshot::channel();

        self.deliverer.lock().await.replace(deliver_tx);

        let contagion_handle = if self.keypair.public() == &self.sender {
            Some(self.handle.clone())
        } else {
            None
        };

        ContagionHandle::new(self.keypair.clone(), deliver_rx, contagion_handle)
    }
}

///
pub struct ContagionHandle<M: Message> {
    signer: Signer,
    incoming: Option<oneshot::Receiver<M>>,
    outgoing: Option<MaybeHandle<M>>,
}

impl<M: Message> ContagionHandle<M> {
    fn new(
        keypair: Arc<KeyPair>,
        incoming: oneshot::Receiver<M>,
        outgoing: Option<MaybeHandle<M>>,
    ) -> Self {
        Self {
            signer: Signer::new(keypair.deref().clone()),
            incoming: Some(incoming),
            outgoing,
        }
    }
}

#[async_trait]
impl<M: Message> Handle<M, M> for ContagionHandle<M> {
    type Error = ContagionError;

    async fn deliver(&mut self) -> Result<M, Self::Error> {
        self.incoming
            .take()
            .context(AlreadyUsed)?
            .await
            .map_err(|_| snafu::NoneError)
            .context(NoMessage)
    }

    async fn try_deliver(&mut self) -> Result<Option<M>, Self::Error> {
        let mut deliver = self.incoming.take().context(AlreadyUsed)?;

        match deliver.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(oneshot::error::TryRecvError::Empty) => {
                self.incoming.replace(deliver);
                Ok(None)
            }
            _ => NoMessage.fail(),
        }
    }

    async fn broadcast(&mut self, message: &M) -> Result<(), Self::Error> {
        let handle = self.outgoing.take().context(AlreadyUsed)?;
        let signature = self
            .signer
            .sign(message)
            .map_err(|_| snafu::NoneError)
            .context(Sign)?;

        let mut guard = handle.lock().await;

        guard
            .as_mut()
            .context(AlreadyUsed)?
            .broadcast(&(message.clone(), signature))
            .await
            .context(Broadcast)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use sieve::classic::test::sieve_message_sequence;

    use drop::crypto::key::exchange;
    use drop::test::*;

    const SIZE: usize = 100;
    const THRESHOLD: usize = 50;

    fn create_contagion_manager<M: Message + 'static>(
        keypair: &KeyPair,
        message: M,
        peer_count: usize,
    ) -> (DummyManager<ContagionMessage<(M, Signature)>, M>, Signature) {
        let signature = Signer::new(keypair.clone())
            .sign(&message)
            .expect("sign failed");
        let keys = keyset(peer_count).collect::<Vec<_>>();
        let messages = contagion_message_sequence(keypair, message, peer_count);
        let messages = keys.iter().cloned().cycle().zip(messages);
        let manager = DummyManager::with_key(messages, keys.clone());

        (manager, signature)
    }

    fn contagion_message_sequence<M: Message + 'static>(
        keypair: &KeyPair,
        message: M,
        peer_count: usize,
    ) -> impl Iterator<Item = ContagionMessage<(M, Signature)>> {
        let signature = Signer::new(keypair.clone())
            .sign(&message)
            .expect("sign failed");
        let message = (message, signature);
        let sieve = sieve_message_sequence(keypair, message.clone(), peer_count)
            .map(ContagionMessage::Sieve);
        let ready = (0..peer_count).map(move |_| ContagionMessage::Ready(message.clone()));

        sieve.chain(ready)
    }

    #[tokio::test]
    async fn check_set_whitebox() {
        let message = 0usize;
        let keyset = (0..SIZE)
            .map(|_| *exchange::KeyPair::random().public())
            .collect::<HashSet<_>>();
        let arg_keyset = RwLock::new(keyset.clone());
        let signature = Signer::random().sign(&message).expect("sign failed");
        let messages = iter::repeat((message, signature));
        let replies = RwLock::new(HashMap::new());

        for ((message, signature), key) in messages.zip(keyset.iter()).take(THRESHOLD - 1) {
            let result =
                Contagion::check_set(&arg_keyset, &replies, &message, signature, *key, THRESHOLD)
                    .await;

            assert!(!result, "validated set before reaching threshold");
        }

        replies.read().await.values().for_each(|set| {
            assert_eq!(set.len(), 1, "too many message inserted");
            assert!(
                set.iter()
                    .all(|(msg, sig)| { msg == &message && sig == &signature }),
                "bad message stored"
            );
        });

        let key = keyset.iter().last().unwrap();
        let last =
            Contagion::check_set(&arg_keyset, &replies, &message, signature, *key, THRESHOLD).await;

        assert!(last, "failed to deliver when reached threshold");
    }

    #[tokio::test]
    async fn blackbox_delivery() {
        init_logger();
        let keypair = KeyPair::random();
        let sender = keypair.public();
        let (mut manager, signature) = create_contagion_manager(&keypair, 0usize, SIZE);
        let processor = Contagion::new_receiver(
            *keypair.public(),
            Arc::new(KeyPair::random()),
            SIZE / 5,
            SIZE / 5,
            SIZE / 5,
            SIZE / 2,
        );

        let mut handle = manager.run(processor).await;
        let message = handle.deliver().await.expect("delivery failed");

        Signer::random()
            .verify(&signature, &sender, &0usize)
            .expect("bad signature");
        assert_eq!(message, 0usize, "wrong message");
    }
}
