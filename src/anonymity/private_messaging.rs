use std::{cell::RefCell, pin::Pin, sync::Arc};
use std::hash::Hash;
use derivative;

use crate::{consensus::{client::{ClientTransport, EventStream}, types::{Id, Value}}, crypto::*};
use futures::{Future, Stream, StreamExt, future::ready, stream::BoxStream};
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};

use super::{anonymous_client::{AnonymousClient,CommitResult}, logic::{AnonymityMessage, Config, ReconstructionResults, NewRound}};

/// A deciphered private message from some client
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateMessage<V: Value> {
    pub from: Id,
    #[serde(bound = "V: Value")]
    pub contents: V
}

#[derive(Debug, Clone)]
pub enum PMEvent<V: Value> {
    NewRoundEvent { new_round: usize },
    DecipheredMessage { 
        // #[serde(bound = "V: Value")]
        message: PrivateMessage<V>,
        channel: usize,
        round: usize }
}

/// An anonymous client capable of sending private messages to other clients
#[derive(Derivative)]
#[derivative(Debug)]
pub struct PMClient<V: Value + Hash> {
    my_id: Id,
    pki: Arc<PKISettings>,

    #[derivative(Debug="ignore")]
    anonym_client: AnonymousClient<AsymEncrypted>,

    #[derivative(Debug="ignore")]
    event_receiver: RefCell<Option<EventStream<PMEvent<V>>>>
}


impl <V: Value + Hash> PMClient<V> {

    pub fn new<CT: ClientTransport<AnonymityMessage<AsymEncrypted>>>(client_transport: CT, 
        config: Arc<Config>, pki: Arc<PKISettings>, id: Id,
        event_recv: Pin<Box<dyn Send + Stream<Item = NewRound<AsymEncrypted>>>>) -> Self 
    {
        let anonym_client = AnonymousClient::new(client_transport, config, pki.clone(), id, event_recv);

        let (tx, rx) = mpsc::unbounded_channel();
        let mut event_stream = anonym_client.event_stream().expect("Event stream shouldn't be none");
        let pki_c = pki.clone();
        tokio::spawn(async move {
            while let Some(event) = event_stream.next().await {
                let round = event.round;
                tx.send(PMEvent::NewRoundEvent { new_round: round})
                  .unwrap_or_else(|_| error!("Couldn't send PMEvent, receiver was dropped"));
                if let ReconstructionResults::Some { chan_to_val } = event.last_reconstruct_results {
                    info!("Handling reconstruction results, round: {}, chan_to_val: {:?}", round - 1, chan_to_val);
                    for (channel, val) in chan_to_val.into_iter() {
                        if val.is_err() {
                            continue;
                        }
                        let val = val.unwrap();
                        if let Some(val) = crate::crypto::asym_decrypt(&pki_c.my_key, &pki_c.my_pkey, &val) {
                            let message = bincode::deserialize(&val).expect("Couldn't deserialize deciphered ciphertext");
                            tx.send(PMEvent::DecipheredMessage {
                                message,
                                channel,
                                round: round - 1
                            }).unwrap_or_else(|_| error!("Couldn't send PMEvent, receiver was dropped"));
                        } else {
                            warn!("Failed to decrypt value at round {} channel {}", round, channel);
                        }
                    }
                }
            }
        });

        let event_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        Self {
            anonym_client,
            pki,
            my_id: id,
            event_receiver: RefCell::new(Some(Box::pin(event_stream)))
        }
    }

    #[instrument]
    pub fn send_anonymously(&self, to: Id, value: V) -> impl Future<Output = Result<CommitResult, anyhow::Error>> {
        let recipient_pkey = &self.pki.clients_keys[to].0;
        let v_ser = bincode::serialize(&PrivateMessage {
            from: self.my_id,
            contents: value
        });
        match v_ser {
            Ok(v_ser) => {
                let encrypted = crate::crypto::asym_encrypt(&recipient_pkey, v_ser);
                let send_task = self.anonym_client.send_anonymously(encrypted);
                futures::future::Either::Left(send_task)
            }
            Err(e) => futures::future::Either::Right(async {
                Err(anyhow::Error::from(e))
            })
        }
    }

    /// Can be used to be notified of new rounds(& deciphered messages) seen by the client.
    /// Should only be called once
    pub fn event_stream(&self) -> Option<EventStream<PMEvent<V>>> {
        self.event_receiver.borrow_mut().take()
    }

    pub fn config(&self) -> &Config {
        self.anonym_client.config()
    }

    pub fn client_id(&self) -> Id {
        self.anonym_client.client_id()
    }
}