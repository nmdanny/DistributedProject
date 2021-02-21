use crate::{anonymity::secret_sharing::*, consensus::client::EventStream};
use crate::anonymity::logic::*;
use crate::anonymity::callbacks::*;
use crate::consensus::client::{ClientTransport, Client};
use crate::consensus::types::*;
use std::{cell::RefCell, hash::Hash, pin::Pin, rc::Rc};
use std::collections::{VecDeque, HashMap};
use futures::{Future, Stream, StreamExt, channel::mpsc::Receiver};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use rand::distributions::{Distribution, Uniform};
use tokio_stream::StreamMap;
use tracing_futures::Instrument;
use derivative;
use std::sync::Arc;


/// Combines NewRound streams(ideally from at least a majority of the servers) onto a single stream, allowing to handle events
/// from many servers in case some are faulty
pub fn combined_subscriber<V: Value>(receivers: impl Iterator<Item = Pin<Box<dyn Send + Stream<Item = NewRound<V>>>>>) 
    -> Pin<Box<dyn Send + Stream<Item = NewRound<V>>>> {

    let mut stream_map = StreamMap::new();

    for (i, stream) in (0 ..).zip(receivers) {
        let _res = stream_map.insert(i, stream);
        assert!(_res.is_none());
    }

    let mut next_round_to_send = 0;
    let combined_stream = stream_map.filter_map(move |(_, round)| {
        let res = if round.round >= next_round_to_send {
            next_round_to_send = round.round + 1;
            Some(round)
        } else { None };
        futures::future::ready(res)
    });

    Box::pin(combined_stream)
}


#[derive(Derivative)]
#[derivative(Debug)]
/// Handles logic of sending a value anonymously
struct AnonymousClientInner<V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> {
    #[derivative(Debug="ignore")]
    mut_client: Client<CT, AnonymityMessage<V>>,

    #[derivative(Debug="ignore")]
    client: Arc<Client<CT, AnonymityMessage<V>>>,

    #[derivative(Debug="ignore")]
    config: Arc<Config>,

    pub client_id: Id

}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousClient<V: Value + Hash> {
    #[derivative(Debug="ignore")]
    handle: JoinHandle<()>,

    #[derivative(Debug="ignore")]
    send_anonym_queue: mpsc::UnboundedSender<(V, CommitResolver)>,

    id: Id,

    receiver: RefCell<Option<mpsc::UnboundedReceiver<NewRound<V>>>>,

    config: Arc<Config>


}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct CommitResult { 
    pub round: usize,
    pub channel: usize
}

type CommitResolver = oneshot::Sender<Result<CommitResult, anyhow::Error>>;

/// Contains a value sent/enqueued by the client but not yet committed
#[derive(Derivative)]
#[derivative(Debug)]
struct ToBeCommitted<V> {
    value: V,

    /// At which channel and round the value was submitted. Initially
    /// None when the value is enqueued.
    channel_and_round: Option<(usize, usize)>,

    /// Used to notify that
    #[derivative(Debug="ignore")]
    resolver: CommitResolver
}

/// Given a NewRound event and a value which was sent, determines if that value should be resolved
fn was_value_committed<V: Value + PartialEq>(new_round: &NewRound<V>, last_sent: &ToBeCommitted<V>) -> bool {
        last_sent.channel_and_round
            .filter(|(_, round)| *round == new_round.round - 1)
            .and_then(|(channel, _)| match &new_round.last_reconstruct_results {
                ReconstructionResults::TimedOut => { None }
                ReconstructionResults::Some { chan_to_val } => { chan_to_val.get(&channel)}
            })
            // if the client is faulty, he might've sent a value via the channel
            // but it wasn't mixed during reconstruction, and another client had gotten its
            // value committed to that channel.
            // 
            // NOTE: in case another client has sent an identical value, no way to tell if it's his or ours
            // other than adding some unique one-time identifier generated by the client when the message was sent.
            .filter(|res| res.as_ref() == Ok(&last_sent.value))
            .is_some()
}

impl <V: Value + Hash> AnonymousClient<V> {
    pub fn new<CT: ClientTransport<AnonymityMessage<V>>>(client_transport: CT, 
        config: Arc<Config>, id: Id,
        mut event_recv: Pin<Box<dyn Send + Stream<Item = NewRound<V>>>>) -> Self 
    {
        let mut client = AnonymousClientInner::new(client_transport, config.clone(), id);

        let (tx, mut rx) = mpsc::unbounded_channel();

        let (v_sender, v_recv) = mpsc::unbounded_channel();

        let orig_tx = tx.clone();

        let handle = tokio::spawn(async move {
            // contains values in the order they were 
            let mut uncommited_queue = VecDeque::<ToBeCommitted<V>>::new();
            let mut round = 0usize;

            let mut sent_for_round = -1i64;

            // used to trigger sending of shares every new round, if any
            let (notify, mut notify_rx) = watch::channel(());

            loop {
               tokio::select! {
                   // handle client requests
                   Some((value, resolver)) = rx.recv() => {
                       uncommited_queue.push_back(ToBeCommitted {
                           value, channel_and_round: None, resolver: resolver
                       });
                       notify.send(()).unwrap();
                   },
                
                   // try sending a new value for the current round
                   Ok(()) = notify_rx.changed() => {
                       if let Some(tbc) = uncommited_queue.get_mut(0) {
                           if sent_for_round < round as i64 {
                                sent_for_round = round as i64;
                                info!("Found that its time to submit my value {:?}, at round {}", tbc, round);
                                let send_res = client.send_anonymously(Some(tbc.value.clone()), round).await;
                                match send_res {
                                    Ok((sec_channel, succ_nodes)) => {
                                        tbc.channel_and_round = Some((sec_channel, round));
                                        let succ_count = succ_nodes.len();
                                        info!("Sent {:?} for round {} via channel {} to {} nodes: {:?}", tbc.value, round, sec_channel, succ_count, succ_nodes);
                                    },
                                    Err(e) => {
                                        error!("Client couldn't send {:?} due to error {:?}", tbc.value, e);
                                        let tbc = uncommited_queue.pop_front().unwrap();
                                        tbc.resolver.send(Err(e)).unwrap();
                                    }
                                }
                           }
                       } else {
                            let send_res = client.send_anonymously(None, round).await;
                            if let Err(e) = send_res {
                                error!("Client couldn't send zero value: {:?}", e);
                            }

                       }
                   },

                   // handle new rounds
                   Some(new_round) = event_recv.next() => {
                       trace!("Saw new round: {:?}", new_round);

                       // ignore delayed new-round messages
                       if new_round.round < round {
                           continue;
                       }
                       // check if the previous value was committed
                       if !uncommited_queue.is_empty() && was_value_committed(&new_round, &uncommited_queue[0]) {
                           let tbc = uncommited_queue.pop_front().unwrap();
                           let (channel, round) = tbc.channel_and_round.unwrap();
                           info!("My value {:?} was committed at channel {} and round {}", tbc.value, channel, round);
                           tbc.resolver.send(Ok(CommitResult { round, channel})).unwrap();
                       } 


                       round = new_round.round;

                        v_sender.send(new_round).unwrap_or_else(|_e| 
                            error!("Receiver of NewRound channel has been dropped")
                        );

                       // try sending an uncommitted value
                       notify.send(()).unwrap();
                   }
               } 
            }
        }.instrument(info_span!("anonym_client_loop", id=?id)));

        AnonymousClient {
            handle,
            send_anonym_queue: orig_tx,
            id,
            receiver: RefCell::new(Some(v_recv)),
            config
        }

    }


    #[instrument]
    pub fn send_anonymously(&self, value: V) -> impl Future<Output = Result<CommitResult, anyhow::Error>> {
        let (tx, rx) = oneshot::channel();
        self.send_anonym_queue.send((value, tx)).expect("AnonymousClient send receiver dropped early");
        async move {
            let res = rx.await.expect("AnonymousClient resolver dropped")?;
            Ok(res)
        }
    }

    /// Can be used to be notified of new rounds(& reconstructed values) seen by the client.
    /// Should only be called once
    pub fn event_stream(&self) -> Option<EventStream<NewRound<V>>> {
        self.receiver.borrow_mut().take().map(|rec| {
            tokio_stream::wrappers::UnboundedReceiverStream::new(rec).boxed()
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn client_id(&self) -> Id {
        self.id
    }
}

impl <CT: ClientTransport<AnonymityMessage<V>>, V: Value + Hash> AnonymousClientInner<V, CT> {
    fn new(client_transport: CT, config: Arc<Config>, client_id: Id) -> Self {
        let client_name = format!("Client {}", client_id);
        AnonymousClientInner {
            mut_client: Client::new(client_name.clone(), client_transport.clone(), config.num_nodes),
            client: Arc::new(Client::new(client_name, client_transport, config.num_nodes)),
            config,
            client_id
        }
    }

    /// Sends a value anonymously, returning the channel via it was sent and the nodes that got the shares
    #[instrument]
    async fn send_anonymously(&mut self, value: Option<V>, round: usize) -> Result<(usize, Vec<Id>), anyhow::Error> {
        // note that thread_rng is crypto-secure
        let val_channel = Uniform::new(0, self.config.num_channels).sample(&mut rand::thread_rng());
        let secret_val = if let Some(value) = value { encode_secret(value)? } else { encode_zero_secret() };
        let zero_val = encode_zero_secret();


        // create 'd' collections of shares, one for each server
        let chan_secrets = (0.. self.config.num_channels).map(|chan| {
            let secret = if chan == val_channel { &secret_val } else { &zero_val };
            let threshold = self.config.threshold as u64;
            let num_nodes = self.config.num_nodes as u64;
            create_share(secret.clone(), threshold, num_nodes)
        }).collect::<Vec<_>>();



        // create tasks for sending batches of channels for every server
        let client = self.client.clone();
        let timeout_duration = self.config.phase_length / 2;
        let mut batch_futs = (0.. self.config.num_nodes).map(|node_id| {
            let batch = chan_secrets.iter().map(|chan_shares| {
                chan_shares[node_id].clone()
            }).collect();

            let client = client.clone();
            let client_id = self.client_id.clone();
            tokio::spawn(async move {
                on_anonym_client_send(client_id, round, Some(node_id));
                let submit_fut = client.submit_without_commit(node_id, AnonymityMessage::ClientShare {
                    channel_shares: batch, client_id, round
                }); //.await.map_err(|e| e.context(format!("while sending to server ID {}", node_id)));
                let res = tokio::time::timeout(timeout_duration, submit_fut).await;
                (match res {
                    Ok(inner) => { inner }
                    Err(_elapsed) => { Err(anyhow::anyhow!("Timeout of duration {:?} elapsed while sending to {}", timeout_duration, node_id)) }
                }, node_id)
            }.instrument(info_span!("send_share", round=?round, to=?node_id)))
        }).collect::<Vec<_>>();

        debug!("Client {} beginning to send shares for round {} via channel {}", self.client_id, round, val_channel);
        let mut succ_nodes = Vec::new();
        while !batch_futs.is_empty() {
            match futures::future::select_all(batch_futs).await {
                (Ok((Err(e), node_id)), _index, remaining) => {
                    error!("Got an error while sending shares to server {}: {:?}", node_id, e);
                    batch_futs = remaining;
                },
                (Err(e), _index, remaining) => {
                    error!("Got an error while joining send-share task: {:?}", e);
                    batch_futs = remaining;
                },
                (Ok((Ok(_res), node_id)), _index, remaining) => {
                    succ_nodes.push(node_id);
                    batch_futs  = remaining;
                }
            }
        }

        on_anonym_client_send(self.client_id, round, None);
        debug!("Node {} submitting liveness for round {}", self.client_id, round);
        match self.mut_client.submit_value(AnonymityMessage::ClientNotifyLive { client_id: self.client_id.clone(), round: round }).await {
            Ok(_) => {}
            Err(e) => { error!("Couldn't notify that I am live to some servers for round {}: {:?}", round, e) }
        }

        Ok((val_channel, succ_nodes))
    }
}