use crate::anonymity::secret_sharing::*;
use crate::anonymity::logic::*;
use crate::consensus::client::{ClientTransport, Client};
use crate::consensus::types::*;
use std::{hash::Hash, rc::Rc};
use std::collections::VecDeque;
use tokio::sync::{watch, mpsc};
use tokio::task::JoinHandle;
use rand::distributions::{Distribution, Uniform};
use tracing_futures::Instrument;
use derivative;

#[derive(Derivative)]
#[derivative(Debug)]
/// Handles logic of sending a value anonymously
struct AnonymousClientInner<V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> {
    #[derivative(Debug="ignore")]
    mut_client: Client<CT, AnonymityMessage<V>>,

    #[derivative(Debug="ignore")]
    client: Rc<Client<CT, AnonymityMessage<V>>>,

    #[derivative(Debug="ignore")]
    config: Rc<Config>,

    #[derivative(Debug="ignore")]
    phantom: std::marker::PhantomData<V>,

    pub client_name: String

}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousClient<V: Value + Hash> {
    #[derivative(Debug="ignore")]
    handle: JoinHandle<()>,

    #[derivative(Debug="ignore")]
    send_anonym_queue: mpsc::UnboundedSender<V>,

    pub client_name: String

}

struct ToBeCommitted<V> {
    value: V,
    channel_and_round: Option<(usize, usize)>
}

fn was_value_committed<V: Value>(new_round: &NewRound<V>, last_sent: &ToBeCommitted<V>) -> Option<()> {
    {
        let (channel, round) = last_sent.channel_and_round?;
        if new_round.round != round + 1 {
            return None
        }
        let recon_res = new_round.last_reconstruct_results.as_ref()?;
        if let Ok(val) = &recon_res[channel] {
            assert_eq!(val, &last_sent.value);
            info!("Client saw that his value {:?} was committed at round {}", val, round);
            return Some(());
        }
        return None;
    }
}

impl <V: Value + Hash> AnonymousClient<V> {
    pub fn new<CT: ClientTransport<AnonymityMessage<V>>>(client_transport: CT, 
        config: Rc<Config>, client_name: String, 
        mut event_recv: mpsc::UnboundedReceiver<NewRound<V>>) -> Self 
    {
        let mut client = AnonymousClientInner::new(client_transport, config, client_name.clone());

        let (tx, mut rx) = mpsc::unbounded_channel();


        let orig_tx = tx.clone();

        let handle = tokio::task::spawn_local(async move {
            // contains values in the order they were 
            let mut uncommited_queue = VecDeque::<ToBeCommitted<V>>::new();
            let mut round = 0;

            let mut sent_for_round = -1i64;

            // used to trigger sending of shares every new round, if any
            let (notify, mut notify_rx) = watch::channel(());

            loop {
               tokio::select! {
                   // handle client requests
                   Some(value) = rx.recv() => {
                       uncommited_queue.push_back(ToBeCommitted {
                           value, channel_and_round: None
                       });
                       notify.broadcast(()).unwrap();
                   },
                
                   // try sending a new value for the current round
                   Some(()) = notify_rx.recv() => {
                       if let Some(tbc) = uncommited_queue.get_mut(0) {
                           if sent_for_round < round as i64 {
                                sent_for_round = round as i64;
                                if let Ok(sec_channel) = client.send_anonymously(tbc.value.clone(), round).await {
                                        tbc.channel_and_round = Some((sec_channel, round));
                                        info!("Sending {:?} for round {} via channel {}", tbc.value, round, sec_channel);
                                } else {
                                    error!("Client Couldn't send {:?}", tbc.value);
                                }
                           }
                       } else {
                           // TODO: send a zero value instead

                       }
                   },

                   // handle new rounds
                   Some(new_round) = event_recv.recv() => {
                       trace!("Saw new round: {:?}", new_round);
                       // check if the previous value was committed
                        if !uncommited_queue.is_empty() && was_value_committed(&new_round, &uncommited_queue[0]).is_some() {
                            let _ = uncommited_queue.pop_front();
                        }

                       // update the round
                       round = new_round.round;
                       // try sending an uncommitted value
                       notify.broadcast(()).unwrap();
                   }
               } 
            }
        }.instrument(info_span!("anonym_client_loop", name=?client_name.clone())));

        AnonymousClient {
            handle,
            send_anonym_queue: orig_tx,
            client_name
        }

    }

    #[instrument]
    pub async fn send_anonymously(&mut self, value: V) -> Result<(), anyhow::Error> {
        self.send_anonym_queue.send(value)?;
        Ok(())
    }
}

impl <CT: ClientTransport<AnonymityMessage<V>>, V: Value + Hash> AnonymousClientInner<V, CT> {
    fn new(client_transport: CT, config: Rc<Config>, client_name: String) -> Self {
        AnonymousClientInner {
            mut_client: Client::new(client_name.clone(), client_transport.clone(), config.num_nodes),
            client: Rc::new(Client::new(client_name.clone(), client_transport, config.num_nodes)),
            config,
            phantom: Default::default(),
            client_name
        }
    }

    /// Sends a value anonymously, returning the channel via it was sent
    #[instrument]
    async fn send_anonymously(&mut self, value: V, round: usize) -> Result<usize, anyhow::Error> {
        // note that thread_rng is crypto-secure
        let val_channel = Uniform::new(0, self.config.num_channels).sample(&mut rand::thread_rng());
        let secret_val = encode_secret(value)?;


        // create 'd' collections of shares, one for each server
        let chan_secrets = (0.. self.config.num_channels).map(|chan| {
            let secret = if chan == val_channel { secret_val } else { encode_zero_secret() };
            let threshold = self.config.threshold as u64;
            let num_nodes = self.config.num_nodes as u64;
            create_share(secret, threshold, num_nodes)

        }).collect::<Vec<_>>();

        // create tasks for sending batches of channels for every server


        let client = self.client.clone();
        let batch_futs = (0.. self.config.num_nodes).map(|node_id| {
            let batch = chan_secrets.iter().map(|chan_shares| {
                chan_shares[node_id].to_bytes()
            }).collect();

            let client = client.clone();
            let client_name = self.client_name.clone();
            async move {
                client.submit_without_commit(node_id, AnonymityMessage::ClientShare {
                    channel_shares: batch, client_name, round
                }).await.map_err(|e| e.context(format!("while sending to server ID {}", node_id)))
            }
        });

        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {
            let handles = batch_futs.into_iter().map(|f| tokio::task::spawn_local(f));
            let results = futures::future::join_all(handles).await;
            for result in results {
                match result {
                    Ok(Err(e)) => error!("Got an error while sending shares to servers: {:?}", e),
                    Err(e) => error!("There was an error when joining a send-share task: {:?}", e),
                    _ => {}
                }
            }
        }).await;

        match self.mut_client.submit_value(AnonymityMessage::ClientNotifyLive { client_name: self.client_name.clone(), round: round }).await {
            Ok(_) => {}
            Err(_) => { error!("Couldn't notify that I am live") }
        }

        Ok(val_channel)
    }
}