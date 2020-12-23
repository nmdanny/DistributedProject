use crate::anonymity::secret_sharing::*;
use crate::anonymity::logic::*;
use crate::consensus::client::{ClientTransport, Client};
use crate::consensus::types::*;
use std::{hash::Hash, rc::Rc};
use rand::distributions::{Distribution, Uniform};
use tracing_futures::Instrument;


/// Handles logic of sending a value anonymously
pub struct AnonymousClient<V: Value + Hash, CT: ClientTransport<AnonymityMessage>> {
    client: Rc<Client<CT, AnonymityMessage>>,
    config: Rc<Config>,
    phantom: std::marker::PhantomData<V>
}

impl <CT: ClientTransport<AnonymityMessage>, V: Value + Hash> AnonymousClient<V, CT> {
    pub fn new(client_transport: CT, config: Rc<Config>, client_name: String) -> Self {
        AnonymousClient {
            client: Rc::new(Client::new(client_name, client_transport, config.num_nodes)),
            config,
            phantom: Default::default()
        }
    }

    pub async fn send_anonymously(&mut self, value: V) -> Result<(), anyhow::Error> {
        // note that thread_rng is crypto-secure
        let val_channel = Uniform::new(0, self.config.num_channels).sample(&mut rand::thread_rng());
        let secret_val = encode_secret(value)?;

        // create 'd' collections of shares, one for each server
        let chan_secrets = (0.. self.config.num_channels).map(|chan| {
            let secret = if chan == val_channel { secret_val } else { encode_zero_secret() };
            let threshold = self.config.threshold as u32;
            let num_nodes = self.config.num_nodes as u32;
            create_share(secret, threshold, num_nodes)

        }).collect::<Vec<_>>();

        // create tasks for sending batches of channels for every server


        let client = self.client.clone();
        let batch_futs = (0.. self.config.num_nodes).map(|node_id| {
            let batch = chan_secrets.iter().map(|chan_shares| {
                chan_shares[node_id].to_bytes()
            }).collect();

            let client = client.clone();
            async move {
                client.submit_without_commit(node_id, AnonymityMessage::ClientShare {
                    channel_shares: batch
                }).await
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

        Ok(())
    }
}