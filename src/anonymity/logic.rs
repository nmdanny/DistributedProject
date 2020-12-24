use crate::consensus::types::*;
use crate::consensus::state_machine::StateMachine;
use crate::consensus::transport::Transport;
use crate::consensus::client::{Client, ClientTransport};
use crate::anonymity::secret_sharing::*;
use serde::{Serialize, Deserialize};
use time::Instant;
use std::boxed::Box;
use std::hash::Hash;
use std::rc::Rc;
use std::time;
use async_trait::async_trait;
use derivative;

#[derive(Debug, Clone)]
/// Configuration used for anonymous message sharing
pub struct Config {
    pub num_nodes: usize,
    pub num_clients: usize,
    pub threshold: usize,
    pub num_channels: usize,
    pub phase_length: std::time::Duration
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    ClientSharing {
        // Contains 'd' channels, each channel containing shares from many clients
        last_share_at: time::Instant,
        num_shares_seen: usize
    },
    Reconstructing 
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnonymityMessage {
    ClientShare { channel_shares: Vec<ShareBytes> } ,
    ServerReconstruct { channel_shares: Vec<ShareBytes>, node_id: Id }
}


#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousLogSM<V: Value + Hash, CT: ClientTransport<AnonymityMessage>> {
    pub committed_messages: Vec<V>,
    #[derivative(Debug="ignore")]
    pub client: Client<CT, AnonymityMessage>,
    pub id: Id,
    pub config: Rc<Config>,
    pub state: Phase,
    pub shares: Vec<Vec<Share>>
}

impl <V: Value + Hash, CT: ClientTransport<AnonymityMessage>> AnonymousLogSM<V, CT> {
    pub fn new(config: Rc<Config>, id: usize, client_transport: CT) -> AnonymousLogSM<V, CT> {
        assert!(id < config.num_nodes, "Invalid node ID");
        let client = Client::new(format!("SM-{}-cl", id), client_transport, config.num_nodes);
        let state = Phase::ClientSharing {
            last_share_at: Instant::now(), num_shares_seen: 0
        };
        let shares = vec![Vec::new(); config.num_channels];
        AnonymousLogSM {
            committed_messages: Vec::new(), client, id, config, state, shares
        }
        
    }

    pub async fn handle_client_share(&mut self, batch: &[ShareBytes]) {
        match &mut self.state {
            Phase::Reconstructing { .. } => error!("Got client share while in reconstruct phase"),
            Phase::ClientSharing { last_share_at, num_shares_seen } => {
                assert_eq!(self.shares.len(), batch.len() , "client batch size mismatch with expected channels");
                let batch = batch.into_iter().map(|s| s.to_share()).collect::<Vec<_>>();
                *num_shares_seen += batch.len();
                assert!(*num_shares_seen <= self.config.num_channels * self.config.num_clients, "Got too many shares, impossible");

                info!("Got client shares: {:?}", batch);
                for (chan, share) in (0..).zip(batch.into_iter()) {
                    assert_eq!(share.x, ((self.id + 1) as u64).into(), "Got wrong share, bug within client");
                    if self.shares[chan].is_empty() {
                        self.shares[chan].push(share);
                    } else {
                        assert_eq!(self.shares[chan].len(), 1);
                        let prev_share = &mut self.shares[chan][0];
                        *prev_share = add_shares(prev_share, &share);
                    }
                }

                *last_share_at = Instant::now();

                if *num_shares_seen == self.config.num_channels * self.config.num_clients {
                    self.begin_reconstructing().await;
                }

            }
        }
    }

    #[instrument]
    pub async fn on_receive_reconstruct_share(&mut self, batch: &[ShareBytes], from_node: Id) {
        info!("Got request to reconstruct share");
    }

    #[instrument]
    pub async fn on_fully_reconstruct(&mut self, chan: usize, val: &V) {

    }

    #[instrument]
    pub async fn begin_reconstructing(&mut self) {
        match self.state {
            Phase::ClientSharing { ..  } => { 
                self.state = Phase::Reconstructing {};
                info!("Beginning re-construct phase, sending to all other nodes");
                let shares = self.shares.iter().flatten().map(|s| s.to_bytes()).collect::<Vec<_>>();
                let id = self.id;
                
                // This must be done in another task, because as part of submit value, we will also submit a value to
                // our own node, whose state machine is busy handling this one, therefore, never getting an answer
                let _ = self.client.submit_value(
                    AnonymityMessage::ServerReconstruct {
                        channel_shares: shares,
                        node_id: id
                    }
                ).await.map_err(|e| {
                    error!("Error submitting my shares: {:?}", e);
                }).map(|(res, _)| {
                    trace!("Share submitted and committed at {}", res);
                });
                
            },
            Phase::Reconstructing => { 
                error!("Begin reconstructing twice in a row");
            }
        } 
    }

    pub fn begin_client_sharing(&mut self) {
        self.state = match self.state {
            s @ Phase::ClientSharing { ..  } => {
                error!("Begin client sharing while already sharing");
                s
            },
            Phase::Reconstructing => {
                self.shares = vec![Vec::new(); self.config.num_channels];
                Phase::ClientSharing { last_share_at: Instant::now(), num_shares_seen: 0 }
            }
        }
    }
}

#[async_trait(?Send)]
impl <V: Value + Hash, T: Transport<AnonymityMessage>, C: ClientTransport<AnonymityMessage>> StateMachine<AnonymityMessage, T> for AnonymousLogSM<V, C> {
    async fn apply(&mut self, entry: &AnonymityMessage) -> () {
        match entry {
            AnonymityMessage::ClientShare { channel_shares } => { self.handle_client_share(channel_shares.as_slice()).await },
            AnonymityMessage::ServerReconstruct { channel_shares, node_id } => {  self.on_receive_reconstruct_share(channel_shares.as_slice(), *node_id).await }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}