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
use tracing_futures::Instrument;

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
    ClientShare { 
        // A client must always send 'num_channels` shares to a particular server
        channel_shares: Vec<ShareBytes> 
    },
    ServerReconstruct { 
        channel_shares: Vec<ShareBytes>, 
        node_id: Id 
    }
}


#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousLogSM<V: Value + Hash, CT: ClientTransport<AnonymityMessage>> {

    pub committed_messages: Vec<V>,

    #[derivative(Debug="ignore")]
    pub client: Client<CT, AnonymityMessage>,

    pub id: Id,
    
    #[derivative(Debug="ignore")]
    pub config: Rc<Config>,

    pub state: Phase,

    #[derivative(Debug="ignore")]
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

    pub fn handle_client_share(&mut self, batch: &[ShareBytes]) {
        match &mut self.state {
            Phase::Reconstructing { .. } => error!("Got client share while in reconstruct phase"),
            Phase::ClientSharing { last_share_at, num_shares_seen } => {
                assert_eq!(self.shares.len(), batch.len() , "client batch size mismatch with expected channels");
                let batch = batch.into_iter().map(|s| s.to_share()).collect::<Vec<_>>();
                *num_shares_seen += batch.len();
                assert!(*num_shares_seen <= self.config.num_channels * self.config.num_clients, "Got too many shares, impossible");

                debug!("Got client shares: {:?}", batch);
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
                    self.begin_reconstructing();
                }

            }
        }
    }

    pub fn on_receive_reconstruct_share(&mut self, batch: &[ShareBytes], from_node: Id) {
        if self.id == from_node {
            return;
        }
        assert_eq!(batch.len(), self.config.num_channels, "batch size mismatch on receive_reconstruct_share");

        // first, if we're not in re-construct stage yet, begin reconstruct
        if let Phase::ClientSharing { .. } = &self.state {
            info!("Got reconstruct share while still on ClientSharing phase");
            self.begin_reconstructing();
        }

        // add all shares for every channel. 
        for chan_num in 0 .. self.config.num_channels {
            let chan_new_share = batch[chan_num].to_share();
            assert!(chan_new_share.x != ((self.id + 1) as u64).into(), 
                "impossible, a ServerReconstruct share message can only contain shares of a different ID");
            self.shares[chan_num].push(chan_new_share);

            // try decoding a value once we passed the threshold
            if self.shares[chan_num].len() >= self.config.threshold {
                self.reconstruct_channel(chan_num);
            }
        }
    }

    pub fn reconstruct_channel(&mut self, chan: usize) {
        let shares = &self.shares[chan];
        let val = reconstruct_secret(shares, self.config.threshold as u32);
        let decoded = decode_secret(val);
        match decoded {
            Ok(None) => {
                trace!("Got nothing on channel {}", chan);
            },
            Ok(Some(decoded)) => {
                info!("Decoded value {:?} on channel {}", decoded, chan);
                self.commit_decoded_secret(decoded);
            }
            Err(e) => {
                error!("Error while decoding channel {}: {:?}", chan, e);
            }
        }
    }

    pub fn commit_decoded_secret(&mut self, val: V) {
    }

    pub fn begin_reconstructing(&mut self) {
        match self.state {
            Phase::ClientSharing { ..  } => { 
                self.state = Phase::Reconstructing {};
                info!("Beginning re-construct phase, sending to all other nodes");
                let shares = self.shares.iter().flatten().map(|s| s.to_bytes()).collect::<Vec<_>>();
                let id = self.id;
                
                // This must be done in another task, otherwise we will get a deadlock
                // (see explanation in `state_machine` trait)
                let mut client = self.client.clone();
                tokio::task::spawn_local(async move {
                    let res = client.submit_value(
                        AnonymityMessage::ServerReconstruct {
                            channel_shares: shares,
                            node_id: id
                        }
                    ).await;
                    match res {
                        Ok(_) => trace!("Sent all my shares successfully"),
                        Err(e) => error!("There was an error sending my shares: {:?}", e)
                    }

                }.instrument(info_span!("begin_reconstructing", id=?self.id)));
                
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
            AnonymityMessage::ClientShare { channel_shares } => { self.handle_client_share(channel_shares.as_slice()) },
            AnonymityMessage::ServerReconstruct { channel_shares, node_id } => {  self.on_receive_reconstruct_share(channel_shares.as_slice(), *node_id) }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}