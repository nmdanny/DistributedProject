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


#[derive(Debug)]
pub enum Phase {
    ClientSharing {
        // Should have 'd' shares, all of attributed to this node (same x)
        shares: Vec<Share>,
        last_share_at: time::Instant
    },
    Reconstructing {
        // Should have between 'd' to 'n*d' shares, at this point 
        shares: Vec<Vec<Share>>
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnonymityMessage {
    ClientShare { channel_shares: Vec<ShareBytes> } ,
    ServerReconstruct { channel_shares: Vec<ShareBytes> }
}


#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousLogSM<V: Value + Hash, CT: ClientTransport<AnonymityMessage>> {
    pub committed_messages: Vec<V>,
    #[derivative(Debug="ignore")]
    pub client: Client<CT, AnonymityMessage>,
    pub id: usize,
    pub config: Rc<Config>,
    pub state: Phase
}

impl <V: Value + Hash, CT: ClientTransport<AnonymityMessage>> AnonymousLogSM<V, CT> {
    pub fn new(config: Rc<Config>, id: usize, client_transport: CT) -> AnonymousLogSM<V, CT> {
        assert!(id < config.num_nodes, "Invalid node ID");
        let client = Client::new(format!("SM-{}-cl", id), client_transport, config.num_nodes);
        let state = Phase::ClientSharing {
            last_share_at: Instant::now(),
            shares: Vec::new()
        };
        AnonymousLogSM {
            committed_messages: Vec::new() ,client, id, config, state
        }
        
    }
}

#[async_trait(?Send)]
impl <V: Value + Hash, T: Transport<AnonymityMessage>, C: ClientTransport<AnonymityMessage>> StateMachine<AnonymityMessage, T> for AnonymousLogSM<V, C> {
    async fn apply(&mut self, entry: &AnonymityMessage) -> () {
        unimplemented!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}