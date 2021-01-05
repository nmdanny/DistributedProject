use crate::consensus::{node, types::*};
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
use std::collections::HashSet;
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

type ClientId = String;

/// Maps each server ID to the set of clients who sent him shares
/// This is maintained by each node during the share phase
type ContactGraph = Vec<HashSet<ClientId>>;

fn create_contact_graph(num_nodes: usize) -> ContactGraph {
    (0 .. num_nodes).into_iter().map(|_| HashSet::new()).collect()
}

/// Given names of live clients, that is, clients that didn't crash after sending all their shares, 
/// and a contact graph, finds the maximal set of servers who contacted all said clients.
fn find_reconstruction_group(live_clients: &HashSet<ClientId>, contact_graph: &ContactGraph) -> HashSet<Id> {
    return (0 .. contact_graph.len()).zip(contact_graph).filter_map(|(node_id, contacted_clients)| {
        if live_clients.is_subset(contacted_clients) { Some(node_id) } else { None }
    }).collect()
}


/// Given a list of clients who are 'live', that is, didn't crash before sending all their shares,
/// and all of the node's shares, sums shares from those channels
fn mix_shares_from(my_id: Id, live_clients: &HashSet<ClientId>, shares_view: &Vec<Vec<(Share, ClientId)>>) -> Option<Vec<ShareBytes>> {
    if !shares_view.into_iter().all(|chan| {
        chan.iter().map(|(_, client_id)| client_id)
            .cloned()
            .collect::<HashSet<_>>()
            .is_superset(live_clients)
    }) {
        return None;
    }
    Some(shares_view.iter().map(|chan| {
        chan.iter()
            .inspect(|(share, _)| assert_eq!(share.x, my_id as u64 + 1))
            .fold(Share::new(my_id as u64 + 1), |acc, (share, from_client)| {
                if live_clients.contains(from_client) {
                    add_shares(&acc, share)
                } else {
                    acc
                }
            })
            .to_bytes()
    }).collect())
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    ClientSharing {
        last_share_at: time::Instant,
        num_shares_seen: usize,

        /// Synchronized among all nodes, identifies which servers got shares from which clients
        contact_graph: ContactGraph,

        /// Synchronized among all nodes, identifies clients that didn't crash after sending all their shares
        live_clients: HashSet<ClientId>
    },
    Reconstructing {
        shares: Vec<Vec<Share>>
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnonymityMessage {
    ClientShare { 
        // A client must always send 'num_channels` shares to a particular server
        channel_shares: Vec<ShareBytes>,
        client_name: ClientId
    },
    /// Used by the client after he sent(or at least, tried and failed) all his shares.
    /// This is necessary to know which client shares to mix before proceeding to reconstruct
    ClientNotifyLive {
        client_name: ClientId
    },
    /// Sent whenever a server obtains a share, used to update 
    ServerNotifyGotShare {
        node_id: Id,
        client_name: ClientId
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

    // Contains 'd' channels, each channel containing shares from many clients
    #[derivative(Debug="ignore")]
    pub shares: Vec<Vec<(Share, ClientId)>>
}

impl <V: Value + Hash, CT: ClientTransport<AnonymityMessage>> AnonymousLogSM<V, CT> {
    pub fn new(config: Rc<Config>, id: usize, client_transport: CT) -> AnonymousLogSM<V, CT> {
        assert!(id < config.num_nodes, "Invalid node ID");
        let client = Client::new(format!("SM-{}-cl", id), client_transport, config.num_nodes);
        let state = Phase::ClientSharing {
            last_share_at: Instant::now(), num_shares_seen: 0,
            live_clients: HashSet::new(),
            contact_graph: create_contact_graph(config.num_nodes)
        };
        let shares = vec![Vec::new(); config.num_channels];
        AnonymousLogSM {
            committed_messages: Vec::new(), client, id, config, state, shares
        }
        
    }

    pub fn handle_client_share(&mut self, client_name: ClientId, batch: &[ShareBytes]) {
        match &mut self.state {
            Phase::Reconstructing { .. } => error!("Got client share while in reconstruct phase"),
            Phase::ClientSharing { last_share_at, num_shares_seen, 
                                   contact_graph, .. } => {
                assert_eq!(self.shares.len(), batch.len() , "client batch size mismatch with expected channels");
                let batch = batch.into_iter().map(|s| s.to_share()).collect::<Vec<_>>();
                *num_shares_seen += batch.len();
                assert!(*num_shares_seen <= self.config.num_channels * self.config.num_clients, "Got too many shares, impossible");

                debug!("Got client shares: {:?}", batch);

                for (chan, share) in (0..).zip(batch.into_iter()) {
                    assert_eq!(share.x, ((self.id + 1) as u64), "Got wrong share, bug within client");
                    self.shares[chan].push((share, client_name.clone()));
                }

                *last_share_at = Instant::now();
                contact_graph[self.id].insert(client_name);
            }
        }
    }

    pub fn handle_got_share_notification(&mut self, node_id: Id, client_name: &str) {
        if let Phase::ClientSharing {contact_graph, .. } = &mut self.state {
            contact_graph[node_id].insert(client_name.to_owned());
        } else {
            error!("Got share notification too late");
        }
    }

    pub fn handle_client_live_notification(&mut self, client_name: ClientId) {
        if let Phase::ClientSharing { live_clients, .. } = &mut self.state {
            info!("{} is live", client_name);
            live_clients.insert(client_name);
            if live_clients.len() == self.config.num_clients {
                self.begin_reconstructing();
            }
        } else {
            error!("Got client live notification at wrong phase");
        }
    }

    pub fn on_receive_reconstruct_share(&mut self, batch: &[ShareBytes], _from_node: Id) {
        assert_eq!(batch.len(), self.config.num_channels, "batch size mismatch on receive_reconstruct_share");

        // first, if we're not in re-construct stage yet, begin reconstruct
        if let Phase::ClientSharing { .. } = &self.state {
            info!("Got reconstruct share while still on ClientSharing phase");
            self.begin_reconstructing();
        }

        if let Phase::Reconstructing { shares } = &mut self.state {
            // add all shares for every channel. 
            for chan_num in 0 .. self.config.num_channels {
                let chan_new_share = batch[chan_num].to_share();
                shares[chan_num].push(chan_new_share);

                // try decoding a value once we passed the threshold
                if shares[chan_num].len() >= self.config.threshold {
                    match decode_secret::<V>(reconstruct_secret(&shares[chan_num], self.config.threshold as u64)) {
                        Ok(None) => trace!("Got nothing on channel {}", chan_num),
                        Ok(Some(decoded)) => {
                            info!("Decoded value {:?} on channel {}", decoded, chan_num);
                        }
                        Err(e) => error!("Error while decoding channel {}: {:?}", chan_num, e)
                    }
                }
            }
        }
    }


    pub fn begin_reconstructing(&mut self) {
        let live_clients = match &self.state {
            Phase::ClientSharing { live_clients, .. } => live_clients,
            _ => { panic!("Begin reconstructing twice in a row") }
        };
        let shares = mix_shares_from(self.id, &live_clients, &self.shares);
        drop(live_clients);

        let channels = vec![Vec::new(); self.config.num_channels];
        self.state = Phase::Reconstructing { shares: channels };
        info!("Beginning re-construct phase, sending to all other nodes");

        if shares.is_none() {
            error!("I am missing shares from some clients, skipping reconstruct round");
            return;
        }
        let shares = shares.unwrap();
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
    }

    pub fn begin_client_sharing(&mut self) {
        match self.state {
            Phase::ClientSharing { .. } => {
                error!("Begin client sharing while already sharing");
            }
            Phase::Reconstructing { .. } => {
                self.shares = vec![Vec::new(); self.config.num_channels];
                let num_nodes = self.config.num_nodes;
                self.state = Phase::ClientSharing { last_share_at: Instant::now(), num_shares_seen: 0, 
                    live_clients: HashSet::new(), contact_graph: create_contact_graph(num_nodes) }
            }
        }
    }
}

#[async_trait(?Send)]
impl <V: Value + Hash, T: Transport<AnonymityMessage>, C: ClientTransport<AnonymityMessage>> StateMachine<AnonymityMessage, T> for AnonymousLogSM<V, C> {
    async fn apply(&mut self, entry: &AnonymityMessage) -> () {
        match entry {
            AnonymityMessage::ClientShare { channel_shares, client_name } => { self.handle_client_share(client_name.to_owned(), channel_shares.as_slice()) },
            AnonymityMessage::ClientNotifyLive { client_name } => { self.handle_client_live_notification(client_name.to_owned()) }
            AnonymityMessage::ServerReconstruct { channel_shares, node_id } => {  self.on_receive_reconstruct_share(channel_shares.as_slice(), *node_id) }
            AnonymityMessage::ServerNotifyGotShare { node_id, client_name } => { self.handle_got_share_notification(*node_id, client_name) }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}