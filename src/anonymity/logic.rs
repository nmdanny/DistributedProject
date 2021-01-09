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
use futures::stream::{Stream, StreamExt};
use tokio::sync::broadcast;
use tokio::time::{Interval, Duration};
use std::collections::HashMap;

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

    debug!("node {} is trying to mix his shares from clients {:?}, with view {:?}", my_id, live_clients, shares_view);

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


#[derive(Derivative, Clone, PartialEq, Eq)]
#[derivative(Debug)]
pub enum Phase {
    ClientSharing {
        last_share_at: time::Instant,

        #[derivative(Debug="ignore")]
        /// Synchronized among all nodes, identifies which servers got shares from which clients
        contact_graph: ContactGraph,

        /// Synchronized among all nodes, identifies clients that didn't crash after sending all their shares
        live_clients: HashSet<ClientId>,

        // Contains 'd' channels, each channel containing shares from many clients
        #[derivative(Debug="ignore")]
        shares: Vec<Vec<(Share, ClientId)>>
    },
    Reconstructing {
        shares: Vec<Vec<Share>>,

        last_share_at: time::Instant
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconstructError {
    Collision, NoValue
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnonymityMessage<V: Value> {
    ClientShare { 
        // A client must always send 'num_channels` shares to a particular server
        channel_shares: Vec<ShareBytes>,
        client_name: ClientId,
        round: usize
    },
    /// Used by the client after he sent(or at least, tried and failed) all his shares.
    /// This is necessary to know which client shares to mix before proceeding to reconstruct
    ClientNotifyLive {
        client_name: ClientId,
        round: usize
    },
    /// Sent whenever a server obtains a share, used to update 
    ServerNotifyGotShare {
        node_id: Id,
        client_name: ClientId,
        round: usize
    },
    ServerReconstruct { 
        channel_shares: Vec<ShareBytes>, 
        node_id: Id,
        round: usize
    },
    ReconstructResult {
        /// Maps each channel to the client's secret, or None upon collision
        #[serde(bound = "V: Value")]
        results: Vec<Result<V, ReconstructError>>,
        round: usize
    }
}

const NEW_ROUND_CHAN_SIZE: usize = 1024;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousLogSM<V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> {

    pub committed_messages: Vec<V>,

    #[derivative(Debug="ignore")]
    pub client: Client<CT, AnonymityMessage<V>>,

    pub id: Id,
    
    #[derivative(Debug="ignore")]
    pub config: Rc<Config>,

    pub state: Phase,

    pub round: usize,

    #[derivative(Debug="ignore")]
    pub new_round_sender: broadcast::Sender<NewRound<V>>,


    /// Maps rounds from the future to a list of clients along with their shares
    /// This is used to handle share requests from future rounds
    #[derivative(Debug="ignore")]
    pub round_to_shares: HashMap<usize, Vec<(String, Vec<ShareBytes>)>>
}


impl <V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> AnonymousLogSM<V, CT> {
    pub fn new(config: Rc<Config>, id: usize, client_transport: CT) -> AnonymousLogSM<V, CT> {
        assert!(id < config.num_nodes, "Invalid node ID");
        let client = Client::new(format!("SM-{}-cl", id), client_transport, config.num_nodes);
        let state = Phase::ClientSharing {
            last_share_at: Instant::now(),
            live_clients: HashSet::new(),
            contact_graph: create_contact_graph(config.num_nodes),
            shares: vec![Vec::new(); config.num_channels]
        };
        let (nr_tx, _nr_rx) = broadcast::channel(NEW_ROUND_CHAN_SIZE);
        AnonymousLogSM {
            committed_messages: Vec::new(), client, id, config, state, round: 0, 
            new_round_sender: nr_tx,
            round_to_shares: HashMap::new()
        }
        
    }

    #[instrument]
    pub fn handle_client_share(&mut self, client_name: ClientId, batch: &[ShareBytes], round: usize) {
        // with bad enough timing, this might be possible
        // assert!(round <= self.round + 1, "Cannot receive share from a round bigger than 1 from the current one");
        if round < self.round {
            warn!("Client {:} sent shares for old round: {}, I'm at round {}", client_name, round, self.round);
            return;
        }
        if round > self.round {
            let entries = self.round_to_shares.entry(round).or_default();
            entries.push((client_name, batch.iter().cloned().collect()));
            warn!("Got share from the next round {}, enqueueing shares {}", round, self.round);
            return;
        }
        match &mut self.state {
            Phase::Reconstructing { .. } => error!("Got client share while in reconstruct phase"),
            Phase::ClientSharing { last_share_at, shares, 
                                   contact_graph, .. } => {
                assert_eq!(shares.len(), batch.len() , "client batch size mismatch with expected channels");
                let batch = batch.into_iter().map(|s| s.to_share()).collect::<Vec<_>>();
                assert!(shares.iter().map(|chan| chan.len()).sum::<usize>() <= self.config.num_channels * self.config.num_clients, "Got too many shares, impossible");

                info!("Got shares from client {} for round {}: {:?}", client_name, round, batch);

                for (chan, share) in (0..).zip(batch.into_iter()) {
                    assert_eq!(share.x, ((self.id + 1) as u64), "Got wrong share, bug within client");
                    shares[chan].push((share, client_name.clone()));
                }

                *last_share_at = Instant::now();
                contact_graph[self.id].insert(client_name);
            }
        }
    }

    pub fn handle_got_share_notification(&mut self, node_id: Id, client_name: &str, round: usize) {
        if round != self.round {
            return
        }
        if let Phase::ClientSharing {contact_graph, .. } = &mut self.state {
            contact_graph[node_id].insert(client_name.to_owned());
        } else {
            error!("Got share notification too late");
        }
    }

    pub fn handle_client_live_notification(&mut self, client_name: ClientId, round: usize) {
        if self.round != round {
            return;
        }
        if let Phase::ClientSharing { live_clients, .. } = &mut self.state {
            debug!("{} is live", client_name);
            live_clients.insert(client_name);
            if live_clients.len() == self.config.num_clients {
                info!("Beginning re-construct for round {} since all client sent shares to everyone", self.round);
                self.begin_reconstructing();
            }
        } else {
            error!("Got client live notification at wrong phase");
        }
    }

    pub fn on_receive_reconstruct_share(&mut self, batch: &[ShareBytes], _from_node: Id, round: usize) {
        if self.round != round {
            return;
        }
        assert_eq!(batch.len(), self.config.num_channels, "batch size mismatch on receive_reconstruct_share");
        // first, if we're not in re-construct stage yet, begin reconstruct
        if let Phase::ClientSharing { .. } = &self.state {
            debug!("Got reconstruct share while still on ClientSharing phase");
            self.begin_reconstructing();
        }

        if let Phase::Reconstructing { shares, last_share_at } = &mut self.state {
            // add all shares for every channel. 
            for channel_num in 0 .. self.config.num_channels {
                let chan_new_share = batch[channel_num].to_share();
                shares[channel_num].push(chan_new_share);
            }
            *last_share_at = Instant::now();

            // try decoding all channels once we passed the threshold
            if shares[0].len() >= self.config.threshold {
                let mut results = Vec::new();
                for (channel_num, channel) in (0..).zip(shares) {
                    let result = match decode_secret::<V>(reconstruct_secret(channel, self.config.threshold as u64)) {
                        Ok(None) => { 
                            Err(ReconstructError::NoValue)
                        },
                        Ok(Some(decoded)) => {
                            Ok(decoded)
                        }
                        Err(e) => { 
                            error!("Error while decoding channel {}: {:?}", channel_num, e);
                            Err(ReconstructError::Collision)
                        }
                    };
                    results.push(result);
                }
                Self::submit_message(self.client.clone(), self.id, AnonymityMessage::ReconstructResult { results, round: self.round });
            }
        }
    }

    /// Sends a message in a different task
    #[instrument(skip(client))]
    pub fn submit_message(mut client: Client<CT, AnonymityMessage<V>>, my_id: Id, msg: AnonymityMessage<V>) {
        // This must be done in another task, otherwise we will get a deadlock
        // (see explanation in `state_machine` trait)
        tokio::task::spawn_local(async move {
            let res = client.submit_value(msg).await;
            match res {
                Ok(_) => trace!("Sent message successfully"),
                Err(e) => error!("There was an error while submitting message: {:?}", e)
            }

        }.instrument(info_span!("submit_message", id=?my_id)));
    }


    #[instrument]
    pub fn begin_reconstructing(&mut self) {
        let shares = match &self.state {
            Phase::ClientSharing { live_clients, shares, .. } => {
                mix_shares_from(self.id, &live_clients, shares)
            },
            _ => { panic!("Begin reconstructing twice in a row") }
        };

        let channels = vec![Vec::new(); self.config.num_channels];
        self.state = Phase::Reconstructing { shares: channels, last_share_at: Instant::now() };
        debug!("Beginning re-construct phase, sending my shares {:?} to all other nodes", shares);

        if shares.is_none() {
            error!("I am missing shares from some clients, skipping reconstruct round {}", self.round);
            return;
        }
        let shares = shares.unwrap();
        let id = self.id;
        
        Self::submit_message(self.client.clone(), self.id, AnonymityMessage::ServerReconstruct {
            channel_shares: shares,
            node_id: id,
            round: self.round
        });
    }

    pub fn begin_client_sharing(&mut self, round: usize) {
        match self.state {
            Phase::ClientSharing { .. } => {
                error!("Begin client sharing while already sharing");
            }
            Phase::Reconstructing { .. } => {
                let num_nodes = self.config.num_nodes;
                self.state = Phase::ClientSharing { 
                    last_share_at: Instant::now(), 
                    live_clients: HashSet::new(), 
                    contact_graph: create_contact_graph(num_nodes),
                    shares: vec![Vec::new(); self.config.num_channels] 
                };
                self.round = round;
            }
        }
    }

    #[instrument]
    pub fn handle_reconstruct_result(&mut self, results: &[Result<V, ReconstructError>], round: usize) {
        assert!(round <= self.round, "got reconstruct message from the future");
        if round < self.round {
            return
        }
        for (chan, result) in (0..).zip(results) {
            match result {
                Ok(val) => {
                    info!(round=?self.round, "Node committed value {:?} via channel {} into index {}", val, chan, self.committed_messages.len());
                    self.committed_messages.push(val.clone());
                },
                Err(ReconstructError::Collision) => {
                    error!("Node detected collision at channel {}", chan);
                },
                Err(ReconstructError::NoValue) => {}
            }
        }
        self.begin_client_sharing(self.round + 1);
        
        self.new_round_sender.send(NewRound {
            round: self.round,
            last_reconstruct_results: Some(results.iter().cloned().collect())
        }).unwrap_or_else(|_e| {
            error!("No one to notify of new round");
            0
        });

        if let Some(entries) = self.round_to_shares.remove(&self.round) {
            for (client, batch) in entries {
                debug!("Handling delayed share from client {}", client);
                self.handle_client_share(client, &batch, self.round);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewRound<V> {
    pub round: usize,
    pub last_reconstruct_results: Option<Vec<Result<V, ReconstructError>>>
}

impl <V: Value> NewRound<V> {
    fn new(round: usize) -> Self {
        NewRound {
            round,
            last_reconstruct_results: None
        }
    }
}

#[async_trait(?Send)]
impl <V: Value + Hash, T: Transport<AnonymityMessage<V>>, C: ClientTransport<AnonymityMessage<V>>> StateMachine<AnonymityMessage<V>, T> for AnonymousLogSM<V, C> {
    async fn apply(&mut self, entry: &AnonymityMessage<V>) -> () {
        match entry {
            AnonymityMessage::ClientShare { channel_shares, client_name, round} => { self.handle_client_share(client_name.to_owned(), channel_shares.as_slice(), *round) },
            AnonymityMessage::ClientNotifyLive { client_name, round} => { self.handle_client_live_notification(client_name.to_owned(), *round) }
            AnonymityMessage::ServerReconstruct { channel_shares, node_id, round} => {  self.on_receive_reconstruct_share(channel_shares.as_slice(), *node_id, *round) }
            AnonymityMessage::ServerNotifyGotShare { node_id, client_name, round } => { self.handle_got_share_notification(*node_id, client_name, *round) }
            AnonymityMessage::ReconstructResult { results, round } => { self.handle_reconstruct_result(&results, *round) }
        }
    }

    type HookEvent = tokio::time::Instant;

    type HookStream = Interval;

    type PublishedEvent = NewRound<V>;

    fn create_hook_stream(&mut self) -> Self::HookStream {
        tokio::time::interval(self.config.phase_length)
    }

    fn handle_hook_event(&mut self, _: Self::HookEvent) {
        let now = Instant::now();
        match &self.state {
            Phase::ClientSharing { last_share_at, .. } => {
                if (now - *last_share_at) > self.config.phase_length {
                    debug!("ClientSharing timeout");
                    self.begin_reconstructing();
                }
            }
            Phase::Reconstructing { last_share_at, .. } => {
                if (now - *last_share_at) > self.config.phase_length {
                    debug!("Reconstructing time out");
                    self.begin_client_sharing(self.round + 1);
                }
            }
        };
    }

    fn get_event_stream(&mut self) -> broadcast::Sender<Self::PublishedEvent> {
        self.new_round_sender.clone()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}