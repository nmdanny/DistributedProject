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
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Interval, Duration};
use std::collections::HashMap;
use chrono::Local;

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
    Collision, NoValue, Timeout
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


/// Used to send messages to other nodes (via Raft)
struct SMSender<V: Value, CT: ClientTransport<AnonymityMessage<V>>> {
    task_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::UnboundedSender<(AnonymityMessage<V>, oneshot::Sender<()>)>,
    phantom: std::marker::PhantomData<CT>,
    node_id: usize
}

impl <V: Value, CT: ClientTransport<AnonymityMessage<V>>> SMSender<V, CT> {
    pub fn new(node_id: usize, mut client: Client<CT, AnonymityMessage<V>>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<(AnonymityMessage<V>, oneshot::Sender<()>)>();
        let task_handle = tokio::task::spawn_local(async move {
            while let Some((msg, resolver)) = rx.recv().await {
                let res = client.submit_value(msg.clone()).await;
                if let Err(e) = res {
                    panic!("Couldn't commit message to other nodes: {:?}", e);
                } 
                if let Ok(res) = res {
                    trace!("Result of sending {:?} is {:?}", msg, res);
                }
                resolver.send(()).unwrap_or_else(|_e| {
                    error!("Couldn't send SM Send response, initiator dumped his receiver?");
                });
                
            }
        }.instrument(info_span!("SMSender", node.id=?node_id)));
        Self {
            task_handle, sender: tx, phantom: Default::default(), node_id
        }
    }

    
    pub fn submit(&mut self, message: AnonymityMessage<V>) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.sender.send((message, tx)).unwrap();
        rx
    }
}

const NEW_ROUND_CHAN_SIZE: usize = 1024;

use tokio::fs::{File};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

struct Metrics {
    share_tx: mpsc::UnboundedSender<(usize, usize, Share)>,
    decode_tx: mpsc::UnboundedSender<(usize, usize, bool)>,
    join_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>
}

impl Metrics {
    fn new(node_id: usize, config: Rc<Config>) -> Self {
        let (share_tx, mut share_rx) = mpsc::unbounded_channel::<(usize, usize, Share)>();
        let (decode_tx, mut decode_rx) = mpsc::unbounded_channel::<(usize, usize, bool)>();


        let join_handle = tokio::task::spawn_local(async move {
            let folder = PathBuf::from(file!()).parent().unwrap().parent().unwrap().parent().unwrap().join("out");
            let share_file = folder.join(format!("{}-share.csv", node_id));

            let time = Local::now();
            let time_st = time.format("%H-%M-%S");

            let decode_file = folder.join(format!("{}-decode-{}.csv", node_id, time_st));
            let mut share_file = File::create(share_file).await?;
            let mut decode_file = File::create(decode_file).await?;

            share_file.write_all("id,round,chan,share_x,share_px\n".as_bytes()).await?;
            decode_file.write_all("num_nodes,num_clients,num_channels,id,round,chan,malformed\n".as_bytes()).await?;

            loop {
                tokio::select! {
                    Some(tup) = share_rx.recv() => {
                        let (round, chan, share) = tup;
                        let s = format!("{},{},{},{},\"{:?}\"\n", node_id, round, chan, share.x, share.p_x.as_bytes());
                        share_file.write_all(s.as_bytes()).await?;
                    },
                    Some(tup) = decode_rx.recv() => {
                        let (round, chan, malformed) = tup;
                        let Config  { num_nodes, num_clients, num_channels, .. } = *config;
                        let s = format!("{},{},{},{},{},{},{}\n",  num_nodes, num_clients, num_channels, node_id, round, chan, malformed);
                        decode_file.write_all(s.as_bytes()).await?;
                    },
                    else => { return Ok(()); }
                }
            }

        });

        Self {
            share_tx, decode_tx, join_handle
        }
    }
    fn report_share(&mut self, round: usize, chan: usize, share: Share) {
        self.share_tx.send((round, chan, share)).unwrap();
    }

    fn report_decode_result(&mut self, round: usize, chan: usize, malformed: bool) {
        self.decode_tx.send((round, chan, malformed)).unwrap();
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AnonymousLogSM<V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> {

    pub committed_messages: Vec<V>,

    #[derivative(Debug="ignore")]
    client: SMSender<V, CT>,

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
    pub round_to_shares: HashMap<usize, Vec<(String, Vec<ShareBytes>)>>,

    /// Likewise for liveness notifications from the future
    #[derivative(Debug="ignore")]
    pub round_to_live: HashMap<usize, HashSet<ClientId>>,

    // Used to await on messages sent to other nodes
    #[derivative(Debug="ignore")]
    pub messages_being_sent: Vec<oneshot::Receiver<()>>,

    #[derivative(Debug="ignore")]
    metrics: Metrics

}


impl <V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> AnonymousLogSM<V, CT> {
    pub fn new(config: Rc<Config>, id: usize, client_transport: CT) -> AnonymousLogSM<V, CT> {
        assert!(id < config.num_nodes, "Invalid node ID");
        let client = SMSender::new(id, Client::new(format!("SM-{}-cl", id), client_transport, config.num_nodes));
        let state = Phase::ClientSharing {
            last_share_at: Instant::now(),
            live_clients: HashSet::new(),
            contact_graph: create_contact_graph(config.num_nodes),
            shares: vec![Vec::new(); config.num_channels]
        };
        let (nr_tx, _nr_rx) = broadcast::channel(NEW_ROUND_CHAN_SIZE);
        AnonymousLogSM {
            committed_messages: Vec::new(), client, id, config: config.clone(), state, round: 0, 
            new_round_sender: nr_tx,
            round_to_shares: HashMap::new(),
            round_to_live: HashMap::new(),
            messages_being_sent: Vec::new(),
            metrics: Metrics::new(id, config)
        }
        
    }

    #[instrument(skip(batch))]
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
            debug!("Got share from the next round {} while current round is {}, enqueuing", round, self.round);
            return;
        }
        match &mut self.state {
            Phase::Reconstructing { .. } => trace!("Got client share too late (while in reconstruct phase)"),
            Phase::ClientSharing { last_share_at, shares, 
                                   contact_graph, .. } => {
                assert_eq!(shares.len(), batch.len() , "client batch size mismatch with expected channels");
                let batch = batch.into_iter().map(|s| s.to_share()).collect::<Vec<_>>();
                assert!(shares.iter().map(|chan| chan.len()).sum::<usize>() <= self.config.num_channels * self.config.num_clients, "Got too many shares, impossible");

                debug!("Got shares from client {} for round {}: {:?}", client_name, round, batch);

                for (chan, share) in (0..).zip(batch.into_iter()) {
                    assert_eq!(share.x, ((self.id + 1) as u64), "Got wrong share, bug within client");
                    self.metrics.report_share(round, chan, share.clone());
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
        if round < self.round {
            return;
        }
        if round > self.round {
            let entries = self.round_to_live.entry(round).or_default();
            warn!("Client {:} sent liveness notification for old round: {}, I'm at round {}", client_name, round, self.round);
            entries.insert(client_name.clone());
            return;
        }
        if let Phase::ClientSharing { live_clients, .. } = &mut self.state {
            debug!("{} is live", client_name);
            live_clients.insert(client_name);
            if live_clients.len() == self.config.num_clients {
                // TODO:
                debug!("Beginning re-construct for round {} since all client sent shares to everyone", self.round);
                self.begin_reconstructing();
            }
        } else {
            trace!("Got client live notification from {} of round {} at wrong phase", client_name, round);
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
                            trace!("Error while decoding channel {}: {:?}", channel_num, e);
                            Err(ReconstructError::Collision)
                        }
                    };
                    results.push(result);
                }
                self.submit_message(AnonymityMessage::ReconstructResult { results, round: self.round });
            }
        }
    }

    /// Submits a message without waiting for it
    #[instrument(skip(self))]
    pub fn submit_message(&mut self, msg: AnonymityMessage<V>) {
        let rx = self.client.submit(msg);
        self.messages_being_sent.push(rx);
    }

    pub async fn wait_for_messages_to_be_sent(&mut self) {
        let _res = futures::future::join_all(self.messages_being_sent.drain(..)).await;
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
        
        self.submit_message(AnonymityMessage::ServerReconstruct {
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

    #[instrument(skip(self))]
    pub fn handle_reconstruct_result(&mut self, results: &[Result<V, ReconstructError>], round: usize) {
        // TODO how to handle this? how is this even possible
        assert!(round <= self.round, "got reconstruct message from the future");
        if round < self.round {
            return
        }
        for (chan, result) in (0..).zip(results) {
            match result {
                Ok(val) => {
                    trace!(round=?self.round, "Node committed value {:?} via channel {} into index {}", val, chan, self.committed_messages.len());
                    self.committed_messages.push(val.clone());
                    self.metrics.report_decode_result(self.round, chan, false);
                },
                Err(ReconstructError::Collision) => {
                    trace!("Node detected collision at channel {}", chan);
                    self.metrics.report_decode_result(self.round, chan, true);
                },
                Err(ReconstructError::NoValue) => {}
                Err(ReconstructError::Timeout) => {
                    trace!("Saw that reconstruct for round {} has timed out, not enough valid servers", round);
                    break;
                }
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
        if let Some(entries) = self.round_to_live.remove(&self.round) {
            for client in entries {
                debug!("Handling delayed liveness note from client {}", client);
                self.handle_client_live_notification(client, self.round);

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
                    trace!("ClientSharing timeout");
                    self.begin_reconstructing();
                }
            }
            Phase::Reconstructing { last_share_at, .. } => {
                if (now - *last_share_at) > self.config.phase_length {
                    trace!("Reconstructing time out");

                    let results = vec![Err(ReconstructError::Timeout); self.config.num_channels];
                    let msg = AnonymityMessage::ReconstructResult {
                        round: self.round,
                        results
                    };
                    self.submit_message(msg);
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
    use rand::distributions::{Distribution, Uniform};
    use tokio::task;

    const MAX_ITERS: usize = 50000;

    const THRESHOLD: usize = 3;
    const NUM_NODES: usize = 5;
    const NUM_CHANNELS: usize = 1;

    #[tokio::test]
    pub async fn test_simulation() {
        crate::consensus::logging::setup_logging().unwrap();

        let config = Rc::new(Config {
            threshold: THRESHOLD,
            num_channels: NUM_CHANNELS,
            num_nodes: NUM_NODES,
            num_clients: 1, phase_length: Duration::from_millis(1)
        });

        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {

            let mut metricses = (0 .. config.num_nodes).into_iter().map(|node_id| Metrics::new(node_id, config.clone())).collect::<Vec<_>>();
            let mut rand_metrics = Metrics::new(1337, config.clone());

            for round in 0 .. MAX_ITERS {
                let val_channel = Uniform::new(0, config.num_channels).sample(&mut rand::thread_rng());
                let secret_val = encode_secret(0u64).unwrap();

                // create 'd' collections of shares, one for each server
                for chan in 0.. config.num_channels {
                    let secret = if chan == val_channel { secret_val } else { encode_zero_secret() };
                    let threshold = config.threshold as u64;
                    let num_nodes = config.num_nodes as u64;
                    let shares = create_share(secret, threshold, num_nodes);
                    for (node_id, share) in (0..).zip(shares) {
                        metricses[node_id].report_share(round, chan, share);
                    }
                }


                let p_x = FP::random(&mut rand::thread_rng());
                let share = Share {
                    x: 1338,
                    p_x
                };
                rand_metrics.report_share(round, 0, share);

                if round % (MAX_ITERS / 10).max(1) == 0 {
                    info!("Progress {:.1}%", 100.0 * (round as f64/MAX_ITERS as f64));
                    tokio::task::yield_now().await;
                }

            }
        }).await;
        ls.await;
    }
}