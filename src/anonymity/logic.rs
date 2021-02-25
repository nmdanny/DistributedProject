use crate::{consensus::{node, types::*}, crypto::PKISettings};
use crate::consensus::state_machine::StateMachine;
use crate::consensus::transport::Transport;
use crate::consensus::client::{Client, ClientTransport};
use crate::anonymity::secret_sharing::*;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use time::Instant;
use std::{boxed::Box, collections::BTreeMap, convert::TryInto};
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
use clap::Clap;

fn parse_phase_length(src: &str) -> Result<std::time::Duration, anyhow::Error> {
    let millis = src.parse::<u64>()?;
    Ok(std::time::Duration::from_millis(millis))
}


#[derive(Debug, Clone, Clap)]
/// Configuration used for anonymous message sharing
pub struct Config {
    #[clap(short = 's', long = "num_servers", about = "Number of servers", default_value = "5")]
    pub num_nodes: usize,

    #[clap(short = 'c', long = "num_clients", about = "Number of clients", default_value = "100")]
    pub num_clients: usize,

    #[clap(short = 't', long = "threshold", about = "Minimal number of shares needed to recover a secret", default_value = "51")]
    pub threshold: usize,

    #[clap(short = 'h', long = "num_channels", about = "Number of channels. Should be a multiple of the number of clients ", default_value = "300")]
    pub num_channels: usize,

    #[clap(short='l', long = "phase_length", about = "Length of a phase(share/recover) in miliseconds", 
           parse(try_from_str = parse_phase_length), default_value = "1000")]
    pub phase_length: std::time::Duration
}


/// Given a list of clients who are 'live', that is, didn't crash before sending all their shares,
/// and all of the node's shares, sums shares from those channels

// Mixes shares of several clients for each channel, returning a share per channel,
// or None if no shares were seen
fn mix_shares(my_id: Id, shares_view: &Vec<Vec<(Share, Id)>>) -> Option<Vec<Share>> {
    let clients = shares_view[0].iter().map(|s| &s.1).collect::<Vec<_>>();
    info!("node {} is trying to mix his shares from clients {:?}", my_id, clients);

    if clients.len() == 0 {
        return None;
    }

    Some(shares_view.iter().map(|chan| {
        chan.iter()
            .inspect(|(share, _)| assert_eq!(share.x, my_id as u64 + 1))
            .fold(Share::new(my_id as u64 + 1), |acc, (share, _from_client)| {
                add_shares(&acc, share)
            })
    }).collect())
}


#[derive(Derivative, Clone, PartialEq, Eq)]
#[derivative(Debug)]
pub enum Phase {
    ClientSharing {
        last_share_at: time::Instant,

        // Contains 'd' channels, each channel containing shares from many clients
        #[derivative(Debug="ignore")]
        shares: Vec<Vec<(Share, Id)>>
    },
    Reconstructing {
        #[derivative(Debug="ignore")]
        shares: Vec<Vec<Share>>,

        last_share_at: time::Instant
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconstructError {
    Collision
}

/// Results of reconstruct phase
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconstructionResults<V> {
    TimedOut,
    Some {
        chan_to_val: BTreeMap<usize, Result<V, ReconstructError>>
    }
}

#[derive(Derivative, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derivative(Debug)]
pub enum AnonymityMessage<V: Value> {
    /// Note that the following message is not synchronized
    ClientShare { 
        /// A vector of `num_channels` vectors of `num_nodes` shares
        #[derivative(Debug="ignore")]
        shares: Vec<Vec<EncryptedShare>>,
        client_id: Id,
        round: usize
    },
    ServerBeginReconstruct {
        initiator: Id,
        round: usize
    },
    ServerReconstructShare { 
        #[derivative(Debug="ignore")]
        channel_shares: Vec<Share>, 
        node_id: Id,
        round: usize
    },
    ReconstructResult {
        #[serde(bound = "V: Value")]
        #[derivative(Debug="ignore")]
        results: ReconstructionResults<V>,
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
use std::sync::Arc;

struct Metrics {
    share_tx: mpsc::UnboundedSender<(usize, usize, Share)>,
    decode_tx: mpsc::UnboundedSender<(usize, usize, bool)>,
    join_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>
}

impl Metrics {
    fn new(node_id: usize, config: Arc<Config>) -> Self {
        let (share_tx, mut share_rx) = mpsc::unbounded_channel::<(usize, usize, Share)>();
        let (decode_tx, mut decode_rx) = mpsc::unbounded_channel::<(usize, usize, bool)>();


        let join_handle = tokio::task::spawn_local(async move {
            let folder = PathBuf::from(file!()).parent().unwrap().parent().unwrap().parent().unwrap().join("out");
            std::fs::create_dir_all(&folder).unwrap();
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
                        let bytes = share.p_x.clone();
                        let s = format!("{},{},{},{},\"{:?}\"\n", node_id, round, chan, share.x, bytes);
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
    pub config: Arc<Config>,

    #[derivative(Debug="ignore")]
    pub pki: Arc<PKISettings>,

    pub state: Phase,

    pub round: usize,

    #[derivative(Debug="ignore")]
    pub new_round_sender: broadcast::Sender<NewRound<V>>,


    /// Maps rounds from the future to a list of clients along with their shares
    /// This is used to handle share requests from future rounds
    #[derivative(Debug="ignore")]
    pub round_to_shares: HashMap<usize, Vec<(Id, Vec<Vec<EncryptedShare>>)>>,

    /// Likewise for liveness notifications from the future
    #[derivative(Debug="ignore")]
    pub round_to_live: HashMap<usize, HashSet<Id>>,

    // Used to await on messages sent to other nodes
    #[derivative(Debug="ignore")]
    pub messages_being_sent: Vec<oneshot::Receiver<()>>,

    #[derivative(Debug="ignore")]
    metrics: Metrics

}


impl <V: Value + Hash, CT: ClientTransport<AnonymityMessage<V>>> AnonymousLogSM<V, CT> {
    pub fn new(config: Arc<Config>, pki: Arc<PKISettings>, id: usize, client_transport: CT) -> AnonymousLogSM<V, CT> {
        assert!(id < config.num_nodes, "Invalid node ID");
        let client = SMSender::new(id, Client::new(format!("SM-{}-cl", id), client_transport, config.num_nodes));
        let state = Phase::ClientSharing {
            last_share_at: Instant::now(),
            shares: vec![Vec::new(); config.num_channels]
        };
        let (nr_tx, _nr_rx) = broadcast::channel(NEW_ROUND_CHAN_SIZE);
        AnonymousLogSM {
            committed_messages: Vec::new(), client, id, config: config.clone(), pki, state, round: 0, 
            new_round_sender: nr_tx,
            round_to_shares: HashMap::new(),
            round_to_live: HashMap::new(),
            messages_being_sent: Vec::new(),
            metrics: Metrics::new(id, config)
        }
        
    }

    #[instrument(skip(encrypted_shares))]
    pub fn handle_client_share(&mut self, client_id: Id, encrypted_shares: &Vec<Vec<EncryptedShare>>, round: usize) {
        // with bad enough timing, this might be possible
        // assert!(round <= self.round + 1, "Cannot receive share from a round bigger than 1 from the current one");
        if round < self.round {
            warn!("Client {:} sent shares for old round: {}, I'm at round {}", client_id, round, self.round);
            return;
        }
        if round > self.round {
            let entries = self.round_to_shares.entry(round).or_default();
            entries.push((client_id, encrypted_shares.clone()));
            debug!("Got share from the next round {} while current round is {}, enqueuing", round, self.round);
            return;
        }
        match &mut self.state {
            Phase::Reconstructing { .. } => trace!("Got client share too late (while in reconstruct phase)"),
            Phase::ClientSharing { last_share_at, shares, .. } => {

                let key = &self.pki.clone().clients_keys[client_id].1;
                let id = self.id;
                let batch = encrypted_shares.par_iter()
                    .map(|chan| chan[id].decrypt(key).expect("Should be able to decrypt shares meant for us"))
                    .collect::<Vec<_>>();

                assert_eq!(shares.len(), batch.len() , "client batch size mismatch with expected channels");
                assert!(shares.iter().map(|chan| chan.len()).sum::<usize>() <= self.config.num_channels * self.config.num_clients, "Got too many shares, impossible");

                debug!("Got shares from client {} for round {}: {:?}", client_id, round, batch);

                for (chan, share) in (0..).zip(batch.into_iter()) {
                    assert_eq!(share.x, ((self.id + 1) as u64), "Got wrong share, bug within client");
                    self.metrics.report_share(round, chan, share.clone());
                    shares[chan].push((share, client_id));
                }

                *last_share_at = Instant::now();
            }
        }
    }

    #[instrument(skip(batch))]
    pub fn on_receive_reconstruct_share(&mut self, batch: &[Share], _from_node: Id, round: usize) {
        if self.round != round {
            return;
        }
        assert_eq!(batch.len(), self.config.num_channels, "batch size mismatch on receive_reconstruct_share");
        if let Phase::ClientSharing { .. } = &self.state {
            // shares must be synchronized(arrive after a BeginReconstruct message),
            panic!("Got reconstruct share for round {} from node {} while still on ClientSharing phase", round, _from_node);
        }

        if let Phase::Reconstructing { shares, last_share_at } = &mut self.state {

            info!("Node {} at round {} got a batch batch from node {}", self.id, round, _from_node);
            debug!("Batch: {:?}", batch);

            // add all shares for every channel. 
            for channel_num in 0 .. self.config.num_channels {
                let chan_new_share = batch[channel_num].clone();
                shares[channel_num].push(chan_new_share);
            }
            *last_share_at = Instant::now();

            // try decoding all channels once we passed the threshold
            if shares[0].len() >= self.config.threshold {
                let mut results = BTreeMap::new();
                for (channel_num, channel) in (0..).zip(shares) {
                    match decode_secret::<V>(reconstruct_secret(&channel[ .. self.config.threshold],
                                                                             self.config.threshold as u64)) {
                        Ok(None) => {},
                        Ok(Some(decoded)) => {
                            results.insert(channel_num, Ok(decoded));
                        },
                        Err(e) => { 
                            error!("Error while decoding channel {}: {:?}", channel_num, e);
                            results.insert(channel_num, Err(ReconstructError::Collision));
                        }
                    };
                }
                self.submit_message(AnonymityMessage::ReconstructResult { 
                    results: ReconstructionResults::Some { chan_to_val: results },
                    round: self.round 
                });
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
    pub fn begin_reconstructing(&mut self, initiator: Id, round: usize) {

        assert!(round <= self.round, "Cannot receive BeginReconstruct message from the future, my id: {}, initiator: {}", self.id, initiator);
        if round < self.round {
            warn!("Got old BeginReconstruct message from initiator {}, his round: {}, my round: {}", initiator, round, self.round);
            return;
        }
        if let Phase::Reconstructing { .. } = &self.state {
            // duplicate begin reconstruct messages are likely and OK
            return;
        }

        let shares = match &self.state {
            Phase::ClientSharing { shares, .. } => {
                mix_shares(self.id, shares)
            },
            _ => { panic!("Begin reconstructing twice in a row") }
        };

        let channels = vec![Vec::new(); self.config.num_channels];
        self.state = Phase::Reconstructing { shares: channels, last_share_at: Instant::now() };
        info!("Beginning re-construct phase for round {}, initiated by {}, sending my shares to all other nodes", self.round, initiator);
        debug!("My shares being sent: {:?}", shares);

        if shares.is_none() {
            warn!("I am missing shares from some clients, skipping reconstruct round {}", self.round);
            return;
        }
        let shares = shares.unwrap();
        let id = self.id;
        
        self.submit_message(AnonymityMessage::ServerReconstructShare {
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
                self.state = Phase::ClientSharing { 
                    last_share_at: Instant::now(), 
                    shares: vec![Vec::new(); self.config.num_channels] 
                };
                self.round = round;
            }
        }
    }

    #[instrument(skip(self))]
    pub fn handle_reconstruct_result(&mut self, results: &ReconstructionResults<V>, round: usize) {
        // TODO how to handle this? how is this even possible
        assert!(round <= self.round, "got reconstruct message from the future");
        if round < self.round {
            return
        }
        match results {
            ReconstructionResults::TimedOut => {
                trace!("Saw that reconstruct for round {} has timed out, not enough valid servers", round);
            }
            ReconstructionResults::Some { chan_to_val } => {
                for (&chan, res) in chan_to_val {
                    match res {
                        Ok(val) => {
                            trace!(round=?self.round, "Node committed value {:?} via channel {} into index {}", val, chan, self.committed_messages.len());
                            self.committed_messages.push(val.clone());
                            self.metrics.report_decode_result(self.round, chan, false);
                        }
                        Err(_) => {
                            trace!("Node detected collision at channel {}", chan);
                            self.metrics.report_decode_result(self.round, chan, true);
                        }
                    }
                }
            }
        }
        let new_round = self.round + 1;
        self.begin_client_sharing(self.round + 1);
        
        self.new_round_sender.send(NewRound {
            round: new_round,
            last_reconstruct_results: results.clone()
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

/// Pushed to clients every round, indicating a new round as well
/// as containing the results of the previous round(unless this is
/// the first round)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewRound<V> {
    pub round: usize,
    pub last_reconstruct_results: ReconstructionResults<V>
}

#[async_trait]
impl <V: Value + Hash, T: Transport<AnonymityMessage<V>>, C: ClientTransport<AnonymityMessage<V>>> StateMachine<AnonymityMessage<V>, T> for AnonymousLogSM<V, C> {
    fn apply(&mut self, entry: &AnonymityMessage<V>) -> () {
        match entry {
            AnonymityMessage::ClientShare { shares, client_id, round} => { self.handle_client_share(client_id.to_owned(), shares, *round) },
            AnonymityMessage::ServerReconstructShare { channel_shares, node_id, round} => {  self.on_receive_reconstruct_share(channel_shares.as_slice(), *node_id, *round) }
            AnonymityMessage::ReconstructResult { results, round } => { self.handle_reconstruct_result(&results, *round) }
            AnonymityMessage::ServerBeginReconstruct { initiator, round } => { self.begin_reconstructing(*initiator, *round) }
        }
    }

    type HookEvent = tokio::time::Instant;

    type HookStream = tokio_stream::wrappers::IntervalStream;

    type PublishedEvent = NewRound<V>;

    fn create_hook_stream(&mut self) -> Self::HookStream {
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(self.config.phase_length))
    }

    fn handle_hook_event(&mut self, _: Self::HookEvent) {
        let now = Instant::now();
        match &self.state {
            Phase::ClientSharing { last_share_at, .. } => {
                if (now - *last_share_at) > self.config.phase_length {
                    info!("ClientSharing timeout at round {}, beginning reconstruct", self.round);
                    let msg = AnonymityMessage::ServerBeginReconstruct {
                        initiator: self.id,
                        round: self.round
                    };
                    self.submit_message(msg);
                }
            }
            Phase::Reconstructing { last_share_at, .. } => {
                if (now - *last_share_at) > self.config.phase_length {
                    trace!("Reconstructing time out at round {}", self.round);

                    let msg = AnonymityMessage::ReconstructResult {
                        round: self.round,
                        results: ReconstructionResults::TimedOut
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
    #[ignore]
    pub async fn test_simulation() {
        let _guard = crate::consensus::logging::setup_logging().unwrap();

        let config = Arc::new(Config {
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
                let zero_val = encode_zero_secret();

                // create 'd' collections of shares, one for each server
                for chan in 0.. config.num_channels {
                    let secret = if chan == val_channel { &secret_val } else { &zero_val };
                    let threshold = config.threshold as u64;
                    let num_nodes = config.num_nodes as u64;
                    let shares = create_share(secret.clone(), threshold, num_nodes);
                    for (node_id, share) in (0..).zip(shares) {
                        metricses[node_id].report_share(round, chan, share);
                    }
                }

                let p_x = (0 .. NUM_POLYS).map(|_| FP::random(&mut rand::thread_rng())).collect();
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