use crate::consensus::types::*;
use crate::consensus::transport::*;
use crate::consensus::log::*;
use std::collections::BTreeMap;
use tracing::instrument;
use tokio::task;
use std::rc::Rc;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::stream::StreamExt;
use serde;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing_futures::Instrument;
use crate::consensus::node_communicator::{NodeCommand, CommandHandler};
use async_trait::async_trait;
use futures::TryFutureExt;

const MIN_ELECTION_TIMEOUT_MS: u64 = 1500;
const MAX_ELECTION_TIMEOUT_MS: u64 = 3000;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);

pub fn generate_election_timeout() -> Duration {
    use rand::distributions::{Distribution, Uniform};
    let between = Uniform::from(MIN_ELECTION_TIMEOUT_MS .. MAX_ELECTION_TIMEOUT_MS);
    let mut rng = rand::thread_rng();
    Duration::from_millis(between.sample(&mut rng))
}

/// State used by a follower
#[derive(Debug)]
pub struct FollowerState<'a, V: Value, T: Transport<V>> {
    pub node: &'a mut Node<V, T>,

    /// The last time since we granted a vote or got a heartbeat from the leader
    /// Once this elapses, convert to a candidate. (§5.2)
    pub time_since_last_heartbeat_or_grant_vote: Instant
}

impl <'a, V: Value, T: Transport<V>> FollowerState<'a, V, T> {
    /// Creates state used for a node who has just become a follower
    pub fn new(node: &'a mut Node<V, T>) -> Self {
        FollowerState {
            node,
            time_since_last_heartbeat_or_grant_vote: Instant::now()
        }
    }

    /// Performs follower specific changes when receiving an AE request
    /// (notably, updates the follower state)
    pub fn on_receive_append_entries(&mut self, req: &AppendEntries<V>) {
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        // leader ID should've been set(at the beginning, leader is 0)
        // otherwise, we only transition to follower after receiving a matching AE)
        assert!(self.node.leader_id.is_some());
        self.node.voted_for = None;
        loop {
            // One of the later operations might have changed our state
            if self.node.state != ServerState::Follower {
                return Ok(())
            }

            // create a single timer for a heartbeat
            let mut heartbeat_timeout = tokio::time::delay_for(self.node.election_timeout);
            tokio::select! {
                _ = heartbeat_timeout => {
                        warn!("haven't received a heartbeat in too long");
                        self.node.change_state(ServerState::Candidate);
                },
                res = self.node.receiver.next() => {
                    // TODO can this channel close prematurely?
                    let cmd = res.unwrap();
                    self.handle_command(cmd).await;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl <'a, V: Value, T: Transport<V>> CommandHandler<V> for FollowerState<'a, V, T> {
    #[instrument]
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        if req.term < self.node.current_term {
            return Ok(AppendEntriesResponse::failed(self.node.current_term));
        }

        // if we had just lost an election
        if self.node.leader_id.is_none() {
            info!("Now I know the new leader: {}", req.leader_id);
            self.node.leader_id = Some(req.leader_id);
        }

        if self.node.leader_id != Some(req.leader_id) {
            assert!(req.term > self.node.current_term,
                "If there's a mismatch between leaders, the term must have changed(increased)");
        }
        self.node.leader_id = Some(req.leader_id);

        // update heartbeat to prevent switch to candidate
        self.time_since_last_heartbeat_or_grant_vote = Instant::now();

        // continue with the default handling of append entry
        return self.node.on_receive_append_entry(req);
    }

    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        // use default handling of request vote
        let res = self.node.on_receive_request_vote(&req);

        match res.as_ref() {
            Ok(res) if res.vote_granted => {
                // if we granted a vote, delay switch to candidate
                self.time_since_last_heartbeat_or_grant_vote = Instant::now();
            }
            _ => {}
        }
        return res;
    }
}
#[derive(Debug, Eq, PartialEq)]
pub enum ElectionResult {
    Lost,
    Won,
    Undecided
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ElectionState {
    /// NOTE: it is assumed that Raft RPC messages cannot be duplicated, in other words,
    /// since each node only votes once, we can not have more than one response for every nodee
    /// (We can assume reliability by our network protocol, e.g, unique message ID)
    pub votes: Vec<RequestVoteResponse>,

    pub yes: usize,
    pub no: usize,

    pub quorum_size: usize,
    pub term: usize,

    #[derivative(Debug="ignore")]
    pub vote_receiver: UnboundedReceiver<RequestVoteResponse>
}

impl ElectionState {
    pub fn new(quorum_size: usize, term: usize,
               vote_receiver: UnboundedReceiver<RequestVoteResponse>) -> Self {
        // we always vote for ourself
        let votes = vec![
            RequestVoteResponse {
                term, vote_granted: true
            }
        ];
        ElectionState {
            votes, yes: 1, no: 0, quorum_size, term, vote_receiver
        }
    }

    /// Counts the given vote(if it is for the current election)
    pub fn count_vote(&mut self, vote: RequestVoteResponse) {
        if vote.term != self.term {
            error!("Got vote for another term, my term: {}, vote term: {}", self.term, vote.term);
            return;
        }
        if vote.vote_granted {
            self.yes += 1;
        } else {
            self.no += 1;
        }
        self.votes.push(vote);
    }

    /// Tallies all current votes and returns the result
    pub fn tally(&self) -> ElectionResult {
        if self.yes >= self.quorum_size {
            return ElectionResult::Won;
        }
        if self.no >= self.quorum_size {
            return ElectionResult::Lost;
        }
        return ElectionResult::Undecided;
    }
}

/// State used by a candidate over one or more consecutive elections
#[derive(Debug)]
pub struct CandidateState<'a, V: Value, T: Transport<V>> {
    pub node: &'a mut Node<V, T>

}

impl <'a, V: Value, T: Transport<V>> CandidateState<'a, V, T> {
    /// Creates state for a candidate who has just started an election
    pub fn new(candidate: &'a mut Node<V, T>) -> Self {
        // a candidate always votes for itself
        CandidateState {
            node: candidate
        }
    }

    /// Starts a new election
    pub async fn start_election(&mut self) -> Result<ElectionState, RaftError>{
        self.node.leader_id = None;
        self.node.current_term += 1;
        self.node.voted_for = Some(self.node.id);
        warn!("Election started for term {}", self.node.current_term);

        /// for notifying the loop of received votes
        let (tx, rx) = mpsc::unbounded_channel();


        for node_id in self.node.all_other_nodes().collect::<Vec<_>>() {
            let req = RequestVote {
                term: self.node.current_term,
                candidate_id: self.node.id,
                last_log_index: self.node.storage.last_log_index(),
                last_log_term: self.node.storage.last_log_term()
            };
            let transport = self.node.transport.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                trace!("sending request to {}", node_id);
                if let Ok(res) = transport.send_request_vote(node_id, req).await {
                    debug!("response is {:?}", res);
                    tx.send(res.clone()).unwrap_or_else(|e| {
                        // TODO this isn't really an error, just the result of delays
                        error!("Received vote response {:?} too late (loop has dropped receiver, send error: {:?})", res, e);
                    });
                    return;
                }
                error!("request failed(no response)");
            }.instrument(info_span!("vote request", to=node_id)));
        }
        Ok(ElectionState::new(self.node.quorum_size(), self.node.current_term, rx))
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        // loop for multiple consecutive elections(1 or more)
        loop {

            // start an election, updating node state and sending vote requests
            let mut election_state = self.start_election().await?;
            let mut _lost = false;

            // loop for a single election
            loop {

                if self.node.state != ServerState::Candidate {
                    return Ok(());
                }

                // TODO should I generate a new timeout?
                let election_end = tokio::time::delay_for(self.node.election_timeout);

                tokio::select! {
                    _ = election_end => {
                        // election timed out, start another one
                        break;
                    },
                    Some(vote) = election_state.vote_receiver.next() => {
                        // If we get vote from a node at a later term, we'll convert
                        // to follower. (§5.1)
                        if self.node.try_update_term(vote.term, None) {
                            assert!(!vote.vote_granted);
                            return Ok(());
                        }
                        election_state.count_vote(vote);
                        match election_state.tally() {
                            ElectionResult::Lost => {
                                if (!_lost) {
                                    info!("lost election, results: {:?}", election_state);
                                }
                                _lost = true;
                                // we will not change the state yet, this will be done once we
                                // receive a new AppendEntries message(might be slightly wasteful
                                // as we might try another election, but this is a rare scenario
                                // as the election timeout is bigger by an order of magnitude than
                                // the broadcast time.)
                            }
                            ElectionResult::Won => {
                                assert!(!_lost, "Cannot win election after losing(sanity check)");
                                info!("won election, results: {:?}", election_state);
                                self.node.change_state(ServerState::Leader);
                                return Ok(())
                            }
                            ElectionResult::Undecided => {
                                assert!(!_lost, "Cannot become undecided after losing(sanity check)");
                            }
                        }
                    },
                    res = self.node.receiver.next() => {
                        // TODO can this channel close prematurely?
                        let cmd = res.unwrap();
                        self.handle_command(cmd).await;
                    }
                }
            }
        }
    }

}


#[async_trait]
impl <'a, V: Value, T: Transport<V>> CommandHandler<V> for CandidateState<'a, V, T> {
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        return self.node.on_receive_append_entry(req);
    }

    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let res = self.node.on_receive_request_vote(&req);
        if let Ok(res) = &res {
            assert!(!res.vote_granted, "A candidate will never grant votes to other candidates");
        }
        return res;
    }
}

#[derive(Debug)]
pub struct LeaderState<'a, V: Value, T: Transport<V>>{

    pub node: &'a mut Node<V, T>,

    /// For each node, index of the next log entry(in the leader's log) to send to him
    /// Usually next_index[peer] is match_index[peer] + 1, however, match_index[peer] is usually
    /// 0 when a leader is initiated(or might lag behind in other cases where `peer` was partitioned
    /// for a long while),
    pub next_index: BTreeMap<Id, usize>,

    /// For each node, index of the highest log entry known to be replicated on him(the node's
    /// commit index)
    ///
    /// Assuming 1 based indexing, we only need to send log[match_index[peer] + 1, ...] to each peer
    /// after gaining leadership
    pub match_index: BTreeMap<Id, usize>
}

impl<'a, V: Value, T: Transport<V>> LeaderState<'a, V, T> {
    /// Creates state for a node who just became a leader
    pub fn new(node: &'a mut Node<V, T>) -> Self {
        LeaderState {
            node,
            next_index: Default::default(),
            match_index: Default::default()
        }
    }

    /// Sends a heartbeat to all nodes. If it detects we are stale(encounters a higher term),
    /// sends the newer term via given sender.
    pub async fn send_heartbeat(&self, stale_notifier: mpsc::Sender<usize>) {
        let msg = AppendEntries {
            leader_id: self.node.id,
            term: self.node.current_term,
            entries: Vec::new(),
            leader_commit: self.node.commit_index,
            prev_log_index: self.node.storage.last_log_index(),
            prev_log_term: self.node.storage.last_log_term()
        };
        info!("sending heartbeat");
        let my_term = self.node.current_term;
        for node_id in self.node.all_other_nodes() {
            let transport = self.node.transport.clone();
            let msg = msg.clone();
            let mut tx = stale_notifier.clone();
            tokio::spawn(async move {
                let res = transport.send_append_entries(node_id, msg).await;
                if let Ok(res) = res {
                    if res.term > my_term {
                        tx.send(res.term).unwrap_or_else(|_| {
                            // TODO not really an error
                            error!("Couldn't notify leader that he's stale(someone else probably notified him already");
                        });
                    }
                } else {
                    error!("sending heartbeat to {} failed: {:?}", node_id, res);
                }
            }).instrument(info_span!("heartbeat"));
        }
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        self.node.leader_id = Some(self.node.id);
        self.node.voted_for = None;

        info!("became leader for term {}", self.node.current_term);
        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        let (stale_notifier, mut stale_receiver) = mpsc::channel(self.node.number_of_nodes);

        loop {
            if self.node.state != ServerState::Leader {
                return Ok(());
            }

            tokio::select! {
                _ = heartbeat_interval.tick() => {
                        self.send_heartbeat(stale_notifier.clone()).await;
                },
                res = stale_receiver.next() => {
                    let res = res.unwrap();
                    info!(self.node.current_term, new_term=res, "Received out of date term in reply");
                    self.node.try_update_term(res, None);
                    return Ok(())
                },
                res = self.node.receiver.next() => {
                    // TODO can this channel close prematurely?
                    let cmd = res.unwrap();
                    self.handle_command(cmd).await;
                }

            }
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, V: Value, T: Transport<V>> CommandHandler<V> for LeaderState<'a, V, T> {
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        return self.node.on_receive_append_entry(req);

    }

    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        return self.node.on_receive_request_vote(&req);
    }
}


/// In which state is the server currently at
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ServerState {
    Follower,
    Candidate,
    Leader,
}

/// Contains all state used by any node
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Node<V: Value, T: Transport<V>> {

    /* related to async & message sending */

    #[derivative(Debug="ignore")]
    /// Transport, used for making request RPCs and receiving response RPCs (proactive)
    pub transport: T,

    #[derivative(Debug="ignore")]
    /// Used for receiving request RPCs (reactive)
    pub receiver: mpsc::UnboundedReceiver<NodeCommand<V>>,

    /// Node ID
    pub id: Id,

    /// Leader ID. None when we are a candidate
    pub leader_id: Option<Id>,

    #[derivative(Debug="ignore")]
    /// IDs of other nodes
    pub other_nodes: Vec<Id>,

    /// Number of nodes
    pub number_of_nodes: usize,

    /// Server state
    pub state: ServerState,

    /// How long can I go without hearing from the leader in order
    /// to trigger an election
    pub election_timeout: Duration,

    /* persistent state */

    /// Log entries, begins with index 1.
    /// Indices that are 0 are effectively sentinel values
    pub storage: InMemoryStorage<V>,

    /// The latest term the server has seen
    pub current_term: usize,

    /// ID of candidate for which we've voted
    pub voted_for: Option<Id>,

    /* volatile state */

    /// Index of highest log entry known to be committed (replicated to a majority quorum)
    pub commit_index: usize,

    // TODO, maybe add last_applied
}

impl <V: Value, T: Transport<V>> Node<V, T> {
    pub fn new(id: usize,
               number_of_nodes: usize,
               transport: T,
               receiver: mpsc::UnboundedReceiver<NodeCommand<V>>) -> Self {
        let state = if id == 0 { ServerState::Leader } else { ServerState::Follower};
        Node {
            transport,
            receiver,
            id,
            leader_id: Some(0),
            other_nodes: (0 .. number_of_nodes).filter(|&cur_id| cur_id != id).collect(),
            number_of_nodes,
            election_timeout: generate_election_timeout(),
            state,
            storage: Default::default(),
            current_term: 0,
            voted_for: None,
            commit_index: 0,
        }
    }

    /// Size of a majority quorum (the minimal amount of valid nodes)
    pub fn quorum_size(&self) -> usize {
        (self.number_of_nodes / 2) + 1
    }

    /// Iterator over all other node IDs
    pub fn all_other_nodes(&self) -> impl Iterator<Item = Id> {
        self.other_nodes.clone().into_iter()
    }
}


/* Following block contains logic shared with all states of a raft node */
impl <V: Value, T: std::fmt::Debug + Transport<V>> Node<V, T> {

    #[instrument]
    /// The main loop - this does everything, and it has ownership of the Node
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        // the state machine of the consensus module, at first we always follow

        loop {
            info!(state = ?self.state, "Switching to new state");
            match self.state{
                ServerState::Follower => FollowerState::new(&mut self).run_loop().await?,
                ServerState::Candidate => CandidateState::new(&mut self).run_loop().await?,
                ServerState::Leader => LeaderState::new(&mut self).run_loop().await?,
            }
        }
    }

    /// Changes the state of the node
    #[instrument]
    pub fn change_state(&mut self, new_state: ServerState) {
        // from an implementation standpoint, this will cause the old state's loop to terminate
        assert_ne!(self.state, new_state);
        self.state = new_state;
    }

    /// This is the main entry point of a `NodeCommunicator` into a spawned `Node` object
    #[instrument]
    pub async fn on_request(&mut self, req: NodeCommand<V>) {
        match req {
            NodeCommand::AE(ae, res) => {
                res.send(self.on_receive_append_entry(ae)).unwrap();
            },
            NodeCommand::RV(rv, res) => {
                res.send(self.on_receive_request_vote(&rv)).unwrap();
            }
        }
    }

    /// Updates the current term to the given one, if it's more up to date.
    /// Also updates the leader in that case. Returns true if the term was indeed updated
    pub fn try_update_term(&mut self, term: usize, leader: Option<Id>) -> bool {
        if term > self.current_term {
            self.current_term = term;
            self.change_state(ServerState::Follower);
            self.leader_id = leader;
            info!("found newer term {} with leader {:?}, becoming follower.", term, leader);
            return true
        }
        return false
    }

    /// Invoked by any node upon receiving a request to vote
    #[instrument]
    pub fn on_receive_request_vote(&mut self, req: &RequestVote) -> Result<RequestVoteResponse, RaftError> {
        // 1. Our term is more updated
        if self.current_term > req.term {
            info!(cur_term=self.current_term, vote_term=req.term,
                   "My term is more up-to-date, ignoring vote request");
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        // if the candidate has a higher term, then convert to follower(§5.1)
        // in that case, we don't know the leader id(as the candidate doesn't know it either)
        self.try_update_term(req.term, None);

        if self.voted_for.is_some() && self.voted_for != Some(req.candidate_id) {
            info!(cur_vote=self.voted_for.unwrap(),
                  "I already voted for someone else, ignoring vote request",
                 );
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        if (self.storage.last_log_term(), self.storage.last_log_index()) >
            (req.last_log_term, req.last_log_index) {
            info!(last_log_term = self.storage.last_log_term(),
                  last_log_index = self.storage.last_log_index(),
                 "My log is more up to date than the candidate's log"
            );
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        info!("I voted for {}", req.candidate_id);
        self.voted_for = Some(req.candidate_id);
        return Ok(RequestVoteResponse::vote_yes(self.current_term));
    }

    /// Invoked by any node upon receiving a request to append entries
    #[instrument]
    pub fn on_receive_append_entry(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        // 1. The sender is not a leader anymore
        if req.term < self.current_term {
            warn!("old leader (my term = {}, other term = {})", req.term, self.current_term);
            return Ok(AppendEntriesResponse::failed(self.current_term));
        }

        self.try_update_term(req.term, Some(req.leader_id));

        // this is a heartbeat, nothing to do
        if req.entries.is_empty() {
            trace!("got heartbeat");
            return Ok(AppendEntriesResponse::success(self.current_term));
        }

        info!("got receive append entry");

        // 2. We can't append entries as we have a mismatch - the last entry in our log doesn't
        //    match (in index or term) the one expected by the leader
        match self.storage.get(&req.prev_log_index) {
            None => {
                // TODO: should last_applied be a different variable?
                warn!("our log is too short, our last applied index is {}, required prev index is {}",
                      self.storage.last_log_index(), req.prev_log_index);
                return Ok(AppendEntriesResponse::failed(self.current_term))
            },
            Some(LogEntry { term, ..}) if *term != req.prev_log_term => {
                warn!("our log contains a mismatch at the request's prev index {}, our entry's term is {},\
                       the required prev term is {}",
                      req.prev_log_index, *term, req.prev_log_term);
                return Ok(AppendEntriesResponse::failed(self.current_term));
            },
            _ => {}
        }

        // 3. Find conflicting entries
        let index_to_clip_from = {
            let mut clip_index = None;
            for (insertion_index, LogEntry { term, ..}) in req.indexed_entries() {
                assert!(insertion_index >= req.prev_log_index + 1);
                match self.storage.get(&insertion_index) {
                    Some(LogEntry { term: my_term, ..}) if *term != *my_term => {
                        warn!("our log contains a mismatch at an inserted item index {}\
                               our entry's term is {}, the inserted term is {}",
                        insertion_index, my_term, term);
                        clip_index = Some(insertion_index);
                        break;
                    },
                    _ => {}
                }
            }
            clip_index
        };
        // delete all entries after the conflicting one
        if let Some(index_to_clip_from) = index_to_clip_from {
            assert!(index_to_clip_from >= req.prev_log_index + 1);
            let indices_to_clip = (index_to_clip_from ..= self.storage.last_log_index());
            for ix in indices_to_clip {
                let _removed = self.storage.remove(&ix);
                assert!(_removed.is_some());
            }
        }

        // 4. Append entries not already in the log
        let index_to_append_from = index_to_clip_from.unwrap_or(req.prev_log_index + 1);
        for (insert_index, entry) in (index_to_append_from .. ).zip(req.entries.iter()) {
            let _prev = self.storage.insert(insert_index, entry.clone());
            assert!(_prev.is_some(), "Entry shouldn't be in the log by now");
        }

        // 5. update commit_index
        if req.leader_commit > self.commit_index {
            // we now know that 'req.leader_commit' is the minimal commit index,
            // but since we're a follower,
            // TODO use last_applied here maybe
            self.commit_index = req.leader_commit.min(self.storage.last_log_index());
        }

        Ok(AppendEntriesResponse::success(self.current_term))
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn node_initialization_and_getters() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut node = Node::<String, _>::new(2, 5, NoopTransport(), rx);
        assert_eq!(node.id, 2);
        assert_eq!(node.quorum_size(), 3);
        assert_eq!(node.state, ServerState::Follower);
        assert_eq!(node.all_other_nodes().collect::<Vec<_>>(), vec![0, 1, 3, 4]);
    }
}