use crate::consensus::types::*;
use crate::consensus::transport::*;
use crate::consensus::log::*;
use std::collections::BTreeMap;
use tracing::instrument;
use tokio::task;
use std::rc::Rc;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use tokio::sync::oneshot;

const MIN_ELECTION_TIMEOUT_MS: u64 = 150;
const MAX_ELECTION_TIMEOUT_MS: u64 = 300;

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

    pub leader_id: Id,

    pub time_since_last_heartbeat: Instant
}

impl <'a, V: Value, T: Transport<V>> FollowerState<'a, V, T> {
    /// Creates state used for a node who has just become a follower
    pub fn new(node: &'a mut Node<V, T>) -> Self {
        FollowerState {
            node,
            leader_id: 0,
            time_since_last_heartbeat: Instant::now()
        }
    }

    pub fn should_begin_election_at(&self, at: Instant,
                                    timeout: Duration) -> bool {
        let time_passed = at - self.time_since_last_heartbeat;
        time_passed >= timeout
    }

    pub fn should_begin_election(&self, timeout: Duration) -> bool {
        self.should_begin_election_at(Instant::now(), timeout)
    }

    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum ElectionResult {
    Lost,
    Pending,
    Won
}

/// State used by a candidate, during a single election
#[derive(Debug)]
pub struct CandidateState<'a, V: Value, T: Transport<V>> {
    pub node: &'a mut Node<V, T>,

    /// All votes, including the candidate's vote!
    pub responses: Vec<RequestVoteResponse>,
    pub required_quorum_size: usize,
    pub election_start_time: Instant,
    pub election_timeout: Duration
}

impl <'a, V: Value, T: Transport<V>> CandidateState<'a, V, T> {
    /// Creates state for a candidate who has just started an election
    pub fn new(candidate: &'a mut Node<V, T>) -> Self {
        // a candidate always votes for itself
        let my_vote = RequestVoteResponse {
            term: candidate.current_term,
            vote_granted: true
        };
        let required_quorum_size = candidate.quorum_size();
        /// TODO: should I generate a new election timeout?
        let election_timeout = candidate.election_timeout;
        CandidateState {
            node: candidate,
            responses: vec![my_vote],
            required_quorum_size,
            election_start_time: Instant::now(),
            election_timeout
        }
    }

    pub fn election_elapsed_at(&self, at: Instant) -> bool {
        (at - self.election_start_time) >= self.election_timeout
    }

    pub fn election_elapsed(&self) -> bool {
        self.election_elapsed_at(Instant::now())
    }

    /// Checks the election's status
    pub fn tally(&self) -> ElectionResult {
        let mut yes = 0;
        let mut no = 0;
        for vote in self.responses.iter() {
            if vote.vote_granted {
                yes += 1;
            } else {
                no += 1;
            }
        };
        if yes >= self.required_quorum_size {
            assert!(no < self.required_quorum_size);
            return ElectionResult::Won
        }
        if no >= self.required_quorum_size {
            assert!(yes < self.required_quorum_size);
            return ElectionResult::Lost
        }
        return ElectionResult::Pending
    }

    pub async fn start_election(&mut self) -> Result<(), RaftError> {
        self.node.current_term += 1;
        warn!("Election started for term {}", self.node.current_term);

        let local = task::LocalSet::new();
        for node_id in self.node.all_other_nodes().collect::<Vec<_>>() {
            let req = RequestVote {
                term: self.node.current_term,
                candidate_id: self.node.id,
                last_log_index: self.node.storage.last_log_index(),
                last_log_term: self.node.storage.last_log_term()
            };
            let mut transport = self.node.transport.clone();
            task::spawn_local(async move {
                let res = transport.send_request_vote(node_id, req).await;
                // self.responses.push(res.unwrap());
            });
        }
        Ok(())
    }

    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        self.start_election().await?;
        Ok(())
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

    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        Ok(())
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
#[derive(Debug)]
pub struct Node<V: Value, T> {

    /* related to async & message sending */
    local_set: tokio::task::LocalSet,
    pub transport: T,

    /// Node ID
    pub id: Id,

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

impl <V: Value, T> Node<V, T> {
    pub fn new(id: usize, number_of_nodes: usize, transport: T) -> Self {
        Node {
            local_set: tokio::task::LocalSet::new(),
            transport,
            id,
            other_nodes: (0 .. number_of_nodes).filter(|&cur_id| cur_id != id).collect(),
            number_of_nodes,
            election_timeout: generate_election_timeout(),
            state: ServerState::Follower,
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
        info!("Starting loop");

        loop {
            match self.state{
                ServerState::Follower => FollowerState::new(&mut self).run_loop().await?,
                ServerState::Candidate => CandidateState::new(&mut self).run_loop().await?,
                ServerState::Leader => LeaderState::new(&mut self).run_loop().await?,
            }
        }
    }

    /// Updates the current term to the given one, if it's more up to date
    pub fn observe_term(&mut self, term: usize) {
        if term > self.current_term {
            self.current_term = term;
        }
        // TODO convert to follower
    }

    /// Invoked by any node upon receiving a request to vote
    #[instrument]
    pub fn on_receive_request_vote(&mut self, req: &RequestVote) -> RequestVoteResponse {
        // 1. Our term is more updated
        if self.current_term > req.term {
            return RequestVoteResponse::vote_no(self.current_term);
        }
        self.observe_term(req.term);

        // 2. if we've yet to vote(or we only voted for the candidate - in case of multiple elections in
        // a row, we might receive the same vote request more than once, and `self.voted_for` won't be
        // reset, so we will vote for the same candidate again)
        // and the candidate's log is at least as up-to-date as our log (in particular, its term is
        // as big as our term)
        if (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id))
            && (req.last_log_term, req.last_log_index) >= (self.storage.last_log_term(), self.storage.last_log_index())
        {
            self.voted_for = Some(req.candidate_id);
            return RequestVoteResponse::vote_yes(self.current_term)
        }

        // Otherwise(we voted for a different candidate/our log is more up-to-date)
        return RequestVoteResponse::vote_no(self.current_term);
    }

    /// Invoked by any node upon receiving a request to append entries
    #[instrument]
    pub fn on_receive_append_entry(&mut self, req: AppendEntries<V>) -> AppendEntriesResponse {
        // 1. The sender is not a leader anymore
        if req.term < self.current_term {
            warn!("old leader (my term = {}, other term = {})", req.term, self.current_term);
            return AppendEntriesResponse::failed(self.current_term);
        }

        // the sender is on our own term or later
        self.observe_term(req.term);

        // 2. We can't append entries as we have a mismatch - the last entry in our log doesn't
        //    match (in index or term) the one expected by the leader
        match self.storage.get(&req.prev_log_index) {
            None => {
                // TODO: should last_applied be a different variable?
                warn!("our log is too short, our last applied index is {}, required prev index is {}",
                      self.storage.last_log_index(), req.prev_log_index);
                return AppendEntriesResponse::failed(self.current_term)
            },
            Some(LogEntry { term, ..}) if *term != req.prev_log_term => {
                warn!("our log contains a mismatch at the request's prev index {}, our entry's term is {},\
                       the required prev term is {}",
                      req.prev_log_index, *term, req.prev_log_term);
                return AppendEntriesResponse::failed(self.current_term);
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

        AppendEntriesResponse::success(self.current_term)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn node_initialization_and_getters() {
        let mut node = Node::<String, _>::new(2, 5, NoopTransport);
        assert_eq!(node.id, 2);
        assert_eq!(node.quorum_size(), 3);
        assert_eq!(node.state, ServerState::Follower);
        assert_eq!(node.all_other_nodes().collect::<Vec<_>>(), vec![0, 1, 3, 4]);
    }
}