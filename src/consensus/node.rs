use crate::consensus::types::*;
use crate::consensus::transport::*;
use crate::consensus::log::*;
use std::collections::BTreeMap;
use tracing::instrument;
use tokio::task;
use std::rc::Rc;
use std::time::{Duration, Instant};



const MIN_ELECTION_TIMEOUT_MS: u64 = 150;
const MAX_ELECTION_TIMEOUT_MS: u64 = 300;

pub fn generate_election_timeout() -> Duration {
    use rand::distributions::{Distribution, Uniform};
    let between = Uniform::from(MIN_ELECTION_TIMEOUT_MS .. MAX_ELECTION_TIMEOUT_MS);
    let mut rng = rand::thread_rng();
    Duration::from_millis(between.sample(&mut rng))
}

/// State used by a follower
#[derive(Debug, Clone)]
pub struct FollowerState {
    pub leader_id: Id,

    pub time_since_last_heartbeat: Instant
}

impl FollowerState {

    pub fn should_begin_election_at(&self, at: Instant,
                                    timeout: Duration) -> bool {
        let time_passed = at - self.time_since_last_heartbeat;
        time_passed >= timeout
    }

    pub fn should_begin_election(&self, timeout: Duration) -> bool {
        self.should_begin_election_at(Instant::now(), timeout)
    }
}

#[derive(Debug, Clone)]
pub enum ElectionResult {
    Lost,
    Pending,
    Won
}

/// State used by a candidate, during a single election
#[derive(Debug, Clone)]
pub struct CandidateState {
    /// All votes, including the candidate's vote!
    pub responses: Vec<RequestVoteResponse>,
    pub required_quorum_size: usize,
    pub election_start_time: Instant,
    pub election_timeout: Duration
}

impl CandidateState {
    pub fn new<V: Value, T>(candidate: &Node<V, T>, election_start_time: Instant) -> Self {
        // a candidate always votes for itself
        let my_vote = RequestVoteResponse {
            term: candidate.current_term,
            vote_granted: true
        };
        CandidateState {
            responses: vec![my_vote],
            required_quorum_size: candidate.quorum_size(),
            election_start_time,
            election_timeout: candidate.election_timeout
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

}

#[derive(Debug, Clone)]
pub struct LeaderState {
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

/// In which state is the server currently at
#[derive(Debug, Clone)]
pub enum ServerState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState)
}

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
            state: ServerState::Follower(FollowerState {
                time_since_last_heartbeat: Instant::now(),
                leader_id: 0
            }),
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

    pub fn follower_state(&mut self) -> Option<&mut FollowerState> {
        match &mut self.state {
            ServerState::Follower(s) => Some(s),
            _ => None
        }
    }

    pub fn candidate_state(&mut self) -> Option<&mut CandidateState> {
        match &mut self.state {
            ServerState::Candidate(s) => Some(s),
            _ => None
        }
    }

    pub fn leader_state(&mut self) -> Option<&mut LeaderState> {
        match &mut self.state {
            ServerState::Leader(s) => Some(s),
            _ => None
        }
    }
}


/* Following block contains most of the core logic */
impl <V: Value, T: std::fmt::Debug + Transport<V>> Node<V, T> {

    /// The main loop
    pub async fn run() -> Result<(), anyhow::Error> {
        // the state machine of the consensus module, at first we always follow
        let mut state = FollowerState { time_since_last_heartbeat: Instant::now(), leader_id: 0};

        Ok(())
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

    pub fn tick_follower(&mut self) {
        match &self.state {
            ServerState::Follower(state) => {
                if state.should_begin_election(self.election_timeout) {
                    warn!("Did not receive heartbeat in too long, becoming candidate");
                    self.start_election();
                }
            }
            _ => {}
        }
    }

    pub fn tick_candidate(&mut self) {
        match &self.state {
            ServerState::Candidate(state) => {
                if state.election_elapsed() {
                    warn!("Election elapsed, starting another one");
                    self.start_election();
                }
            },
            _ => {}
        }
    }

    pub fn start_election(&mut self) {
        use std::sync::{Arc, Mutex};
        assert!(self.leader_state().is_none(), "A leader cannot start an election");
        self.state = ServerState::Candidate(CandidateState::new(self,
                                                                Instant::now()));
        self.current_term += 1;
        warn!("Election started for term {}", self.current_term);

        let mut cs = Rc::new(Mutex::new(CandidateState::new(self,
        Instant::now())));
        let local = task::LocalSet::new();
        for node_id in self.all_other_nodes().collect::<Vec<_>>() {
            let req = RequestVote {
                term: self.current_term,
                candidate_id: self.id,
                last_log_index: self.storage.last_log_index(),
                last_log_term: self.storage.get(&self.storage.last_log_index())
                    .map(|e| e.term)
                    .unwrap_or(0)
            };
            let mut transport = self.transport.clone();
            let mut cs = cs.clone();
            task::spawn_local(async move {
                let res = transport.send_request_vote(node_id, req).await;
                cs.lock().unwrap().responses.push(res.unwrap());
            });
        }
    }

    pub fn on_vote(&mut self, vote: RequestVoteResponse) {
        if self.current_term != vote.term {

        }
    }

    pub fn become_leader(&mut self) {

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
        assert!(node.follower_state().is_some());;
        assert!(node.leader_state().is_none());;
        assert!(node.candidate_state().is_none());;
        assert_eq!(node.all_other_nodes().collect::<Vec<_>>(), vec![0, 1, 3, 4]);
    }
}