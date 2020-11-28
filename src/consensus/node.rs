use crate::consensus::types::*;
use crate::consensus::transport::*;
use std::collections::BTreeMap;
use tracing::instrument;

/// State used by a follower
#[derive(Debug, Clone)]
pub struct FollowerState {
    pub leader_id: Id,

    pub time_since_last_heartbeat: std::time::Instant
}

impl FollowerState {
    pub fn should_begin_election(&self, timeout: std::time::Duration) -> bool {
        let time_passed = std::time::Instant::now() - self.time_since_last_heartbeat;
        time_passed >= timeout
    }
}

/// State used by a candidate
#[derive(Debug, Clone)]
pub struct CandidateState {
    pub responses: Vec<RequestVoteResponse>,
    pub required_quorum_size: usize
}

impl CandidateState {
    pub fn new(required_quorum_size: usize) -> Self {
        CandidateState {
            responses: Vec::new(), required_quorum_size
        }
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

#[derive(Debug, Clone)]
pub struct Node<V: Clone, T: Transport<V>> {

    transport: T,

    /// Node ID
    id: Id,

    /// Number of nodes
    number_of_nodes: usize,

    /// Server state
    state: ServerState,

    /* persistent state */

    /// Log entries, begins with index 1.
    /// Indices that are 0 are effectively sentinel values
    pub log: BTreeMap<usize, LogEntry<V>>,

    /// The latest term the server has seen
    pub current_term: usize,

    /// ID of candidate for which we've voted
    pub voted_for: Option<Id>,

    /* volatile state */

    /// Index of highest log entry known to be committed (replicated to a majority quorum)
    pub commit_index: usize,

}

impl <V: std::fmt::Debug + Clone, T: std::fmt::Debug + Transport<V>> Node<V, T> {
    pub fn new(id: usize, number_of_nodes: usize, transport: T) -> Self {
        Node {
            transport,
            id,
            number_of_nodes,
            state: ServerState::Follower(FollowerState {
                time_since_last_heartbeat: std::time::Instant::now(),
                leader_id: 0
            }),
            log: Default::default(),
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
        (0 .. self.number_of_nodes)//.filter(|&id| id != self.id)
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

    /// The index of the highest(last) log entry that was applied to the state machine, or 0 if nothing
    /// was applied. Generally this is more useful when we have snapshotting
    pub fn last_applied(&self) -> usize {
        assert!(!self.log.contains_key(&0));
        *self.log.keys().last().unwrap_or(&0)
    }

    /// Invoked by any node upon receiving a request to vote
    #[instrument]
    pub fn on_receive_request_vote(&mut self, req: &RequestVote) -> RequestVoteResponse {
        // 1. Our term is more updated
        if self.current_term > req.term {
            return RequestVoteResponse::vote_no(self.current_term);
        }

        // 2. if we've yet to vote(or we only voted for the candidate - in case of multiple elections in
        // a row, we might receive the same vote request more than once, and `self.voted_for` won't be
        // reset, so we will vote for the same candidate again)
        // and the candidate's log is at least as up-to-date as our log (in particular, its term is
        // as big as our term)
        if (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id))
            && (req.last_log_term, req.last_log_index) >= (self.current_term, self.log.len())
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

        // 2. We can't append entries as we have a mismatch - the last entry in our log doesn't
        //    match (in index or term) the one expected by the leader
        match self.log.get(&req.prev_log_index) {
            None => {
                warn!("our log is too short, our last applied index is {}, required prev index is {}",
                      self.last_applied(), req.prev_log_index);
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
                match self.log.get(&insertion_index) {
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
            let indices_to_clip = (index_to_clip_from ..= self.last_applied());
            for ix in indices_to_clip {
                let _removed = self.log.remove(&ix);
                assert!(_removed.is_some());
            }
        }

        // 4. Append entries not already in the log
        let index_to_append_from = index_to_clip_from.unwrap_or(req.prev_log_index + 1);
        for (insert_index, entry) in (index_to_append_from .. ).zip(req.entries.iter()) {
            let _prev = self.log.insert(insert_index, entry.clone());
            assert!(_prev.is_some(), "Entry shouldn't be in the log by now");
        }

        // 5. update commit_index
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_applied());
        }

        AppendEntriesResponse::success(self.current_term)
    }
}
