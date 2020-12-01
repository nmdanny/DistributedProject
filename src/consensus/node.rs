use crate::consensus::types::*;
use crate::consensus::transport::*;
use crate::consensus::log::*;
use crate::consensus::node_communicator::{NodeCommand, CommandHandler};
use crate::consensus::follower::FollowerState;
use crate::consensus::candidate::CandidateState;
use crate::consensus::leader::LeaderState;
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
use async_trait::async_trait;
use futures::TryFutureExt;




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

        if (self.storage.last_log_index(), self.storage.last_log_term()) >
            (req.last_log_index, req.last_log_term) {
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
            let indices_to_clip = index_to_clip_from ..= self.storage.last_log_index();
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
        let node = Node::<String, _>::new(2, 5, NoopTransport(), rx);
        assert_eq!(node.id, 2);
        assert_eq!(node.quorum_size(), 3);
        assert_eq!(node.state, ServerState::Follower);
        assert_eq!(node.all_other_nodes().collect::<Vec<_>>(), vec![0, 1, 3, 4]);
    }
}