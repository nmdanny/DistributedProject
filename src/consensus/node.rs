use crate::consensus::types::*;
use crate::consensus::transport::*;
use crate::consensus::log::*;
use crate::consensus::node_communicator::{NodeCommand, CommandHandler};
use crate::consensus::follower::FollowerState;
use crate::consensus::candidate::CandidateState;
use crate::consensus::leader::LeaderState;
use crate::consensus::state_machine::{StateMachine, NoopStateMachine, ForceApply};
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
use tokio::sync::broadcast;
use tracing_futures::Instrument;
use async_trait::async_trait;
use futures::TryFutureExt;
use std::cell::RefCell;


/// Size of applied commit notification channel. Note that in case of lagging receivers(clients), they will never block
/// the node from sending values, but they might lose some commit notifications - see https://docs.rs/tokio/0.3.5/tokio/sync/broadcast/index.html#lagging
const BROADCAST_CHAN_SIZE: usize = 1024;

/// In which state is the server currently at
#[derive(Debug, Clone, Eq, PartialEq, Copy)]
pub enum ServerState {
    Follower,
    Candidate,
    Leader,
}

/// Contains all state used by any node
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Node<V: Value, T: Transport<V>, S: StateMachine<V, T>> {

    /* related to async & message sending */

    #[derivative(Debug="ignore")]
    /// Transport, used for making request RPCs and receiving response RPCs (proactive)
    pub transport: T,

    #[derivative(Debug="ignore")]
    /// Used for receiving request RPCs (reactive)
    ///
    /// Note, this can be None in two cases:
    /// 1. At initialization, before hooking a node communicator
    /// 2. LeaderState will move out the receiver, since it needs to have mutable access to await
    ///    on the receiver(use it mutably), yet it also wraps a `Node` in a `Rc<RefCell<..>>`, and we 
    ///    must never hold a mutable shared borrow across await points.
    ///    (He will move the receiver back into the node before finishing)
    #[derivative(Debug="ignore")]
    pub receiver: Option<mpsc::UnboundedReceiver<NodeCommand<V>>>,

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
    pub commit_index: Option<usize>,

    /* related to state machine */


    /// Until the state machine is spawned, allows access to the machine(and a receiver
    /// needed to spawn it)
    #[derivative(Debug="ignore")]
    pub state_machine: Option<(S, mpsc::UnboundedReceiver<CommitEntry<V>>,
                                  mpsc::UnboundedReceiver<ForceApply<V>>)>,

    /// Leader can subscribe in order to be notified of commits
    #[derivative(Debug="ignore")]
    pub sm_result_sender: broadcast::Sender<(CommitEntry<V>, V::Result)>,

    // Used to notify SM of newly committed entries
    #[derivative(Debug="ignore")]
    pub commit_sender: Option<mpsc::UnboundedSender<CommitEntry<V>>>,

    // Used to force SM to apply a value without committing it
    #[derivative(Debug="ignore")]
    pub force_apply_sender: Option<mpsc::UnboundedSender<ForceApply<V>>>,

    #[derivative(Debug="ignore")]
    sm_phantom: std::marker::PhantomData<S>
}

impl <V: Value, T: Transport<V>, S: StateMachine<V, T>> Node<V, T, S> {
    pub fn new(id: usize,
               number_of_nodes: usize,
               transport: T) -> Self {
        let state = if id == 0 { ServerState::Leader } else { ServerState::Follower};
        let (sm_result_sender, _) = broadcast::channel(1024);
        Node {
            transport,
            receiver: None,
            id,
            leader_id: Some(0),
            other_nodes: (0 .. number_of_nodes).filter(|&cur_id| cur_id != id).collect(),
            number_of_nodes,
            state,
            storage: Default::default(),
            current_term: 0,
            voted_for: None,
            commit_index: None,
            state_machine: None,
            sm_result_sender,
            commit_sender: None,
            force_apply_sender: None,
            sm_phantom: Default::default()
        }
    }

    pub fn attach_state_machine(&mut self, mut machine: S) -> broadcast::Sender<S::PublishedEvent> {
        let (commit_sender, commit_receiver) = mpsc::unbounded_channel();
        let (force_apply_sender, force_apply_recv) = mpsc::unbounded_channel();
        
        self.commit_sender = Some(commit_sender);
        self.force_apply_sender = Some(force_apply_sender);
        let recv = machine.get_event_stream();
        self.state_machine = Some((machine, commit_receiver, force_apply_recv));
        recv
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
impl <V: Value, T: std::fmt::Debug + Transport<V>, S: StateMachine<V, T>> Node<V, T, S> {

    #[instrument(skip(self))]
    /// The main loop - this does everything, and it has ownership of the Node
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {

        let id = self.id;
        self.transport.before_node_loop(id).await;

        let task_set = task::LocalSet::new();

        task_set.run_until(async move {
        
            let (sm, commit_recv, fe_recv) = self.state_machine.take().expect("State machine was deleted");
            let sender = self.sm_result_sender.clone();
            let _jh = sm.spawn(commit_recv, fe_recv, sender);

            let node = Rc::new(RefCell::new(self));

            loop {
                let id = node.borrow().id;
                let state = node.borrow().state;
                info!(state = ?state, id = ?id, log=?node.borrow().storage, "@@@@@@@@ Node {} switching to state {:?} @@@@@@@",
                    id, state);
                match state {
                    ServerState::Follower => FollowerState::new(&mut node.borrow_mut()).run_loop().await?,
                    ServerState::Candidate => CandidateState::new(&mut node.borrow_mut()).run_loop().await?,
                    ServerState::Leader => LeaderState::new(node.clone()).run_loop().await?
                }
            }
        }).await
    }

    /// Changes the state of the node
    #[instrument]
    pub fn change_state(&mut self, new_state: ServerState) {
        self.state = new_state;
    }

    #[instrument]
    /// Updates the commit index, notifying subscribed clients of the new entries
    pub fn update_commit_index(&mut self, new_commit_index: Option<usize>)
    {
        assert!(self.commit_index <= new_commit_index, "Cannot decrease commit index");
        if self.commit_index == new_commit_index {
            return
        }

        assert!(new_commit_index.is_some(), "if new commit index is bigger than old, it cant be None");
        let new_commit_index = new_commit_index.unwrap();

        let old_commit_index = self.commit_index;
        self.commit_index = Some(new_commit_index);


        let new_entries_from = old_commit_index.map(|i| i + 1).unwrap_or(0);
        let new_entries_to_inc = new_commit_index;

        let new_entries = self.storage.get_from_to(
            new_entries_from, new_entries_to_inc + 1);

        info!("Updated commit index from {:?} to {:?}, new entries: {:?}",
              old_commit_index, self.commit_index, new_entries);

        for (entry, index) in new_entries.iter().zip(new_entries_from ..= new_entries_to_inc) {
            self.commit_sender
                .as_ref()
                .expect("SM wasn't initialized, no commit_sender")
                .send(CommitEntry {
                    index, term: entry.term, value: entry.value.clone()
                }).unwrap_or_else(|_e| {
                    panic!("Couldn't notify SM of commited entry, did the SM thread panic?");
                });
        }


    }

    /// Updates the current term to the given one, if it's more up to date.
    /// Also updates the leader in that case. Returns true if the term was indeed updated
    #[instrument]
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
            debug!(cur_term=self.current_term, vote_term=req.term,
                   "My term is more up-to-date, ignoring vote request");
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        // if the candidate has a higher term, then convert to follower(§5.1)
        // in that case, we don't know the leader id(as the candidate doesn't know it either)
        self.try_update_term(req.term, None);

        if self.voted_for.is_some() && self.voted_for != Some(req.candidate_id) {
            debug!(cur_vote=self.voted_for.unwrap(),
                  "I already voted for someone else, ignoring vote request",
                 );
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        if self.storage.last_log_index_term() > req.last_log_index_term {
            info!(my=?self.storage.last_log_index_term(), req=?req.last_log_index_term,
                "My log is more up to date than the candidate's log"
            );
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        debug!("I voted for {}", req.candidate_id);
        self.voted_for = Some(req.candidate_id);
        return Ok(RequestVoteResponse::vote_yes(self.current_term));
    }

    /// Invoked by any node upon receiving a request to append entries
    #[instrument]
    pub fn on_receive_append_entry(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {

        assert_ne!(req.leader_id, self.id, "A leader cannot send append entry to himself");
        assert!(req.entries.iter().all(|e| e.term <= req.term), "A leader cannot have entries whose term is higher than his own");

        // 1. Check if sender is stale leader
        if req.term < self.current_term {
            warn!("sender is stale leader (my term = {}, other term = {})", req.term, self.current_term);
            return Ok(AppendEntriesResponse::failed(self.current_term));
        }

        self.try_update_term(req.term, Some(req.leader_id));

        // 2. Check if we have a mismatch with the prev_log_index_term
        if req.prev_log_index_term.contains_entry() {
            let (prev_log_index, prev_log_term) = req.prev_log_index_term.0.unwrap();
            match self.storage.get(prev_log_index) {
                None => {
                    warn!("mismatching prev_log_term for index {}. my log is too short, their term: {}",
                          prev_log_index, prev_log_term);
                    return Ok(AppendEntriesResponse::failed(self.current_term));
                }
                Some(entry) => {
                    if entry.term != prev_log_term {
                        warn!("mismatching prev_log_term for index {}. mine: {}, their: {}",
                            prev_log_index, entry.term, prev_log_term);
                        return Ok(AppendEntriesResponse::failed(self.current_term));
                    }
                }
            }
        }

        // this is a heartbeat, nothing to do
        if req.entries.is_empty() {
            trace!("got heartbeat/empty entry list");
            return Ok(AppendEntriesResponse::success(self.current_term));
        }

        let insertion_index = req.prev_log_index_term.index()
            .map(|prev_log_index| prev_log_index + 1).unwrap_or(0);

        // 3. delete conflicting entries
        if let Some(conflicting_index) = self.storage.find_index_of_conflicting_entry(&req.entries,
                                                                                      insertion_index) {
            warn!("Found conflicting index {}", conflicting_index);
            assert!(self.commit_index.map(|commit_index| {
                conflicting_index > commit_index
            }).unwrap_or(true), "there cannot be conflicting entries before/at the commit index");
            self.storage.delete_entries(conflicting_index);
        }

        // 4. append entries that aren't in the log
        self.storage.append_entries_not_in_log(&req.entries, insertion_index);

        if req.leader_commit > self.commit_index {
            let index_of_last_new_entry = req.entries.len() - 1 + insertion_index;
            self.update_commit_index(req.leader_commit.min(Some(index_of_last_new_entry)));
        }

        Ok(AppendEntriesResponse::success(self.current_term))
    }

    pub fn on_receive_client_force_apply(&mut self, force_apply: ForceApply<V>) {
        self.force_apply_sender
            .as_ref()
            .expect("SM wasn't initialized, no force_apply_sender")
            .send(force_apply).unwrap_or_else(|_e| {
                error!("Couldn't send force-apply request+responder to state machine task");
            });
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[tokio::test]
    async fn node_initialization_and_getters() {
        let mut node = Node::<String, _, _>::new(
            2, 
            5, 
            NoopTransport());
        node.attach_state_machine(NoopStateMachine::default());
        assert_eq!(node.id, 2);
        assert_eq!(node.quorum_size(), 3);
        assert_eq!(node.state, ServerState::Follower);
        assert_eq!(node.all_other_nodes().collect::<Vec<_>>(), vec![0, 1, 3, 4]);
    }

    // just sanity checks on Ord
    #[tokio::test]
    async fn ord_on_option() {
        assert_eq!(None.min(Some(0)), None);
        assert_eq!(None.max(Some(0)), Some(0));
        assert_eq!(Some(0).min(None), None);
        assert_eq!(Some(0).max(None), Some(0));
        assert_eq!(Some(3).min(None), None);
        assert_eq!(Some(3).max(None), Some(3));

        assert!(None < Some(0) && Some(0) < Some(1) && Some(0) > None);
    }

    #[tokio::test]
    async fn node_on_receive_ae() {
        let mut node = Node::<i32, _, _>::new(2, 5, NoopTransport());
        node.attach_state_machine(NoopStateMachine::default());

        let req1 = AppendEntries {
            term: 0,
            leader_id: 0,
            prev_log_index_term: IndexTerm::no_entry(),
            entries: vec![
                LogEntry::new(1337, 0),
                LogEntry::new(1338, 0),
                LogEntry::new(1339, 0),
            ],
            leader_commit: None
        };


        // Valid request + RPCs are idempotent
        for _ in 1 .. 3 {
            let res1 = node.on_receive_append_entry(req1.clone()).unwrap();
            assert_eq!(res1.term, 0);
            assert!(res1.success);
        }

        // Another attempt of sending entries that are already in the log
        let req2 = AppendEntries {
            term: 0,
            leader_id: 0,
            prev_log_index_term: IndexTerm::new(0, 0),
            entries: vec![
                LogEntry::new(1338, 0),
                LogEntry::new(1339, 0),
            ],
            leader_commit: None
        };

        let res2 = node.on_receive_append_entry(req2).unwrap();
        assert_eq!(res2.term, 0);
        assert!(res2.success);

        let req3 = AppendEntries {
            term: 0,
            leader_id: 0,
            prev_log_index_term: IndexTerm::new(1, 0),
            entries: vec![
                LogEntry::new(1339, 0),
            ],
            leader_commit: None
        };

        let res3 = node.on_receive_append_entry(req3).unwrap();
        assert_eq!(res3.term, 0);
        assert!(res3.success);

        assert_eq!(node.storage.get_from(0), &[
            LogEntry::new(1337, 0),
            LogEntry::new(1338, 0),
            LogEntry::new(1339, 0),
        ]);

        assert_eq!(node.commit_index, None);
    }

    #[tokio::test]
    async fn node_on_receive_ae_clipping() {
        let mut node = Node::<i32, _, _>::new(2, 5, NoopTransport());
        node.attach_state_machine(NoopStateMachine::default());
        node.current_term = 2;


        // first request is just to initialize the node
        let req1 = AppendEntries {
            term: 4,
            leader_id: 0,
            prev_log_index_term: IndexTerm::no_entry(),
            entries: vec![
                LogEntry::new(0, 0),
                LogEntry::new(1, 0),
                LogEntry::new(2, 0),
                LogEntry::new(3, 2),
                LogEntry::new(4, 2),
                LogEntry::new(5, 3),
            ],
            leader_commit: Some(3)
        };

        let res1 = node.on_receive_append_entry(req1).unwrap();
        assert!(res1.success);
        assert_eq!(res1.term, 4);
        assert_eq!(node.leader_id, Some(0));
        assert_eq!(node.current_term, 4);
        assert_eq!(node.commit_index, Some(3));

        // another request is from a different leader at term 5
        // that leader did not see entries (4,2) and (5,3), thus they will be clipped
        let req2 = AppendEntries {
            term: 5,
            leader_id: 3,
            prev_log_index_term: IndexTerm::new(3, 2),
            entries: vec![
                LogEntry::new(1337, 5),
                LogEntry::new(1338, 5),
                LogEntry::new(1339, 5),
            ],
            leader_commit: Some(4)
        };
        let res2 = node.on_receive_append_entry(req2).unwrap();
        assert!(res2.success);
        assert_eq!(res2.term, 5);
        assert_eq!(node.current_term, 5);
        assert_eq!(node.leader_id, Some(3));
        assert_eq!(node.commit_index, Some(4));

        assert_eq!(node.storage.get_from(0), &[
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
            LogEntry::new(2, 0),
            LogEntry::new(3, 2),
            LogEntry::new(1337, 5),
            LogEntry::new(1338, 5),
            LogEntry::new(1339, 5),
        ]);
    }

    #[tokio::test]
    async fn node_on_receive_mismatching_ae() {
        let mut node = Node::<i32, _, _>::new(2, 5, NoopTransport());
        node.attach_state_machine(NoopStateMachine::default());
        node.current_term = 2;

        // first request is just to initialize the node
        let req1 = AppendEntries {
            term: 3,
            leader_id: 0,
            prev_log_index_term: IndexTerm::no_entry(),
            entries: vec![
                LogEntry::new(0, 0),
                LogEntry::new(1, 0),
            ],
            leader_commit: None
        };
        let res1 = node.on_receive_append_entry(req1).unwrap();
        assert!(res1.success);
        assert_eq!(res1.term, 3);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.commit_index, None);

        // now, a mismatching req
        let req2 = AppendEntries {
            term: 4,
            leader_id: 5,
            prev_log_index_term: IndexTerm::new(1, 3),
            entries: vec![
                LogEntry::new(1337, 3)
            ],
            leader_commit: Some(1)
        };
        let res2 = node.on_receive_append_entry(req2).unwrap();
        assert!(!res2.success);

        // ensure the failed request did not affect the node other than updating his term
        assert_eq!(res2.term, 4);
        assert_eq!(node.current_term, 4);
        assert_eq!(node.commit_index, None);

        assert_eq!(node.storage.get_from(0), &[
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
        ]);
    }
}