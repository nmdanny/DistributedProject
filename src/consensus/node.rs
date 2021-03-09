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
use serde;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::broadcast;
use tracing_futures::Instrument;
use async_trait::async_trait;
use futures::TryFutureExt;
use std::cell::RefCell;

use super::timing::RaftServerSettings;


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

    pub settings: RaftServerSettings,

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

    /// Used to notify communicator of newly published state machine events(serialized)
    #[derivative(Debug="ignore")]
    pub sm_publish_sender: broadcast::Sender<Vec<u8>>,

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
               transport: T,
                settings: RaftServerSettings) -> Self {
        let state = if id == 0 { ServerState::Leader } else { ServerState::Follower};
        let (sm_result_sender, _) = broadcast::channel(BROADCAST_CHAN_SIZE);
        let (sm_publish_sender, _) = broadcast::channel(BROADCAST_CHAN_SIZE);
        Node {
            transport,
            receiver: None,
            settings,
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
            sm_publish_sender,
            sm_result_sender,
            commit_sender: None,
            force_apply_sender: None,
            sm_phantom: Default::default()
        }
    }

    pub fn attach_state_machine(&mut self, mut machine: S) {
        let (commit_sender, commit_receiver) = mpsc::unbounded_channel();
        let (force_apply_sender, force_apply_recv) = mpsc::unbounded_channel();
        
        self.commit_sender = Some(commit_sender);
        self.force_apply_sender = Some(force_apply_sender);
        let mut recv = machine.get_event_stream().subscribe();
        let ser_sender = self.sm_publish_sender.clone();
        tokio::spawn(async move {
            while let Ok(res) = recv.recv().await {
                let ser = serde_json::to_vec_pretty(&res).unwrap();
                ser_sender.send(ser).unwrap_or_else(|_| {
                    warn!("Nobody is listening to serialized state machine outputs");
                    0
                });
            }
        }.instrument(info_span!("attach_state_machine event serialization loop", id=?self.id)));
        self.state_machine = Some((machine, commit_receiver, force_apply_recv));
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

#[derive(Debug, Copy, Clone)]
pub enum ChangeStateReason {
    WonElection { yes: usize, no: usize},
    FollowerTimeout { timeout_duration: std::time::Duration },
    SawHigherTerm { my_term: usize, new_term: usize, new_term_leader: Option<Id>},
    SomeoneElseWon { term: usize, leader: Id }
}

#[derive(Debug, Copy, Clone)]
pub enum UpdateCommitIndexReason {
    ObservedHigherLeaderCommit { leader_commit_index: Option<usize> },
    LeaderMatchIndexIncreased 
}

/* Following block contains logic shared with all states of a raft node */
impl <V: Value, T: std::fmt::Debug + Transport<V>, S: StateMachine<V, T>> Node<V, T, S> {

    #[instrument(skip(self), fields(id=self.id))]
    /// The main loop - this does everything, and it has ownership of the Node
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {

        let id = self.id;
        self.transport.before_node_loop(id).await;

        let task_set = task::LocalSet::new();

        task_set.run_until(async move {
        
            let (sm, commit_recv, fe_recv) = self.state_machine.take().expect("State machine was deleted");
            let sender = self.sm_result_sender.clone();
            let _jh = sm.spawn(self.id, commit_recv, fe_recv, sender);

            let node = Rc::new(RefCell::new(self));

            loop {
                let state = {
                    let node = node.borrow();
                    let id = node.id;
                    let state = node.state;
                    let storage = &node.storage;
                    let term = &node.current_term;
                    let commit_index = &node.commit_index;
                    info!(state = ?state, id = ?id, log=?storage, term=?term, commit_index=?commit_index,
                          important=true, "@@@@ Node {} switching to state {:?}",
                          id, state);
                    state
                };
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
    pub fn change_state(&mut self, new_state: ServerState, change_reason: ChangeStateReason) {
        info!(important=true, "!!! changing state from {:?} at term {} to {:?}, reason: {:?}",
              self.state, self.current_term, new_state, change_reason);
        self.state = new_state;
    }

    #[instrument]
    /// Updates the commit index, notifying subscribed clients of the new entries
    pub fn update_commit_index(&mut self, new_commit_index: Option<usize>, reason: UpdateCommitIndexReason)
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

        debug!(old=?old_commit_index, new=?self.commit_index, new_entries=?new_entries, reason=?reason, "Updated commit index");

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

    /// Observes a term(and a leader for an AE request), updating them and the state if necessary.
    /// Returns true iff the state was changed(to follower)
    #[instrument]
    pub fn try_update_term(&mut self, term: usize, leader: Option<Id>) -> bool {

        // Election safety property check
        if self.current_term == term && self.leader_id.is_some() && leader.is_some() {
            assert_eq!(self.leader_id, leader, 
                       "Election safety property violated: \
                        For term {}, my node's current leader is {:?}, \
                        yet I observed an RPC for that term whose leader is {:?}", term, self.leader_id, leader);
        }

        // If we got an AE(only request where leader is Some) for the current term
        if term == self.current_term && leader.is_some(){
            assert_ne!(self.state, ServerState::Leader, "A leader node doesn't send itself AE, so he cannot receive an AE \
                                                         for the current term, and there are no other leaders for said term");
            if self.state == ServerState::Follower {
                if self.leader_id.is_none() {
                    self.leader_id = leader;
                    info!("Follower at term {} is now aware of leader for said term: {}", term, leader.unwrap());
                } else {
                    assert!(self.leader_id == leader, "A follower cannot see an AE whose leader is different from the \
                                                       current leader for said term");
                }
                return false;
            }

            if self.state == ServerState::Candidate {
                self.leader_id = leader;
                self.change_state(ServerState::Follower, ChangeStateReason::SomeoneElseWon {
                    term, leader: leader.unwrap()
                });
                return true;
            } else {
                unreachable!("Not a leader, not a follower and not a candidate, impossible");
            }
        }

        // If we saw a request/response with a higher term
        if term > self.current_term  {
            let old_term = self.current_term;
            self.change_state(ServerState::Follower, ChangeStateReason::SawHigherTerm {
                my_term: old_term, new_term: term, new_term_leader: leader
            });
            self.current_term = term;
            self.leader_id = leader;
            return true;
        }
        return false;
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
            debug!(my_index_term=?self.storage.last_log_index_term(), candidate_index_term=?req.last_log_index_term,
                "My log is more up to date than the candidate's log, rejecting vote"
            );
            return Ok(RequestVoteResponse::vote_no(self.current_term));
        }

        debug!("I voted for {}", req.candidate_id);
        self.voted_for = Some(req.candidate_id);
        return Ok(RequestVoteResponse::vote_yes(self.current_term));
    }

    /// Invoked by any node upon receiving a request to append entries
    #[instrument(skip(self))]
    pub fn on_receive_append_entry(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {

        assert_ne!(req.leader_id, self.id, "A leader cannot send append entry to himself");
        assert!(req.entries.iter().all(|e| e.term <= req.term), "A leader cannot have entries whose term is higher than his own");

        // note that in any case we return early, we also perform
        // step 5 - in case leader_commit > commit_index, update our commit_index
        // to the latest entry. (Even if the leader is stale or whatever, his leader_commit should always be correct)

        // 1. Check if sender is stale leader
        if req.term < self.current_term {
            trace!("got ae: sender is stale leader (my term = {}, other term = {})", req.term, self.current_term);
            // self.observe_leader_commit(req.leader_commit);
            return Ok(AppendEntriesResponse::failed(self.current_term));
        }

        self.try_update_term(req.term, Some(req.leader_id));

        if req.entries.is_empty() {
            trace!("got ae: heartbeat/empty entry list, OK");
            self.observe_leader_commit(req.leader_commit);
            return Ok(AppendEntriesResponse::success(self.current_term));
        }

        trace!("got ae: with {} entries", req.entries.len());

        // 2. Check if we have a mismatch with the prev_log_index_term
        if req.prev_log_index_term.contains_entry() {
            let (prev_log_index, prev_log_term) = req.prev_log_index_term.as_tuple().unwrap();
            match self.storage.get(prev_log_index) {
                None => {
                    warn!(
                        req=?req,
                        "mismatching prev_log_term for index {}, term of entry: {}. My log is too short(len: {})",
                          prev_log_index, prev_log_term, self.storage.len());
                          
                    self.observe_leader_commit(req.leader_commit);
                    return Ok(AppendEntriesResponse::failed(self.current_term));
                }
                Some(entry) => {
                    if entry.term != prev_log_term {
                        warn!(req=?req,
                            "mismatching prev_log_term for index {}. mine: {}, their: {}",
                            prev_log_index, entry.term, prev_log_term);
                        self.observe_leader_commit(req.leader_commit);
                        return Ok(AppendEntriesResponse::failed(self.current_term));
                    }
                }
            }
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

        // TODO: sometimes this assert fails, why?
        let _index_of_last_new_entry = req.entries.len() - 1 + insertion_index;
        assert_eq!(_index_of_last_new_entry, self.storage.last_log_index_term().index().unwrap());

        // 5. if leader's commit index is bigger than our own, update 
        self.observe_leader_commit(req.leader_commit);

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

    pub fn observe_leader_commit(&mut self, leader_commit_index: Option<usize>) {
        if leader_commit_index > self.commit_index {
            let last_log_entry = self.storage.last_log_index_term().index();
            self.update_commit_index(leader_commit_index.min(last_log_entry),
                                     UpdateCommitIndexReason::ObservedHigherLeaderCommit { leader_commit_index });
        }
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
            NoopTransport(),
            RaftServerSettings::default());
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
        let mut node = Node::<i32, _, _>::new(2, 5, NoopTransport(), RaftServerSettings::default());
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
        let mut node = Node::<i32, _, _>::new(2, 5, NoopTransport(), RaftServerSettings::default());
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
        let mut node = Node::<i32, _, _>::new(2, 5, NoopTransport(), RaftServerSettings::default());
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
        // and commit index
        assert_eq!(res2.term, 4);
        assert_eq!(node.current_term, 4);
        assert_eq!(node.commit_index, Some(1));

        assert_eq!(node.storage.get_from(0), &[
            LogEntry::new(0, 0),
            LogEntry::new(1, 0),
        ]);
    }
}