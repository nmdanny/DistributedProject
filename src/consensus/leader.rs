use std::{cell::{Cell, RefCell}, sync::Arc};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use crossbeam::atomic::AtomicCell;
use futures::{Future, FutureExt, TryFutureExt};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::sync::watch::Ref;
use tokio::task;
use tokio_stream::StreamExt;
use tracing_futures::Instrument;

use crate::logging::TimeOp;
use crate::consensus::node::{Node, ServerState, UpdateCommitIndexReason};
use crate::consensus::node_communicator::{CommandHandler, NodeCommand};
use crate::consensus::state_machine::StateMachine;
use crate::consensus::timing::HEARTBEAT_INTERVAL;
use crate::consensus::transport::Transport;
use crate::consensus::types::*;

#[derive(Derivative)]
#[derivative(Debug)]
/// Contains state used to replicate the leader's data to a peer.
/// Note, this is re-initialized every time a node becomes a leader
pub struct PeerReplicationStream<V: Value, T: Transport<V>, S: StateMachine<V, T>> {

    #[derivative(Debug="ignore")]
    /// Reference to node
    pub node: Rc<RefCell<Node<V, T, S>>>,

    /// Peer ID
    pub id: Id,

    /// The index of the new log entry(in the leader's log) to send to the peer
    pub next_index: usize,

    /// The maximal index of a leader log entry known to be replicated on the peer
    pub match_index: Option<usize>,

    #[derivative(Debug="ignore")]
    /// Triggered whenever there's a heartbeat or a client submits a new request
    pub tick_receiver: watch::Receiver<()>,

    #[derivative(Debug="ignore")]
    /// Used to notify leader of match index updates
    pub match_index_sender: mpsc::Sender<(Id, usize)>,

    /// Term of leader
    pub leader_term: usize,

    /// Used to send AEs to peer
    #[derivative(Debug="ignore")]
    pub transport: T,

    /// Time at which last AE (heartbeat/data) was sent
    pub last_ae_send: Arc<AtomicCell<Instant>>,

    #[derivative(Debug="ignore")]
    phantom: std::marker::PhantomData<V>

}

#[derive(Debug, Clone)]
pub struct StaleLeader { pub newer_term: usize }


/// Creates an AppendEntries request containing `log[index ..]`, using `index-1` as the previous log
/// index(or an empty IndexTerm if `index = 0`)
/// In case `index == log.len`, this will be treated as a heartbeat request.
fn create_append_entries_for_index<V: Value, T: Transport<V>, S: StateMachine<V,T>>(node: &Node<V, T, S>, index: usize) -> AppendEntries<V> {
    assert!(index <= node.storage.len(), "invalid index");
    let leader_id = node.id;
    let term = node.current_term;

    let entries = if index < node.storage.len() {
        node.storage.get_from(index).iter().cloned().collect()
    } else { Vec::new() };

    let leader_commit = node.commit_index;

    let prev_log_index_term = if index >= 1 {
        let entry = node.storage.get(index - 1).unwrap();
        IndexTerm::new(index - 1, entry.term)
    } else { IndexTerm::no_entry() };

    AppendEntries {
        leader_id, term, entries, leader_commit, prev_log_index_term
    }
}

#[derive(Error, Debug)]
pub enum ReplicationLoopError {
    #[error("Stale leader error")]
    StaleLeaderError(StaleLeader),

    #[error("Peer error: {0}")]
    PeerError(RaftError),
}

#[instrument(skip(transport), level = "trace")]
/// Sends an AE, treating network errors or stale responses as errors, and conflict or successful
/// responses as success.
async fn send_ae<V: Value, T: Transport<V>>(transport: &T, to: Id, current_term: usize, req: AppendEntries<V>) 
    -> Result<AppendEntriesResponse, ReplicationLoopError> 
{
    let res = transport.send_append_entries(to, req).await;
    trace!(?res, "send_ae response");
    let res = res .map_err(ReplicationLoopError::PeerError)?;
    match res.meaning(current_term) {
        AEResponseMeaning::Ok => Ok(res),
        AEResponseMeaning::Conflict => Ok(res),
        AEResponseMeaning::Stale { newer_term } =>
            Err(ReplicationLoopError::StaleLeaderError(StaleLeader { newer_term })),
    }
}

impl <V: Value, T: Transport<V>, S: StateMachine<V, T>> PeerReplicationStream<V, T, S> {
    pub fn new(node: Rc<RefCell<Node<V, T, S>>>, id: Id,
               tick_receiver: watch::Receiver<()>,
               match_index_sender: mpsc::Sender<(Id, usize)>) -> Self {


        // at first, assume the peer is completely synchronized, so `next_index` points to one past
        // the last element
        let node_b = node.borrow();
        let next_index = node_b.storage.len();
        let leader_term = node_b.current_term;
        let transport = node_b.transport.clone();
        drop(node_b);
        let match_index = None;
        PeerReplicationStream {
            node, id, next_index, match_index, tick_receiver, match_index_sender,
            leader_term, transport, phantom: Default::default(),
            last_ae_send: Arc::new(AtomicCell::new(Instant::now()))
        }
    }

    /// Tries to replicate all data available at this time to the peer,
    /// or sends a heartbeat if he's already synchronized.
    #[instrument]
    pub async fn try_replication(&mut self) -> Result<(), ReplicationLoopError>
    {
        let node = self.node.borrow();

        /* Note, it's possible that during an await point, a concurrent client write request will cause
           the storage to increase, but for simplicity, we will not synchronize in call values
           beyond that point - that will be done in the next call to `try_replication`

           (Of course, values at or before 'last_log_index' cannot change in term or value as we're
           the leader)
        */
        let last_log_index = node.storage.last_log_index_term().index();
        drop(node);

        let mut ran = false;

        // we run at least once(in case this is a heartbeat), or as long as we are not synchronized
        while self.match_index != last_log_index || !ran {
            ran = true;
            let node = self.node.borrow();
            let span = debug_span!("whileLoopStart",
                                    peer_id=?self.id,
                                    match_index=?self.match_index,
                                    next_index=?self.next_index,
                                    commit_index=?node.commit_index,
                                    storage=?node.storage
            );
            span.in_scope(|| {
                trace!("Synchronizing node {}, match_index: {:?}, last_log_index_term: {:?}",
                       self.id, self.match_index, node.storage.last_log_index_term());
            });


            assert!(self.next_index <= node.storage.len(), "if we're not synchronized, next_index must be valid");
            let req = create_append_entries_for_index(&node, self.next_index);
            let is_heartbeat = req.entries.is_empty();
            assert_eq!(req.entries.len(), node.storage.len() - self.next_index);

            drop(node);
            let res = send_ae(&self.transport, self.id, self.leader_term, req).await?;
            self.last_ae_send.store(Instant::now());
            let node = self.node.borrow();
            let span = info_span!("whileLoopEnd",
                                  peer_id=?self.id,
                                  match_index=?self.match_index,
                                  next_index=?self.next_index,
                                  commit_index=?node.commit_index,
                                  recorded_last_log_index=?last_log_index,
                                  storage=?node.storage);
            drop(node);

            if res.success && !is_heartbeat {
                let last_index = last_log_index
                    .expect("If this wasn't a heartbeat, last_log_index shouldn't be None");
                self.next_index = last_index + 1;
                self.match_index = Some(last_index);
                self.match_index_sender.send((self.id, last_index)).unwrap_or_else(|e| {
                    error!("Peer couldn't send match index to leader: {:?}", e);
                }).await;
                span.in_scope(|| {
                    debug!("Successfully replicated to peer {} values up to, including, index {}",
                           self.id, last_index);
                });
                return Ok(())
            } else if res.success {
                return Ok(())
            }

            assert!(self.next_index > 0, "We could not have failed for next_index = 0");
            self.next_index -= 1;
        }
        Ok(())

    }

    /// Creates a loop which sends heartbeats. While `run_replication_loop` can also send
    /// heartbeats, it can hang when sending large AEs. This ensures heartbeats will still be sent.
    pub fn spawn_heartbeat_loop(&self, stale_sender: mpsc::Sender<StaleLeader>) -> JoinHandle<()> {
        let transport = self.transport.clone();
        let id = self.id.clone();
        let last_ae_send = self.last_ae_send.clone();
        let term = self.leader_term;
        // note that the commit index & prev_log_index_term here are stale, they aren't updated
        // and in fact, `prev_log_index_term` refers to no entry. 
        // (In my implementation of `Node::on_receive_append_entry` this isn't an issue)
        //
        // Therefore, this loop doesn't handle `match_index` (handled by `try_replication` which
        // can also send heartbeats), it's sole purpose is to prevent premature leader switch
        // when sending big AE entries, and this can run on a different thread rather than on the node's local set.

        let leader_commit = self.node.borrow().commit_index;
        let leader_id = self.node.borrow().id;
        tokio::spawn(async move {
            let delay = tokio::time::sleep(HEARTBEAT_INTERVAL);
            tokio::pin!(delay);
            loop {
                delay.as_mut().await;
                // in case the replication task already sent a heartbeat/message before
                
                let last_sent = last_ae_send.load();
                if Instant::now() - last_sent < HEARTBEAT_INTERVAL {
                    delay.as_mut().reset((last_sent + HEARTBEAT_INTERVAL).into());
                    continue;
                }
                let transport = transport.clone();
                debug!("sending heartbeat from alternate loop");
                let res = send_ae(&transport, id, term, AppendEntries {
                    entries: Vec::new(),
                    leader_commit,
                    term,
                    leader_id,
                    prev_log_index_term: IndexTerm::no_entry()
                }).await;
                debug!("heartbeat send response: {:?}", res);
                let now = Instant::now();
                last_ae_send.store(now);
                delay.as_mut().reset((now + HEARTBEAT_INTERVAL).into());
                match res {
                    Ok(_) => {},
                    Err(ReplicationLoopError::PeerError(_e)) => 
                        error!(trans=true, net_err=true, "Received IO error during heartbeat stream for {}, will try again later: {}",
                               id, _e),
                    Err(ReplicationLoopError::StaleLeaderError(stale)) => {
                        let _ = stale_sender.send(stale).await;
                        return;
                    }
                }
            }
        }.instrument(debug_span!("heartbeat-loop", from=?leader_id, to=?id, ?term)))
    }

    /// To run concurrently as long as the leader is active.
    pub async fn run_replication_loop(mut self, stale_sender: mpsc::Sender<StaleLeader>) {
        let current_term = self.node.borrow().current_term;
        while let Ok(()) = self.tick_receiver.changed().await {
            match self.try_replication().await {
                Ok(_) => {},
                Err(ReplicationLoopError::PeerError(e)) => {
                    error!(trans=true, net_err=true, "Received IO error during replication stream for {}, will try again later: {}",
                           self.id, e);
                },
                Err(ReplicationLoopError::StaleLeaderError(stale)) => {
                    warn!("Determined I'm a stale leader via peer {}, my term is {}, newer term is {}",
                          self.id, current_term, stale.newer_term);
                    let _ = stale_sender.send(stale).await;
                    return;
                }
            }
        }
    }

}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct PendingWriteRequest<V: Value> {
    #[derivative(Debug = "ignore")]
    pub responder: Option<oneshot::Sender<Result<ClientWriteResponse<V>,RaftError>>>,

    pub pending_entry_log_index: usize,
}

impl <V: Value> std::ops::Drop for PendingWriteRequest<V> {
    fn drop(&mut self) {
        let responder = self.responder.take();
        if let Some(responder) = responder {
            let _ = responder.send(Err(RaftError::NoLongerLeader()));
        }
    }
}

impl <V: Value> PendingWriteRequest<V> {
    pub fn new(pending_entry_log_index: usize,
                         responder: oneshot::Sender<Result<ClientWriteResponse<V>,RaftError>>) -> Self
    {
       PendingWriteRequest { pending_entry_log_index, responder: Some(responder) }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct LeaderState<'a, V: Value, T: Transport<V>, S: StateMachine<V, T>>{

    pub term: usize,

    #[derivative(Debug="ignore")]
    pub node: Rc<RefCell<Node<V, T, S>>>,

    #[derivative(Debug="ignore")]
    /// Used to notify replication streams of a heartbeat or a
    /// newly inserted entry that came from a client
    pub replicate_sender: watch::Sender<()>,

    #[derivative(Debug="ignore")]
    /// To be passed to all peer replication streams
    pub replicate_receiver: watch::Receiver<()>,

    /// Used to resolve client write requests
    pub pending_writes: BTreeMap<usize, PendingWriteRequest<V>>,

    /// Maps peer IDs to their latest match index
    pub match_indices: BTreeMap<Id, Option<usize>>,

    #[derivative(Debug="ignore")]
    phantom: std::marker::PhantomData<&'a ()>

}


impl<'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> LeaderState<'a, V, T, S> {
    /// Creates state for a node who just became a leader
    pub fn new(node: Rc<RefCell<Node<V, T, S>>>) -> Self {

        let match_indices = node.borrow().all_other_nodes()
            .map(|i| (i, None)).collect();

        let term = node.borrow().current_term;
        let (replicate_sender, replicate_receiver) = watch::channel(());
        LeaderState {
            term,
            node,
            replicate_sender,
            replicate_receiver,
            pending_writes: Default::default(),
            match_indices,
            phantom: Default::default()
        }
    }



    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        // let mut node = self.node.borrow_mut();
        let mut receiver = self.node.borrow_mut().receiver
            .take().expect("Receiver was null");

        // drop(node);
        let res = self.run_loop_inner(&mut receiver).await;

        // since only LeaderState can take out `receiver`, we ensure the receiver is back in the
        // state no matter if run_loop_inner has exited in an error or not.

        self.node.borrow_mut().receiver = Some(receiver);
        res

    }

    #[instrument]
    pub fn find_higher_commit_index(&mut self)
    {
        let mut node = self.node.borrow_mut();

        let maximal_n = node.storage.len() - 1;

        // (if node is committed, commit_index + 1 == storage.len(), hence why we use 'min)
        let minimal_n = node.commit_index.map(|i| i + 1).unwrap_or(0)
            .min(maximal_n);

        assert!(minimal_n <= maximal_n, "index math sanity check");

        trace!("match index increased, trying all N from {} to {}", maximal_n, minimal_n);

        // find 'n' such that n > commit_index AND forall peer. match_index[peer] >= n
        // AND log[n].term = current_term
        // we try to be optimistic, starting with the highest 'n' and going down
        for n in (minimal_n ..= maximal_n).rev()
        {
            if node.storage.get(n).unwrap().term != node.current_term  {
                trace!("while checking N={}, found that term mismatches our own, skipping", n);
                continue;
            }

            let match_count = self.match_indices
                .values()
                .filter(|v| **v >= Some(n))
                .count();

            trace!("while checking N={}, match count is {}", n, match_count);

            // add 1 for the leader itself who also matches index 'n'
            if match_count + 1 >= node.quorum_size()
            {
                debug!("Found that a majority quorum of size {} who committed all values up to(including) index {}",
                       match_count + 1, n);
                node.update_commit_index(Some(n), UpdateCommitIndexReason::LeaderMatchIndexIncreased);
                return;

                // note: this will indirectly trigger `on_commit` (by commit channel)
                // It would've been slightly more efficient to call 'on_commit' here, but for
                // the sake of uniformity, only the commit channel will trigger this.
            }
        }
        debug!("Determined that current commit index {:?} is maximal", node.commit_index);
    }

    /// Updates
    #[instrument]
    pub fn on_receive_match_index(&mut self, peer: Id, match_index: usize)
    {
        let node = self.node.borrow();
        if node.state != ServerState::Leader {
            return;
        }

        debug!("on_receive_match_index from peer {} and match_index {}", peer, match_index);
        {
            let entry = self.match_indices.get_mut(&peer).unwrap();
            assert!(Some(match_index) >= *entry, "match index for a peer cannot decrease");
            if Some(match_index) == *entry {
                debug!("match index did not increase");
                return;
            }
            *entry = Some(match_index);
        }

        assert!(node.storage.len() > 0, "if match_index increased, log is definitely not empty");

        drop(node);
        self.find_higher_commit_index();

    }

    /// Executed whenever the state machine applies a committed entry, notifies
    /// the client which is responsible for that commit
    #[instrument]
    fn on_commit_apply(&mut self, commit_entry: &CommitEntry<V>, result: V::Result)
    {

        let node = self.node.borrow();

        assert!(node.commit_index.unwrap() >= commit_entry.index,
                "commit_entry and commit_index aren't consistent");


        if let Some(mut req) = self.pending_writes.remove(&commit_entry.index) {
            let responder = req.responder.take().expect("Couldn'PendingWriteRequest was already dropped, impossible");
            responder.send(Ok(ClientWriteResponse::Ok { commit_index: commit_entry.index, sm_output: result.clone() })).unwrap_or_else(|e| {
                error!("Couldn't send client write response, client probably dropped his request: {:?}", e);
            });
        }
        return;
    }

    async fn run_loop_inner(&mut self, receiver: &mut mpsc::UnboundedReceiver<NodeCommand<V>>) -> Result<(), anyhow::Error> {
        let mut node = self.node.borrow_mut();
        node.leader_id = Some(node.id);
        node.voted_for = None;
        let leader_id = node.id;
        let leader_term = node.current_term;

        let mut sm_result_recv = node.sm_result_sender.subscribe();

        drop(node);
        let node = self.node.borrow();
        info!("became leader for term {}", node.current_term);

        let (replicate_trigger, heartbeat_receiver) = watch::channel(());
        let (match_index_sender, mut match_index_receiver) = mpsc::channel(node.number_of_nodes);
        let (stale_sender, mut stale_receiver) = mpsc::channel(node.number_of_nodes);
        let all_other_nodes = node.all_other_nodes().collect::<Vec<_>>();
        drop(node);


        let replication_streams = all_other_nodes
            .into_iter()
            .map(|id| PeerReplicationStream::new(self.node.clone(), id,
                                                 heartbeat_receiver.clone(),
                                                 match_index_sender.clone()))
            .collect::<Vec<_>>();

        let ls = task::LocalSet::new();

        for stream in replication_streams.iter() {
            stream.spawn_heartbeat_loop(stale_sender.clone());
        }

        // A replication loop will only be resolved if we detect we are stale, regardless,
        // We don't care about it much as stale notification leaders are sent via channel

        let replication_fut = futures::future::join_all(replication_streams
            .into_iter()
            .map(|stream| {
                let id = stream.id;
                stream
                    .run_replication_loop(stale_sender.clone())
                    .instrument(
                        info_span!(
                            "replication-stream",
                            from = ?leader_id,
                            to = ?id,
                            term = ?leader_term
                        )
                    )
            })
        );


        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);

        ls.run_until(async move {
            task::spawn_local(replication_fut);

        loop {

            if self.node.borrow().state != ServerState::Leader {

                return Ok(());
            }

            // Upon election, leader sends heartbeats
            // Note that since a tokio interval's first tick occurs immediately,
            // we don't need to worry about it.
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                        replicate_trigger.send(()).unwrap_or_else(|e| {
                            error!("Couldn't send heartbeat, no peer replication streams: {:?}", e);
                        });
                },
                _ = self.replicate_receiver.changed() => {
                    replicate_trigger.send(()).unwrap_or_else(|e| {
                        error!("Couldn't trigger replication, no peer replication streams: {:?}", e);
                    });
                },
                res = match_index_receiver.recv() => {
                    match res {
                        Some((peer, match_index)) => self.on_receive_match_index(peer, match_index),
                        None => error!("Match index channel - sender part has dropped")
                    }
                },
                res = sm_result_recv.recv() => {
                    match res {
                        Ok(res) => self.on_commit_apply(&res.0, res.1),
                        Err(broadcast::error::RecvError::Lagged(s)) => error!("Leader lagged for {} commits, how is this possible", s) ,
                        _ => {}
                    }
                },
                Some(StaleLeader { newer_term}) = stale_receiver.recv() => {
                    let mut node = self.node.borrow_mut();
                    let _res = node.try_update_term(newer_term, None);
                    assert!(_res);
                    return Ok(())
                },
                res = receiver.recv() => {
                    // TODO can this channel close prematurely?
                    let cmd = res.unwrap();
                    match cmd {
                        NodeCommand::ClientWriteRequest(req, tx) => self.handle_client_write_command(req, tx),
                        _ => self.handle_command(cmd)

                    }
                }

            }
        }
        }).await

    }

    #[instrument]
    pub fn handle_client_write_command(&mut self, req: ClientWriteRequest<V>,
                                       tx: oneshot::Sender<Result<ClientWriteResponse<V>,RaftError>>) {
        let mut node = self.node.borrow_mut();
        info!("Received request {:?}, ", req);

        let term = node.current_term;
        let entry_index = node.storage.push(LogEntry {
            value: req.value,
            term
        });

        let pending = PendingWriteRequest::new(entry_index, tx);
        let _prev = self.pending_writes.insert(entry_index, pending);
        assert!(_prev.is_none(), "can't insert multiple PendingWriteRequests to same log index");

        if node.number_of_nodes == 1 {
            // edge case where there's only 1 (leader) node,
            // and no replication streams
            let commit_index = node.storage.len() - 1;
            node.update_commit_index(Some(commit_index),
                                     UpdateCommitIndexReason::LeaderMatchIndexIncreased);
            
            return;
        }

        // notify all replication streams of the newly added
        self.replicate_sender.send(()).unwrap_or_else(|e| {
            error!("No replication stream to be notified of added entry: {:?}", e);
        });
    }
}

impl<'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> CommandHandler<V> for LeaderState<'a, V, T, S> {
    fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let mut node = self.node.borrow_mut();
        return node.on_receive_append_entry(req);

    }

    fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let mut node = self.node.borrow_mut();
        return node.on_receive_request_vote(&req);
    }

    fn handle_client_write_request(&mut self, _: ClientWriteRequest<V>) -> Result<ClientWriteResponse<V>,RaftError> {
        panic!("Write requests cannot be handled by this function");
    }

    fn handle_client_read_request(&mut self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        let node = self.node.borrow();
        if node.state != ServerState::Leader {
            return Ok(ClientReadResponse::NotALeader { leader_id: node.leader_id })
        }

        let commit_index = node.commit_index;
        Ok(match (req.from, req.to, node.commit_index) {
            (Some(from), None, Some(commit_index)) if from <= commit_index => {
                ClientReadResponse::Ok {
                    range: node.storage.get_from_to(from, commit_index + 1
                    ).iter().map(|e| e.value.clone()).collect() }
            },
            (Some(from), Some(to), Some(commit_index))
            if from <= commit_index && to <= commit_index + 1 && from < to => {
                ClientReadResponse::Ok { range: node.storage.get_from_to(
                    from, to
                ).iter().map(|e| e.value.clone()).collect() }
            },
            (None, _, _) => {
                ClientReadResponse::Ok { range: node.storage.get_all()
                    .iter().map(|e| e.value.clone()).collect() }
            },
            _ => ClientReadResponse::BadRange { commit_index }
        })
    }

    fn handle_force_apply(&mut self, force_apply: super::state_machine::ForceApply<V>) {
        let mut node = self.node.borrow_mut();
        node.on_receive_client_force_apply(force_apply);
    }
}
