use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState};
use crate::consensus::node_communicator::{CommandHandler, NodeCommand};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::stream::StreamExt;
use futures::TryFutureExt;
use tracing_futures::Instrument;
use std::collections::BTreeMap;
use async_trait::async_trait;
use anyhow::Context;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use tokio::sync::watch::Ref;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Debug)]
/// Contains state used to replicate the leader's data to a peer.
/// Note, this is re-initialized every time a node becomes a leader
pub struct PeerReplicationStream<V: Value> {
    /// Peer ID
    pub id: Id,

    /// The index of the new log entry(in the leader's log) to send to the peer
    pub next_index: usize,

    /// The maximal index of a leader log entry known to be replicated on the peer
    pub match_index: Option<usize>,

    // TODO: this is redundant, why do we even have 'run_replication_loop' as a loop?
    /// Used to receive heartbeats
    pub tick_receiver: watch::Receiver<()>,

    phantom: std::marker::PhantomData<V>

}

#[derive(Debug, Clone)]
pub struct StaleLeader { newer_term: usize }

impl <V: Value> PeerReplicationStream<V> {
    pub fn new<T: Transport<V>>(node: &Node<V, T>, id: Id,
                                tick_receiver: watch::Receiver<()>) -> Self {


        // at first, assume the peer is completely synchronized
        let next_index = node.storage.len();
        let match_index = None;
        PeerReplicationStream {
            id, next_index, match_index, tick_receiver, phantom: Default::default()
        }
    }
}

#[derive(Debug)]
pub struct LeaderState<'a, V: Value, T: Transport<V>>{

    pub node: Rc<RefCell<Node<V, T>>>,

    /// Used to notify replication streams of a heartbeat or a
    /// newly inserted entry that came from a client
    pub replicate_sender: watch::Sender<()>,

    /// To be passed to all peer replication streams
    pub replicate_receiver: watch::Receiver<()>,

    phantom: std::marker::PhantomData<&'a ()>

}


impl<'a, V: Value, T: Transport<V>> LeaderState<'a, V, T> {
    /// Creates state for a node who just became a leader
    pub fn new(node: Rc<RefCell<Node<V, T>>>) -> Self {


        let (replicate_sender, replicate_receiver) = watch::channel(());
        LeaderState {
            node, replicate_sender, replicate_receiver, phantom: Default::default()
        }
    }

    /// Creates an AppendEntries request containing nothing.
    fn create_append_entries_for_heartbeat(&self) -> AppendEntries<V> {
        let node = self.node.borrow();
        AppendEntries {
            leader_id: node.id,
            term: node.current_term,
            entries: Vec::new(),
            leader_commit: node.commit_index,
            prev_log_index_term: node.storage.last_log_index_term()
        }

    }

    /// Creates an AppendEntries request containing `log[index ..]`
    /// Uses `index-1` as the previous log index(or an empty IndexTerm if `index` == 0)
    fn create_append_entries_for_index(&self, index: usize) -> AppendEntries<V> {
        let node = self.node.borrow();
        assert!(index < node.storage.len(), "invalid index");
        let leader_id = node.id;
        let term = node.current_term;
        let entries = node.storage.get_from(index).iter().cloned().collect();
        let leader_commit = node.commit_index;

        let prev_log_index_term = if index > 0 {
            let entry = node.storage.get(index).unwrap();
            IndexTerm::new(index, entry.term)
        } else { IndexTerm::no_entry() };

        AppendEntries {
            leader_id, term, entries, leader_commit, prev_log_index_term
        }
    }


    #[instrument]
    pub async fn replication_stream(&mut self, mut stream: PeerReplicationStream<V>) -> Result<(), StaleLeader> {
        while let Some(()) = stream.tick_receiver.next().await {

        }
        Ok(())
    }

    /// Tries replicating a message to all clients
    #[instrument]
    pub async fn replicate(&mut self, streams: &mut [PeerReplicationStream<V>]) -> Result<(), StaleLeader> {
        Ok(())
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        let mut receiver = self.node
            .borrow_mut().receiver
            .take().expect("Receiver was null");

        let res = self.run_loop_inner(&mut receiver).await;

        // since only LeaderState can take out `receiver`, we ensure the receiver is back in the
        // state no matter if run_loop_inner has exited in an error or not.

        self.node.borrow_mut().receiver = Some(receiver);
        res

    }

    async fn run_loop_inner(&mut self, receiver: &mut mpsc::UnboundedReceiver<NodeCommand<V>>) -> Result<(), anyhow::Error> {
        let mut node_ref = self.node.borrow_mut();
        node_ref.leader_id = Some(node_ref.id);
        node_ref.voted_for = None;

        info!("became leader for term {}", node_ref.current_term);
        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);

        let (heartbeat_sender, heartbeat_receiver) = watch::channel(());

        let (stale_notifier, mut stale_receiver) = mpsc::channel(node_ref.number_of_nodes);


        let mut replication_streams = node_ref
            .all_other_nodes()
            .into_iter()
            .map(|id| PeerReplicationStream::new(&node_ref, id,
                                                 heartbeat_receiver.clone()))
            .collect::<Vec<_>>();

        drop(node_ref);

        loop {
            if self.node.borrow().state != ServerState::Leader {
                return Ok(());
            }
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                        heartbeat_sender.broadcast(()).unwrap_or_else(|e| {
                            error!("Couldn't send heartbeat, no peer replication streams");
                        });
                },
                res = self.replicate_receiver.next() => {
                    let todo = self.replicate(&mut replication_streams).await;
                },
                res = stale_receiver.next() => {
                    let res = res.unwrap();
                    let mut node = self.node.borrow_mut();
                    info!(node.current_term, new_term=res, "Received out of date term in reply");
                    node.try_update_term(res, None);
                    return Ok(())
                },
                res = receiver.next() => {
                    // TODO can this channel close prematurely?
                    let cmd = res.unwrap();
                    self.handle_command(cmd).await;
                }

            }
        }

    }
}

#[async_trait(?Send)]
impl<'a, V: Value, T: Transport<V>> CommandHandler<V> for LeaderState<'a, V, T> {
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let mut node = self.node.borrow_mut();
        return node.on_receive_append_entry(req);

    }

    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let mut node = self.node.borrow_mut();
        return node.on_receive_request_vote(&req);
    }

    async fn handle_client_write_request(&mut self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse, RaftError> {
        let mut node = self.node.borrow_mut();
        if node.state != ServerState::Leader {
            return Ok(ClientWriteResponse::NotALeader { leader_id: node.leader_id })
        }
        let (res_send, res_receive) = oneshot::channel();

        let term = node.current_term;
        node.storage.push(LogEntry {
            value: req.value,
            term
        });

        // notify all replication streams of the newly added
        self.replicate_sender.broadcast(()).unwrap_or_else(|e| {
            error!("No replication stream to be notified of added entry: {:?}", e);
        });
        drop(node);

        // res_send can be closed if we become a follower and the PeerReplicationStream is dropped
        res_receive.await.map_err(|e| RaftError::InternalError((e.into())))
    }

    async fn handle_client_read_request(&mut self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        let node = self.node.borrow();
        if node.state != ServerState::Leader {
            return Ok(ClientReadResponse::NotALeader { leader_id: node.leader_id })
        }

        let commit_index = node.commit_index;
        Ok(match (req.from, req.to, node.commit_index) {
            (from, None, Some(commit_index)) if from <= commit_index => {
                ClientReadResponse::Ok {
                    range: node.storage.get_from_to(from, commit_index + 1
                    ).iter().map(|e| e.value.clone()).collect() }
            },
            (from, Some(to), Some(commit_index))
            if from <= commit_index && to <= commit_index + 1 && from < to => {
                ClientReadResponse::Ok { range: node.storage.get_from_to(
                    from, to
                ).iter().map(|e| e.value.clone()).collect() }
            },
            _ => ClientReadResponse::BadRange { commit_index }
        })
    }
}
