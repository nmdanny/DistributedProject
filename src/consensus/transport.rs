use crate::consensus::types::*;
use async_trait::async_trait;
use tokio::task;
use std::fmt::Debug;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use derivative;
use std::collections::HashMap;
use tokio::sync::{RwLock, Barrier, mpsc};
use std::sync::Arc;
use tokio::stream::StreamExt;

/// Used for sending and receiving Raft messages
/// Should be cheap to clone
#[async_trait(?Send)]
pub trait Transport<V : Value> : Debug + Clone + Send + Sync + 'static {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError>;

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse, RaftError>;

    /// A hook that runs after a `NodeCommunicator` is created(along with a node)
    /// but before the node is spawned.
    async fn on_node_communicator_created(&mut self, _id: Id, _comm: &mut NodeCommunicator<V>) {

    }

    /// A hook that runs right before a node starts running. By this point we should've
    /// configured all peers in the transport.
    async fn before_node_loop(&mut self, _id: Id) {

    }
}


/// Used for debugging purposes, literally does nothing and will never resolve
#[derive(Debug, Clone)]
pub struct NoopTransport();

#[async_trait(?Send)]
impl <V : Value> Transport<V> for NoopTransport {
    async fn send_append_entries(&self, _: usize, _: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        loop {
            task::yield_now().await;
        }
    }

    async fn send_request_vote(&self, _: usize, _: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        loop {
            task::yield_now().await;
        }
    }
}

struct ThreadTransportState<V: Value>
{
    senders: HashMap<Id, NodeCommunicator<V>>,

}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct ThreadTransport<V: Value>
{
    #[derivative(Debug="ignore")]
    state: Arc<RwLock<ThreadTransportState<V>>>,

    #[derivative(Debug="ignore")]
    barrier: Arc<Barrier>
}

impl <V: Value> ThreadTransport<V> {
    pub fn new(expected_num_nodes: usize) -> Self {
        let barrier = Arc::new(Barrier::new(expected_num_nodes));
        let state = ThreadTransportState {
            senders: Default::default()
        };
        ThreadTransport {
            state: Arc::new(RwLock::new(state)),
            barrier
        }
    }
}

#[async_trait(?Send)]
impl <V: Value> Transport<V> for ThreadTransport<V> {
    #[instrument]
    async fn send_append_entries(&self, to: usize, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let comm = {
            self.state.read().await.senders.get(&to).unwrap().clone()
        };
        Ok(comm.append_entries(msg).await?)
    }

    #[instrument]
    async fn send_request_vote(&self, to: usize, msg: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let comm = {
            self.state.read().await.senders.get(&to).unwrap().clone()
        };
        Ok(comm.request_vote(msg).await?)
    }

    async fn on_node_communicator_created(&mut self, id: Id, comm: &mut NodeCommunicator<V>) {
        let mut state = self.state.write().await;

        let _prev = state.senders.insert(id, comm.clone());
        assert!(_prev.is_none(), "Can't insert same node twice");
    }

    async fn before_node_loop(&mut self, _id: Id) {
        self.barrier.wait().await;
    }
}