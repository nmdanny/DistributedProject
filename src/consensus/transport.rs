use crate::consensus::types::*;
use async_trait::async_trait;
use tokio::task;
use anyhow::Result;
use std::fmt::Debug;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;

/// Used for sending and receiving Raft messages
/// Should be cheap to clone
#[async_trait(?Send)]
pub trait Transport<V : Value> : Debug + Clone + Send + Sync + 'static {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse>;

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse>;

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
    async fn send_append_entries(&self, _: usize, _: AppendEntries<V>) -> Result<AppendEntriesResponse> {
        loop {
            task::yield_now().await;
        }
    }

    async fn send_request_vote(&self, _: usize, _: RequestVote) -> Result<RequestVoteResponse> {
        loop {
            task::yield_now().await;
        }
    }
}
