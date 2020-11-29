use crate::consensus::types::*;
use async_trait::async_trait;
use tokio::task;

/// Used for sending and receiving Raft messages
/// Should be cheap to clone
#[async_trait]
pub trait Transport<V : Value> : Clone + Send + 'static {
    async fn send_append_entries(&mut self, to: Id, msg: AppendEntries<V>) -> AppendEntriesResponse;

    async fn send_request_vote(&mut self, to: Id, msg: RequestVote) -> RequestVoteResponse;
}


/// Used for debugging purposes, literally does nothing and will never resolve
#[derive(Debug, Clone)]
pub struct NoopTransport();

#[async_trait]
impl <V : Value> Transport<V> for NoopTransport {
    async fn send_append_entries(&mut self, _: usize, _: AppendEntries<V>) -> AppendEntriesResponse {
        loop {
            task::yield_now().await;
        }
    }

    async fn send_request_vote(&mut self, _: usize, _: RequestVote) -> RequestVoteResponse {
        loop {
            task::yield_now().await;
        }
    }
}
