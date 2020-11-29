use crate::consensus::types::*;
use async_trait::async_trait;
use tokio::task;
use anyhow::Result;

/// Used for sending and receiving Raft messages
/// Should be cheap to clone
#[async_trait]
pub trait Transport<V : Value> : Clone + Send + Sync + 'static {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse>;

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse>;
}


/// Used for debugging purposes, literally does nothing and will never resolve
#[derive(Debug, Clone)]
pub struct NoopTransport();

#[async_trait]
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
