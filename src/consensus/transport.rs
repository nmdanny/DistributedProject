use crate::consensus::types::*;
use async_trait::async_trait;

/// Used for sending and receiving Raft messages
#[async_trait]
pub trait Transport<V> {
    async fn send_append_entries(&mut self, to: Id, msg: AppendEntries<V>) -> AppendEntriesResponse;

    async fn send_request_vote(&mut self, to: Id, msg: RequestVote) -> RequestVoteResponse;
}