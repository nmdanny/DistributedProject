use crate::consensus::types::{Value, AppendEntries, AppendEntriesResponse, RaftError, RequestVote, RequestVoteResponse, ClientWriteRequest, ClientWriteResponse, ClientReadRequest, ClientReadResponse};
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState};
use crate::consensus::node_communicator::CommandHandler;
use std::time::Instant;
use core::result::Result;
use core::option::Option::{None, Some};
use core::result::Result::Ok;
use async_trait::async_trait;
use tokio::stream::StreamExt;
use crate::consensus::candidate::generate_election_length;

/// State used by a follower
#[derive(Debug)]
pub struct FollowerState<'a, V: Value, T: Transport<V>> {
    pub node: &'a mut Node<V, T>,

    /// The last time since we granted a vote or got a heartbeat from the leader
    /// If more time passed than the election timeout, convert to a candidate. (§5.2)
    pub time_since_last_heartbeat_or_grant_vote: Instant
}

impl <'a, V: Value, T: Transport<V>> FollowerState<'a, V, T> {
    /// Creates state used for a node who has just become a follower
    pub fn new(node: &'a mut Node<V, T>) -> Self {
        FollowerState {
            node,
            time_since_last_heartbeat_or_grant_vote: Instant::now()
        }
    }

    /// Performs follower specific changes when receiving an AE request
    /// (notably, updates the follower state)
    pub fn on_receive_append_entries(&mut self, req: &AppendEntries<V>) {
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        self.node.voted_for = None;


        let timeout_duration = generate_election_length();
        info!("Became follower, timeout duration(no heartbeats/vote) is {:?}", timeout_duration);

        loop {
            // One of the later operations might have changed our state
            if self.node.state != ServerState::Follower {
                return Ok(())
            }

            // create a single timer for a heartbeat
            let deadline = self.time_since_last_heartbeat_or_grant_vote + timeout_duration;
            let delay = tokio::time::delay_until(tokio::time::Instant::from_std(deadline));
            tokio::select! {
                _ = delay => {
                        warn!("haven't received a heartbeat in too long, becoming candidate");
                        self.node.change_state(ServerState::Candidate);
                },
                res = self.node.receiver.next() => {
                    // TODO can this channel close prematurely?
                    let cmd = res.unwrap();
                    self.handle_command(cmd).await;
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl <'a, V: Value, T: Transport<V>> CommandHandler<V> for FollowerState<'a, V, T> {
    #[instrument]
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        if req.term < self.node.current_term {
            return Ok(AppendEntriesResponse::failed(self.node.current_term));
        }

        // if we had just lost an election
        if self.node.leader_id.is_none() {
            info!("Now I know the new leader: {}", req.leader_id);
            self.node.leader_id = Some(req.leader_id);
        }

        if self.node.leader_id != Some(req.leader_id) {
            assert!(req.term > self.node.current_term,
                "If there's a mismatch between leaders, the term must have changed(increased)");
        }
        self.node.leader_id = Some(req.leader_id);

        // update heartbeat to prevent switch to candidate
        self.time_since_last_heartbeat_or_grant_vote = Instant::now();

        // continue with the default handling of append entry
        return self.node.on_receive_append_entry(req);
    }

    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        // use default handling of request vote
        let res = self.node.on_receive_request_vote(&req);

        match res.as_ref() {
            Ok(res) if res.vote_granted => {
                // if we granted a vote, delay switch to candidate
                self.time_since_last_heartbeat_or_grant_vote = Instant::now();
            }
            _ => {}
        }
        return res;
    }

    async fn handle_client_write_request(&mut self, _req: ClientWriteRequest<V>) -> Result<ClientWriteResponse, RaftError> {
        Ok(ClientWriteResponse::NotALeader { leader_id: self.node.leader_id })
    }

    async fn handle_client_read_request(&mut self, _req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        Ok(ClientReadResponse::NotALeader { leader_id: self.node.leader_id })
    }
}
