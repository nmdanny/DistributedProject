use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState};
use crate::consensus::state_machine::StateMachine;
use crate::consensus::node_communicator::CommandHandler;
use std::time::Instant;
use core::result::Result;
use core::option::Option::{None, Some};
use core::result::Result::Ok;
use async_trait::async_trait;
use tokio_stream::StreamExt;
use tokio::sync::watch;
use crate::consensus::timing::generate_election_length;

/// State used by a follower
#[derive(Debug)]
pub struct FollowerState<'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> {
    pub node: &'a mut Node<V, T, S>,

    /// Used to notify main loop that that a heartbeat was received/vote granted
    pub heartbeat_or_grant_vote_watch: Option<watch::Sender<()>>,

}

impl <'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> FollowerState<'a, V, T, S> {
    /// Creates state used for a node who has just become a follower
    pub fn new(node: &'a mut Node<V, T, S>) -> Self {
        FollowerState {
            node,
            heartbeat_or_grant_vote_watch: None
        }
    }

    fn update_timer(&mut self) {
        self.heartbeat_or_grant_vote_watch.as_ref().unwrap().send(()).unwrap();
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        self.node.voted_for = None;

        // setup timer
        let (tx, mut rx) = watch::channel(());
        self.heartbeat_or_grant_vote_watch = Some(tx);
        let timeout_duration = generate_election_length();
        let delay_fut = tokio::time::sleep(timeout_duration);
        tokio::pin!(delay_fut);
        info!("Became follower, timeout duration(no heartbeats/vote) is {:?}", timeout_duration);

        loop {
            // One of the later operations might have changed our state
            if self.node.state != ServerState::Follower {
                return Ok(())
            }

            tokio::select! {
                _ = &mut delay_fut => {
                    warn!("haven't received a heartbeat/voted too long, becoming candidate");
                    self.node.change_state(ServerState::Candidate);
                },
                res = self.node.receiver.as_mut().expect("follower - Node::receiver was None").recv() => {
                    let cmd = res.unwrap();
                    self.handle_command(cmd);
                },
                Ok(()) = rx.changed() => {
                    delay_fut.as_mut().reset(tokio::time::Instant::from_std(Instant::now() + timeout_duration));
                }
            }
        }
    }
}

impl <'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> CommandHandler<V> for FollowerState<'a, V, T, S> {
    #[instrument]
    fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        if req.term < self.node.current_term {
            return Ok(AppendEntriesResponse::failed(self.node.current_term));
        }

        // if we had just lost an election
        if self.node.leader_id.is_none() {
            info!("Now I know the new leader: {}", req.leader_id);
            self.node.leader_id = Some(req.leader_id);
        }

        if self.node.leader_id != Some(req.leader_id) {
            // It is impossible to have two different leaders at the same term,
            // Because that would imply each of them got a majority of votes for said term,
            // But a node cannot vote for two different leaders at the same term
            assert_ne!(req.term, self.node.current_term,
                       "If there's a mismatch between leaders, the term must have changed(increased)");
        }
        self.node.leader_id = Some(req.leader_id);

        // update heartbeat to prevent switch to candidate
        self.update_timer();

        // continue with the default handling of append entry
        return self.node.on_receive_append_entry(req);
    }

    fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        // use default handling of request vote
        let res = self.node.on_receive_request_vote(&req);

        match res.as_ref() {
            Ok(res) if res.vote_granted => {
                // if we granted a vote, delay switch to candidate
                self.update_timer();
            }
            _ => {}
        }
        return res;
    }

    fn handle_client_write_request(&mut self, _req: ClientWriteRequest<V>) -> Result<ClientWriteResponse<V>,RaftError> {
        Ok(ClientWriteResponse::NotALeader { leader_id: self.node.leader_id })
    }

    fn handle_client_read_request(&mut self, _req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        Ok(ClientReadResponse::NotALeader { leader_id: self.node.leader_id })
    }

    fn handle_force_apply(&mut self, force_apply: super::state_machine::ForceApply<V>) {
        self.node.on_receive_client_force_apply(force_apply);
    }
}
