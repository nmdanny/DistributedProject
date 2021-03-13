use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState, ChangeStateReason};
use crate::consensus::state_machine::StateMachine;
use crate::consensus::node_communicator::CommandHandler;
use std::{pin::Pin, time::Instant};
use core::result::Result;
use core::option::Option::{None, Some};
use core::result::Result::Ok;
use async_trait::async_trait;
use tokio_stream::StreamExt;
use tokio::{sync::watch, time::Sleep};
use crate::consensus::timing::generate_election_length;

/// State used by a follower
#[derive(Derivative)]
#[derivative(Debug)]
pub struct FollowerState<'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> {
    #[derivative(Debug="ignore")]
    pub node: &'a mut Node<V, T, S>,

    #[derivative(Debug="ignore")]
    sleep: Pin<Box<Sleep>>,

    timeout_duration: std::time::Duration

}

impl <'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> FollowerState<'a, V, T, S> {
    /// Creates state used for a node who has just become a follower
    pub fn new(node: &'a mut Node<V, T, S>) -> Self {
        let timeout_duration = generate_election_length(&node.settings);
        let sleep = tokio::time::sleep(timeout_duration);
        FollowerState {
            node,
            sleep: Box::pin(sleep),
            timeout_duration
        }
    }

    fn update_timer(&mut self) {
        let new_deadline = tokio::time::Instant::from_std(std::time::Instant::now() + self.timeout_duration);
        self.sleep.as_mut().reset(new_deadline);
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        self.node.voted_for = self.node.leader_id;

        info!("Became follower, timeout duration(no heartbeats/vote) is {:?}", self.timeout_duration);

        loop {
            // One of the later operations might have changed our state
            if self.node.state != ServerState::Follower {
                return Ok(())
            }

            tokio::select! {
                _ = self.sleep.as_mut() => {
                    warn!("haven't received a heartbeat/voted too long from leader {:?}, becoming candidate for term {}", self.node.leader_id, self.node.current_term + 1);
                    self.node.change_state(ServerState::Candidate, ChangeStateReason::FollowerTimeout { timeout_duration: self.timeout_duration });
                },
                Some(cmd) = self.node.receiver.as_mut().expect("follower - Node::receiver was None").recv() => {
                    self.handle_command(cmd);
                }
            }
        }
    }
}

impl <'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> CommandHandler<V> for FollowerState<'a, V, T, S> {
    #[instrument]
    fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let mut reset_timer = true;

        // if sender is stale, we will not reset the timer
        if req.term < self.node.current_term {
            reset_timer = false;
        }

        // continue with the default handling of append entry
        let res = self.node.on_receive_append_entry(req);

        if reset_timer {
            // update heartbeat to prevent switch to candidate
            // only if the sender of this AE wasn't stale
            self.update_timer();
        }

        res
    }

    fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let mut reset_timer = false;

        // Raft spec dictates that upon seeing an RPC with a higher term, we should "become a
        // follower". Since we're already a follower there won't really be a state change in our implementation,
        // so we'll just update the timer. 
        reset_timer |= req.term > self.node.current_term;

        // use default handling of request vote
        let res = self.node.on_receive_request_vote(&req);


        reset_timer |= res.as_ref().map(|res| res.vote_granted).unwrap_or(false);

        if reset_timer {
            self.update_timer()
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
