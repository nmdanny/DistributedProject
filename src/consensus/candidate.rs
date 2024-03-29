use std::collections::HashSet;

use tokio::time::Duration;
use crate::consensus::types::*;
use crate::consensus::state_machine::{StateMachine, ForceApply};
use tokio::sync::mpsc::UnboundedReceiver;
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState, ChangeStateReason};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use crate::consensus::node_communicator::CommandHandler;
use tracing_futures::Instrument;
use async_trait::async_trait;
use anyhow::Error;
use crate::consensus::timing::generate_election_length;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ElectionState {
    pub yes_votes: HashSet<usize>,
    pub no_votes: HashSet<usize>,

    pub quorum_size: usize,
    pub term: usize,

    #[derivative(Debug="ignore")]
    pub vote_receiver: UnboundedReceiver<(RequestVoteResponse, Id)>
}

impl ElectionState {
    pub fn new(my_id: Id, quorum_size: usize, term: usize,
               vote_receiver: UnboundedReceiver<(RequestVoteResponse, Id)>) -> Self {
        let mut yes_votes = HashSet::new();
        let no_votes = HashSet::new();
        // A candidate always votes for himself
        yes_votes.insert(my_id);
        ElectionState {
            yes_votes, no_votes, quorum_size, term, vote_receiver
        }
    }

    /// Counts the given vote(if it is for the current election)
    pub fn count_vote(&mut self, vote: RequestVoteResponse, from: Id) {
        if vote.term != self.term {
            error!("Got vote for another term, my term: {}, vote term: {}", self.term, vote.term);
            return;
        }
        if vote.vote_granted {
            self.yes_votes.insert(from);
        } else {
            self.no_votes.insert(from);
        }
    }

    /// Have we won the elections
    pub fn won(&self) -> bool {
        return self.yes_votes.len() >= self.quorum_size
    }
}

/// State used by a candidate over one or more consecutive elections
#[derive(Debug)]
pub struct CandidateState<'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> {
    pub node: &'a mut Node<V, T, S>

}

impl <'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> CandidateState<'a, V, T, S> {
    /// Creates state for a candidate who has just started an election
    pub fn new(candidate: &'a mut Node<V, T, S>) -> Self {
        // a candidate always votes for itself
        CandidateState {
            node: candidate
        }
    }

    /// Starts a new election
    pub fn start_election(&mut self) -> Result<ElectionState, RaftError>{
        self.node.leader_id = None;
        self.node.current_term += 1;
        self.node.voted_for = Some(self.node.id);
        debug!("Starting elections for term {}", self.node.current_term);

        // for notifying the loop of received votes
        let (tx, rx) = mpsc::unbounded_channel();


        for node_id in self.node.all_other_nodes().collect::<Vec<_>>() {
            let req = RequestVote {
                term: self.node.current_term,
                candidate_id: self.node.id,
                last_log_index_term: self.node.storage.last_log_index_term()
            };
            let transport = self.node.transport.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                trace!("sending vote request to {}", node_id);
                let res = transport.send_request_vote(node_id, req).await;
                match res {
                    Ok(res) => {
                        trace!("got vote response {:?}", res);
                        // rx will be dropped if node state changes, not an error.
                        let _ = tx.send((res, node_id));
                    },
                    Err(RaftError::NetworkError(e)) | Err(RaftError::TimeoutError(e)) => {
                        trace!(net_err=true, "Network/timeout error when sending vote request to {}: {}", node_id, e);
                    },
                    Err(e) => {
                        error!("Misc Raft error when sending vote request: {}", e);
                    }
                }
            }.instrument(trace_span!("vote request", to=node_id)));
        }
        Ok(ElectionState::new(self.node.id, self.node.quorum_size(), self.node.current_term, rx))
    }

    #[instrument(skip(self))]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {

        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {
            let mut elections_in_a_row = 0;
        // loop for multiple consecutive elections(1 or more)
        loop {


            if self.node.state != ServerState::Candidate {
                return Ok(());
            }

            elections_in_a_row += 1;
            // start an election, updating node state and sending vote requests
            let mut election_state = self.start_election()?;

            let duration = generate_election_length(&self.node.settings);
            let election_end = tokio::time::Instant::now() + duration;
            info!("elections started for term {} ({} elections in a row), duration: {:?}", self.node.current_term, elections_in_a_row, duration);

            // loop for a single election
            loop {

                if self.node.state != ServerState::Candidate {
                    return Ok(());
                }

                let election_end_fut = tokio::time::sleep_until(election_end);
                tokio::select! {
                    _ = election_end_fut => {
                        // election timed out, start another one
                        info!("elections of term {} timed out(got: {} yes, {} no), so far ran {} elections in a row",
                               self.node.current_term,
                               election_state.yes_votes.len(), election_state.no_votes.len(), elections_in_a_row);
                        break;
                    },
                    Some((vote, from)) = election_state.vote_receiver.recv() => {
                        // If we get vote from a node at a later term, we'll convert
                        // to follower. (§5.1)
                        if self.node.try_update_term(vote.term, None) {
                            assert!(!vote.vote_granted);
                            return Ok(());
                        }
                        election_state.count_vote(vote, from);
                        if election_state.won() {
                            info!("won election after {} elections in a row, results: {:?}", 
                                elections_in_a_row, election_state);
                            self.node.change_state(ServerState::Leader,
                                ChangeStateReason::WonElection {
                                    yes: election_state.yes_votes.len(), no: election_state.no_votes.len()
                                });
                            return Ok(())
                        }
                    },
                    res = self.node.receiver.as_mut().expect("candidate - Node::receiver was None").recv() => {
                        // TODO can this channel close prematurely?
                        let cmd = res.unwrap();
                        self.handle_command(cmd);
                    }
                }
            }
        }}).await
    }

}


impl <'a, V: Value, T: Transport<V>, S: StateMachine<V, T>> CommandHandler<V> for CandidateState<'a, V, T, S> {
    fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        return self.node.on_receive_append_entry(req);
    }

    fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let my_term = self.node.current_term;
        let res = self.node.on_receive_request_vote(&req);
        if let Ok(res) = &res {
            assert!(!(res.vote_granted && req.term == my_term), "A candidate will never grant votes to other candidates from the same term as his");
        }
        return res;
    }

    fn handle_client_write_request(&mut self, _req: ClientWriteRequest<V>) -> Result<ClientWriteResponse<V>, RaftError> {
        Ok(ClientWriteResponse::NotALeader { leader_id: self.node.leader_id })
    }

    fn handle_client_read_request(&mut self, _req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        Ok(ClientReadResponse::NotALeader { leader_id: self.node.leader_id })
    }

    fn handle_force_apply(&mut self, force_apply: ForceApply<V>) {
        self.node.on_receive_client_force_apply(force_apply);
    }
}
