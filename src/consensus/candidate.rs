use tokio::time::Duration;
use crate::consensus::types::*;
use crate::consensus::state_machine::StateMachine;
use tokio::sync::mpsc::UnboundedReceiver;
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState};
use tokio::sync::mpsc;
use tokio::stream::StreamExt;
use crate::consensus::node_communicator::CommandHandler;
use tracing_futures::Instrument;
use async_trait::async_trait;
use anyhow::Error;
use crate::consensus::timing::generate_election_length;

#[derive(Debug, Eq, PartialEq)]
pub enum ElectionResult {
    Lost,
    Won,
    Undecided
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ElectionState {
    /// NOTE: it is assumed that Raft RPC messages cannot be duplicated, in other words,
    /// since each node only votes once, we can not have more than one response for every nodee
    /// (We can assume reliability by our network protocol, e.g, unique message ID)
    pub votes: Vec<RequestVoteResponse>,

    pub yes: usize,
    pub no: usize,

    pub quorum_size: usize,
    pub term: usize,

    #[derivative(Debug="ignore")]
    pub vote_receiver: UnboundedReceiver<RequestVoteResponse>
}

impl ElectionState {
    pub fn new(quorum_size: usize, term: usize,
               vote_receiver: UnboundedReceiver<RequestVoteResponse>) -> Self {
        // we always vote for ourself
        let votes = vec![
            RequestVoteResponse {
                term, vote_granted: true
            }
        ];
        ElectionState {
            votes, yes: 1, no: 0, quorum_size, term, vote_receiver
        }
    }

    /// Counts the given vote(if it is for the current election)
    pub fn count_vote(&mut self, vote: RequestVoteResponse) {
        if vote.term != self.term {
            error!("Got vote for another term, my term: {}, vote term: {}", self.term, vote.term);
            return;
        }
        if vote.vote_granted {
            self.yes += 1;
        } else {
            self.no += 1;
        }
        self.votes.push(vote);
    }

    /// Tallies all current votes and returns the result
    pub fn tally(&self) -> ElectionResult {
        if self.yes >= self.quorum_size {
            return ElectionResult::Won;
        }
        if self.no >= self.quorum_size {
            return ElectionResult::Lost;
        }
        return ElectionResult::Undecided;
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
    pub async fn start_election(&mut self) -> Result<ElectionState, RaftError>{
        self.node.leader_id = None;
        self.node.current_term += 1;
        self.node.voted_for = Some(self.node.id);
        warn!("Starting elections started for term {}", self.node.current_term);

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
            tokio::task::spawn_local(async move {
                trace!("sending vote request to {}", node_id);
                let res = transport.send_request_vote(node_id, req).await;
                match &res {
                    Ok(res) => {
                        trace!("got vote response {:?}", res);
                        tx.send(res.clone()).unwrap_or_else(|e| {
                            // TODO this isn't really an error, just the result of delays
                            error!(vote_granted_too_late=true, "Received vote response {:?} too late (loop has dropped receiver, send error: {:?})", res, e);
                        });
                    },
                    Err(RaftError::NetworkError(e)) => {
                        error!(net_err=true, "Network error when sending vote request: {}", e);
                    },
                    Err(e) => {
                        error!("Misc Raft error when sending vote request: {}", e);
                    }
                }
            }.instrument(info_span!("vote request", to=node_id)));
        }
        Ok(ElectionState::new(self.node.quorum_size(), self.node.current_term, rx))
    }

    #[instrument(skip(self))]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {

        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {
            let mut elections_in_a_row = 0;
        // loop for multiple consecutive elections(1 or more)
        loop {

            elections_in_a_row += 1;
            // start an election, updating node state and sending vote requests
            let mut election_state = self.start_election().await?;
            let mut _lost = false;

            let duration = generate_election_length();
            let election_end = tokio::time::Instant::now() + duration;
            warn!("elections started({} elections in a row), duration: {:?}", elections_in_a_row, duration);

            // loop for a single election
            loop {

                if self.node.state != ServerState::Candidate {
                    return Ok(());
                }

                let election_end_fut = tokio::time::delay_until(election_end);
                tokio::select! {
                    _ = election_end_fut => {
                        // election timed out, start another one
                        warn!("elections timed out(got: {} yes, {} no), starting more elections",
                               election_state.yes, election_state.no);
                        break;
                    },
                    Some(vote) = election_state.vote_receiver.next() => {
                        // If we get vote from a node at a later term, we'll convert
                        // to follower. (§5.1)
                        if self.node.try_update_term(vote.term, None) {
                            assert!(!vote.vote_granted);
                            return Ok(());
                        }
                        election_state.count_vote(vote);
                        match election_state.tally() {
                            ElectionResult::Lost => {
                                if !_lost {
                                    info!("lost election, results: {:?}", election_state);
                                }
                                _lost = true;
                                // we will not change the state yet, this will be done once we
                                // receive a new AppendEntries message(might be slightly wasteful
                                // as we might try another election, but this is a rare scenario
                                // as the election timeout is bigger by an order of magnitude than
                                // the broadcast time.)
                            }
                            ElectionResult::Won => {
                                assert!(!_lost, "Cannot win election after losing(sanity check)");
                                info!("won election, results: {:?}", election_state);
                                self.node.change_state(ServerState::Leader);
                                return Ok(())
                            }
                            ElectionResult::Undecided => {
                                assert!(!_lost, "Cannot become undecided after losing(sanity check)");
                            }
                        }
                    },
                    res = self.node.receiver.as_mut().expect("candidate - Node::receiver was None").next() => {
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
        let res = self.node.on_receive_request_vote(&req);
        if let Ok(res) = &res {
            assert!(!res.vote_granted, "A candidate will never grant votes to other candidates");
        }
        return res;
    }

    fn handle_client_write_request(&mut self, _req: ClientWriteRequest<V>) -> Result<ClientWriteResponse<V>, RaftError> {
        Ok(ClientWriteResponse::NotALeader { leader_id: self.node.leader_id })
    }

    fn handle_client_read_request(&mut self, _req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        Ok(ClientReadResponse::NotALeader { leader_id: self.node.leader_id })
    }
}
