use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::{Node, ServerState};
use crate::consensus::node_communicator::CommandHandler;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::stream::StreamExt;
use futures::TryFutureExt;
use tracing_futures::Instrument;
use std::collections::BTreeMap;
use async_trait::async_trait;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Debug)]
pub struct LeaderState<'a, V: Value, T: Transport<V>>{

    pub node: &'a mut Node<V, T>,

    /// For each node, index of the next log entry(in the leader's log) to send to him
    /// Usually next_index[peer] is match_index[peer] + 1, however, match_index[peer] is usually
    /// 0 when a leader is initiated(or might lag behind in other cases where `peer` was partitioned
    /// for a long while),
    pub next_index: BTreeMap<Id, usize>,

    /// For each node, index of the highest log entry known to be replicated on him(the node's
    /// commit index)
    ///
    /// Assuming 1 based indexing, we only need to send log[match_index[peer] + 1, ...] to each peer
    /// after gaining leadership
    pub match_index: BTreeMap<Id, usize>
}

impl<'a, V: Value, T: Transport<V>> LeaderState<'a, V, T> {
    /// Creates state for a node who just became a leader
    pub fn new(node: &'a mut Node<V, T>) -> Self {
        LeaderState {
            node,
            next_index: Default::default(),
            match_index: Default::default()
        }
    }

    /// Sends a heartbeat to all nodes. If it detects we are stale(encounters a higher term),
    /// sends the newer term via given sender.
    pub async fn send_heartbeat(&self, stale_notifier: mpsc::Sender<usize>) {
        let msg = AppendEntries {
            leader_id: self.node.id,
            term: self.node.current_term,
            entries: Vec::new(),
            leader_commit: self.node.commit_index,
            prev_log_index_term: self.node.storage.last_log_index_term()
        };
        info!("sending heartbeat");
        let my_term = self.node.current_term;
        for node_id in self.node.all_other_nodes() {
            let transport = self.node.transport.clone();
            let msg = msg.clone();
            let mut tx = stale_notifier.clone();
            tokio::spawn(async move {
                let res = transport.send_append_entries(node_id, msg).await;
                if let Ok(res) = res {
                    if res.term > my_term {
                        tx.send(res.term).unwrap_or_else(|_| {
                            // TODO not really an error
                            error!("Couldn't notify leader that he's stale(someone else probably notified him already");
                        }).await;
                    }
                } else {
                    error!("sending heartbeat to {} failed: {:?}", node_id, res);
                }
            }).instrument(info_span!("heartbeat"));
        }
    }

    #[instrument]
    pub async fn run_loop(mut self) -> Result<(), anyhow::Error> {
        self.node.leader_id = Some(self.node.id);
        self.node.voted_for = None;

        info!("became leader for term {}", self.node.current_term);
        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        let (stale_notifier, mut stale_receiver) = mpsc::channel(self.node.number_of_nodes);

        loop {
            if self.node.state != ServerState::Leader {
                return Ok(());
            }

            tokio::select! {
                _ = heartbeat_interval.tick() => {
                        self.send_heartbeat(stale_notifier.clone()).await;
                },
                res = stale_receiver.next() => {
                    let res = res.unwrap();
                    info!(self.node.current_term, new_term=res, "Received out of date term in reply");
                    self.node.try_update_term(res, None);
                    return Ok(())
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

#[async_trait]
impl<'a, V: Value, T: Transport<V>> CommandHandler<V> for LeaderState<'a, V, T> {
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        return self.node.on_receive_append_entry(req);

    }

    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        return self.node.on_receive_request_vote(&req);
    }

    async fn handle_client_write_request(&mut self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse, RaftError> {
        unimplemented!()
    }

    async fn handle_client_read_request(&mut self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        unimplemented!()
    }
}
