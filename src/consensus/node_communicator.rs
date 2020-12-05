use crate::consensus::types::{Value, AppendEntries, AppendEntriesResponse, RaftError, RequestVote, RequestVoteResponse, ClientWriteRequest, ClientWriteResponse, ClientReadRequest, ClientReadResponse, CommitEntry};
use tokio::sync::{oneshot, mpsc};
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use async_trait::async_trait;
use anyhow::{anyhow, Context};
use tokio::sync::broadcast;


/// A command, created within a `NodeCommunicator` for invoking various methods on a Node which is
/// running asynchronously. Sent via concurrency channels. Also includes a sender for sending
/// a response to the originator of the request.
#[derive(Debug)]
pub enum NodeCommand<V: Value> {
    AE(AppendEntries<V>, oneshot::Sender<Result<AppendEntriesResponse, RaftError>>),
    RV(RequestVote, oneshot::Sender<Result<RequestVoteResponse, RaftError>>),
    ClientWriteRequest(ClientWriteRequest<V>, oneshot::Sender<Result<ClientWriteResponse, RaftError>>),
    ClientReadRequest(ClientReadRequest, oneshot::Sender<Result<ClientReadResponse<V>, RaftError>>)
}

/// This is used to communicate with a raft node once we begin the main loop
#[derive(Debug, Clone)]
pub struct NodeCommunicator<V: Value> {
    // used for sending messages to the node
    rpc_sender: mpsc::UnboundedSender<NodeCommand<V>>,

    // only used to subscribe new clients
    commit_sender: broadcast::Sender<CommitEntry<V>>,
}

/// Size of commit notification channel. Note that in case of lagging receivers(clients), they will never block
/// the node from sending values, but they might lose some commit notifications - see https://docs.rs/tokio/0.3.5/tokio/sync/broadcast/index.html#lagging
const COMMIT_CHANNEL_SIZE: usize = 1024;

impl <V: Value> NodeCommunicator<V> {

    /// Creates a node object, returning it along with a communicator that can be used to interact
    /// with it after we spawn it on a task/thread.
    pub async fn create_with_node<T: Transport<V>>(id: usize,
                            number_of_nodes: usize,
                            transport: T) -> (Node<V, T>, NodeCommunicator<V>) {
       let (rpc_sender, rpc_receiver) = mpsc::unbounded_channel();
        let (commit_sender, _commit_receiver) = broadcast::channel(COMMIT_CHANNEL_SIZE);
        let mut communicator = NodeCommunicator {
            rpc_sender, commit_sender: commit_sender.clone()
        };
        let mut node = Node::new(id, number_of_nodes, transport, rpc_receiver,
                             commit_sender);
        Transport::on_node_communicator_created(&mut communicator, &mut node).await;
        (node, communicator)
    }

    /// Allows one to be notified of entries that were committed AFTER this function is called.
    pub async fn commit_channel(&self) -> Result<broadcast::Receiver<CommitEntry<V>>, RaftError> {
        Ok(self.commit_sender.subscribe())
    }

    #[instrument]
    pub async fn append_entries(&self, ae: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::AE(ae, tx);
        self.rpc_sender.send(cmd).context("send command to append entries").map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after submit_value").map_err(RaftError::InternalError)?
    }

    #[instrument]
    pub async fn request_vote(&self, rv: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::RV(rv, tx);
        self.rpc_sender.send(cmd).context("send command to request vote").map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after submit_value").map_err(RaftError::InternalError)?
    }

    #[instrument]
    pub async fn submit_value(&self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::ClientWriteRequest(req, tx);
        self.rpc_sender.send(cmd).context("send command to submit value").map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after submit_value").map_err(RaftError::InternalError)?
    }

    pub async fn request_values(&self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::ClientReadRequest(req, tx);
        self.rpc_sender.send(cmd).context("send command to request values").map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after submit_value").map_err(RaftError::InternalError)?
    }
}

/// Handles commands sent from a `NodeCommunicator`
/// This should be implemented by all states of a `Node`
///
/// This only supports synchronous handlers, which should resolve quickly(as they're in the body
/// of a select! branch handler).
///
/// Write requests to leaders are, by their nature, asynchronous - we have to wait until
/// a submitted value is replicated to a majority before sending a response, and this can take
/// a relatively long time. We do not want to block heartbeats or handling of other messages in that
/// time, therefore, we will handle them differently (see implementation in leader) by passing
/// the `tx` to a different task which will eventually send a response.
pub(in crate::consensus) trait CommandHandler<V: Value> {
    fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError>;
    fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError>;

    /// Should not be used by leader, see note above
    fn handle_client_write_request(&mut self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse, RaftError>;
    fn handle_client_read_request(&mut self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError>;

    /// Handles a command sent from `NodeCommunicator`
    fn handle_command(&mut self, cmd: NodeCommand<V>) {
        match cmd {
            NodeCommand::AE(ae, res) => {
                res.send(self.handle_append_entries(ae)).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            },
            NodeCommand::RV(rv, res) => {
                res.send(self.handle_request_vote(rv)).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            }
            NodeCommand::ClientWriteRequest(req, res) => {
                res.send(self.handle_client_write_request(req)).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            }
            NodeCommand::ClientReadRequest(req, res) => {
                res.send(self.handle_client_read_request(req)).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            }
        }

    }
}
