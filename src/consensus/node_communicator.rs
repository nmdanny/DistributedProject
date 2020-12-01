use crate::consensus::types::{Value, AppendEntries, AppendEntriesResponse, RaftError, RequestVote, RequestVoteResponse, ClientWriteRequest, ClientWriteResponse, ClientReadRequest, ClientReadResponse, CommitEntry};
use tokio::sync::{oneshot, mpsc};
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use async_trait::async_trait;
use anyhow::{anyhow, Context};


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
    rpc_sender: mpsc::UnboundedSender<NodeCommand<V>>,
}

impl <V: Value> NodeCommunicator<V> {

    /// Creates a node object, returning it along with a communicator that can be used to interact
    /// with it after we spawn it on a task/thread.
    pub fn create_with_node<T: Transport<V>>(id: usize,
                            number_of_nodes: usize,
                            transport: T) -> (Node<V, T>, NodeCommunicator<V>) {
       let (rpc_sender, rpc_receiver) = mpsc::unbounded_channel();
        let communicator = NodeCommunicator {
            rpc_sender
        };
        let node = Node::new(id, number_of_nodes, transport, rpc_receiver);
        (node, communicator)
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
/// This should be implemented by all states of a `Node`, in addition to Node itself.
/// States should compose the Node implementation, and may do additional things before or after
/// the node's handlers is called.
#[async_trait]
pub(in crate::consensus) trait CommandHandler<V: Value> {
    async fn handle_append_entries(&mut self, req: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError>;
    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteResponse, RaftError>;
    async fn handle_client_write_request(&mut self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse, RaftError>;
    async fn handle_client_read_request(&mut self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError>;

    /// Handles a command sent from `NodeCommunicator`
    async fn handle_command(&mut self, cmd: NodeCommand<V>) {
        match cmd {
            NodeCommand::AE(ae, res) => {
                res.send(self.handle_append_entries(ae).await).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            },
            NodeCommand::RV(rv, res) => {
                res.send(self.handle_request_vote(rv).await).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            }
            NodeCommand::ClientWriteRequest(req, res) => {
                res.send(self.handle_client_write_request(req).await).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            }
            NodeCommand::ClientReadRequest(req, res) => {
                res.send(self.handle_client_read_request(req).await).unwrap_or_else(|e| {
                    error!("Couldn't send response: {:?}", e);
                });
            }
        }

    }
}
