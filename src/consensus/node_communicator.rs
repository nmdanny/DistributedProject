use std::pin::Pin;

use crate::consensus::types::*;
use futures::Stream;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{oneshot, mpsc};
use tracing_futures::Instrument;
use crate::consensus::state_machine::{StateMachine, ForceApply};
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use async_trait::async_trait;
use anyhow::{anyhow, Context};
use tokio::sync::broadcast;

use super::timing::RaftServerSettings;


/// A command, created within a `NodeCommunicator` for invoking various methods on a Node which is
/// running asynchronously. Sent via concurrency channels. Also includes a sender for sending
/// a response to the originator of the request.
#[derive(Debug)]
pub enum NodeCommand<V: Value> {
    AE(AppendEntries<V>, oneshot::Sender<Result<AppendEntriesResponse, RaftError>>),
    RV(RequestVote, oneshot::Sender<Result<RequestVoteResponse, RaftError>>),
    ClientWriteRequest(ClientWriteRequest<V>, oneshot::Sender<Result<ClientWriteResponse<V>,RaftError>>),
    ClientReadRequest(ClientReadRequest, oneshot::Sender<Result<ClientReadResponse<V>, RaftError>>),
    ClientForceApplyRequest(ClientForceApplyRequest<V>, oneshot::Sender<Result<ClientForceApplyResponse<V>, RaftError>>)
}

/// Used to communicate with a raft node once we begin the main loop - note that this runs on
/// the same process/machine/context where the Node is, and not necessarily where the client is;
/// A client has no direct access to the `NodeCommunicator` and would usually instead use some
/// networking mechanism(e.g, HTTP, RPC) to communicate with a `NodeCommunicator`.
///
/// In essence, `NodeCommunicator` is usually driven by a web server, and responds to requests
/// made by either other Raft nodes (via the `append_entries`, `request_vote` and `commit_channel` functions)
/// or by clients (the `submit_value`, `request_values` and `commit_channel` functions)
///
#[derive(Debug, Clone)]
pub struct NodeCommunicator<V: Value> {
    // used for sending messages to the node
    rpc_sender: mpsc::UnboundedSender<NodeCommand<V>>,

    // used to subscribe clients to new committed entries
    commit_sender: broadcast::Sender<(CommitEntry<V>, V::Result)>,

    // used to subscribe clients to state machine events(serialized)
    // TODO: avoid serialization at this point, somehow tie NodeCommunicator to the state machine?
    sm_event_sender: broadcast::Sender<Vec<u8>>,

    id: usize
}

/// Size of commit notification channel. Note that in case of lagging receivers(clients), they will never block
/// the node from sending values, but they might lose some commit notifications - see https://docs.rs/tokio/0.3.5/tokio/sync/broadcast/index.html#lagging
const COMMIT_CHANNEL_SIZE: usize = 1024;

impl <V: Value> NodeCommunicator<V> {

    /// Creates a node object, returning it along with a communicator that can be used to interact
    /// with it after we spawn it on a task/thread.
    pub async fn create_with_node<T: Transport<V>, S: StateMachine<V, T>>(
                            id: usize,
                            number_of_nodes: usize,
                            transport: T,
                            machine: S,
                            settings: RaftServerSettings) -> (Node<V, T, S>, NodeCommunicator<V>) {
        let mut node = Node::new(id, number_of_nodes, transport, settings);
        let communicator = NodeCommunicator::from_node(&mut node).await;
        node.attach_state_machine(machine);
        (node, communicator)
    }

    /// Creates a NodeCommunicator for a node that wasn't spawned yet
    pub async fn from_node<T: Transport<V>, S: StateMachine<V, T>>(node: &mut Node<V, T, S>) -> NodeCommunicator<V> {
        let (rpc_sender, rpc_receiver) = mpsc::unbounded_channel();
        let mut communicator = NodeCommunicator {
            rpc_sender,
            commit_sender: node.sm_result_sender.clone(),
            sm_event_sender: node.sm_publish_sender.clone(),
            id: node.id
        };
        node.receiver = Some(rpc_receiver);
        node.transport.on_node_communicator_created(node.id, &mut communicator).await;
        communicator
    }

    /// Allows one to be notified of entries that were committed AFTER this function is called.
    pub async fn commit_channel(&self) -> Result<broadcast::Receiver<(CommitEntry<V>, V::Result)>, RaftError> {
        Ok(self.commit_sender.subscribe())
    }

    #[instrument]
    pub fn state_machine_output_channel<E: Value>(&self) -> mpsc::UnboundedReceiver<E> {
        let mut rec = self.sm_event_sender.subscribe();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok(sm_bytes) = rec.recv().await {
                let event = serde_json::from_slice::<E>(&sm_bytes);
                match event {
                    Ok(event) => { 
                        let res = tx.send(event);
                        if let Err(_) = res {
                            error!("SM output receiver was dropped");
                            return;
                        }
                    }
                    Err(err) => {
                        panic!("Couldn't deserialize JSON: {:?}\n", err);
                    }
                }
            }
        }.instrument(info_span!("state_machine_output_channel", id=?self.id)));
        rx
    }

    #[instrument]
    pub fn state_machine_output_channel_raw(&self) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut rec = self.sm_event_sender.subscribe();
        tokio::spawn(async move {
            while let Ok(sm_bytes) = rec.recv().await {
                let res = tx.send(sm_bytes);
                if let Err(_) = res {
                    error!("Raw SM output receiver was dropped");
                    return;
                }
            }
        });
        rx
    }


    #[instrument]
    pub async fn append_entries(&self, ae: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::AE(ae, tx);
        self.rpc_sender.send(cmd).context(anyhow::format_err!("node {} must've crashed when trying to send AppendEntries", self.id)).map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after append_entries").map_err(RaftError::InternalError)?
    }

    #[instrument]
    pub async fn request_vote(&self, rv: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::RV(rv, tx);
        self.rpc_sender.send(cmd).context(anyhow::format_err!("node {} must've crashed when trying to send RequestVote", self.id)).map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after request_vote").map_err(RaftError::InternalError)?
    }

    #[instrument]
    pub async fn submit_value(&self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse<V>,RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::ClientWriteRequest(req, tx);
        self.rpc_sender.send(cmd).context(anyhow::format_err!("node {} must've crashed when trying to send ClientWriteRequest", self.id)).map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after submit_value").map_err(RaftError::InternalError)?
    }

    #[instrument]
    pub async fn request_values(&self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::ClientReadRequest(req, tx);
        self.rpc_sender.send(cmd).context(anyhow::format_err!("node {} must've crashed when trying to send ClientReadRequest", self.id)).map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after request_values").map_err(RaftError::InternalError)?
    }

    #[instrument]
    pub async fn force_apply(&self, req: ClientForceApplyRequest<V>) -> Result<ClientForceApplyResponse<V>, RaftError> {
        let (tx, rx) = oneshot::channel();
        let cmd = NodeCommand::ClientForceApplyRequest(req, tx);
        self.rpc_sender.send(cmd).context(anyhow::format_err!("node {} must've crashed when trying to send ClientForceApplyRequest", self.id)).map_err(RaftError::CommunicatorError)?;
        rx.await.context("receive value after force_apply").map_err(RaftError::InternalError)?

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
    fn handle_client_write_request(&mut self, req: ClientWriteRequest<V>) -> Result<ClientWriteResponse<V>, RaftError>;
    fn handle_client_read_request(&mut self, req: ClientReadRequest) -> Result<ClientReadResponse<V>, RaftError>;

    // this is handled asynchronously, passed to the state machine task
    fn handle_force_apply(&mut self, force_apply: ForceApply<V>);

    /// Handles a command sent from `NodeCommunicator`
    #[instrument(skip(self), level="error")]
    fn handle_command(&mut self, cmd: NodeCommand<V>) {
        match cmd {
            NodeCommand::AE(ae, res) => {
                res.send(self.handle_append_entries(ae)).unwrap_or_else(|e| {
                    error!("Couldn't send response to NodeCommunicator: {:?}", e);
                });
            },
            NodeCommand::RV(rv, res) => {
                res.send(self.handle_request_vote(rv)).unwrap_or_else(|e| {
                    error!("Couldn't send response to NodeCommunicator: {:?}", e);
                });
            }
            NodeCommand::ClientWriteRequest(req, res) => {
                res.send(self.handle_client_write_request(req)).unwrap_or_else(|e| {
                    error!("Couldn't send response to NodeCommunicator: {:?}", e);
                });
            }
            NodeCommand::ClientReadRequest(req, res) => {
                res.send(self.handle_client_read_request(req)).unwrap_or_else(|e| {
                    error!("Couldn't send response to NodeCommunicator: {:?}", e);
                });
            }
            NodeCommand::ClientForceApplyRequest(req, res) => {
                self.handle_force_apply((req, res));
            }
        }

    }
}
