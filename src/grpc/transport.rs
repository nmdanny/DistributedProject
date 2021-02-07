use crate::consensus::{client::ClientTransport, types::*};
use crate::consensus::transport::Transport;
use crate::consensus::node_communicator::NodeCommunicator;
use crate::consensus::timing::HEARTBEAT_INTERVAL;
use curve25519_dalek::digest::generic_array::GenericArray;
use derivative;
use async_trait::async_trait;
use futures::{Future, Stream};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{IntoRequest, Response};
use tracing_futures::Instrument;
use std::{convert::{TryInto, TryFrom}, pin::Pin, sync::Arc, unimplemented};

use std::collections::HashMap;
use serde::Serialize;
use serde::{Deserialize, de::DeserializeOwned};

use anyhow::Context;
use super::pb::{GenericMessage, TypeConversionError, raft_client::RaftClient, raft_server::{Raft, RaftServer}, raft_to_tonic, tonic_to_raft, tonic_stream_to_raft};
use std::fmt::Debug;

const STARTUP_TIME: tokio::time::Duration = tokio::time::Duration::from_secs(0u64);
const RECONNECT_DELAY: tokio::time::Duration = tokio::time::Duration::from_secs(2u64);
const MAX_RECONNECTS: usize = 5;

#[derive(Debug, Clone)]
pub struct GRPCConfig {
    /// Maps a node ID to gRPC URL
    pub nodes: HashMap<usize, String>
}

const DEFAULT_START_PORT: u16 = 18200;

impl GRPCConfig {
    pub fn new() -> Self {
        GRPCConfig { nodes: Default::default() }
    }

    pub fn default_for_nodes(num_nodes: usize) -> Self {
        GRPCConfig {
            nodes: (0 .. num_nodes).into_iter().map(|node_id| {
                (node_id, format!("[::1]:{}", DEFAULT_START_PORT + node_id as u16))
            }).collect()
        }
    }
}

type Client = RaftClient<tonic::transport::channel::Channel>;

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct GRPCTransportInner<V: Value> {
    /// Maps each node to two types of clients:
    /// 1. a general purpose client
    /// 2. a heartbeat client
    ///
    /// See comment under `send_append_entries` on why
    #[derivative(Debug="ignore")]
    clients: HashMap<usize, (Client, Client)>,

    /// Only set when the transport belongs to a raft node
    #[derivative(Debug="ignore")]
    my_node: Option<NodeCommunicator<V>>,

    /// Only set when the transport belongs to a raft node
    #[derivative(Debug="ignore")]
    my_id: Option<Id>,

    #[derivative(Debug="ignore")]
    config: GRPCConfig,

    clients_init: bool
}

/// Attempts to connect to all raft nodes(except my own node, in case we are a node), returns with an error if any connection fails
#[instrument]
pub async fn connect_to_nodes(my_id: Option<Id>, config: &GRPCConfig) -> Result<HashMap<usize, (Client, Client)>, anyhow::Error> {
    let mut clients = HashMap::new();
    for (id, url) in config.nodes.clone().into_iter().filter(|(id, _)| Some(*id) != my_id) {
        let mut attempts = 0;
        let client = loop {
            attempts += 1;
            let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", url))?;
            let endpoint2 = tonic::transport::Endpoint::from_shared(format!("http://{}", url))?;
            info!("Connecting to {}", url);
            let res = tokio::try_join!(
                RaftClient::connect(endpoint), RaftClient::connect(endpoint2)
            );
            match res {
                Ok(clients) => break clients,
                Err(e) => {
                    error!("Attempt {}/{} - Couldn't connect to node: {:?}", attempts, MAX_RECONNECTS, e);
                    if attempts >= MAX_RECONNECTS {
                        return Err(e.into())
                    }
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }

            }
        };
        clients.insert(id, client);
    }
    Ok(clients)
}

#[derive(Debug, Clone)]
pub struct GRPCTransport<V: Value> {
    inner: Arc<RwLock<GRPCTransportInner<V>>>
}

impl <V: Value> GRPCTransport<V> {
    pub async fn new(my_id: Option<Id>, config: GRPCConfig) -> Result<Self, anyhow::Error> {
        let mut inner = GRPCTransportInner {
            clients: Default::default(),
            my_id,
            my_node: None,
            config,
            clients_init: false
        };
        if my_id.is_none() {
            inner.clients = connect_to_nodes(my_id, &inner.config).await?;
            inner.clients_init = true;
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner))
        })
    }

    /* note, due to https://github.com/rust-lang/rust/issues/57017
       code will turn to be somewhat ugly 
    */

    pub async fn connect_to_nodes(&self) {
        let (my_id, config) = {
            let inner = self.inner.read();
            let my_id = inner.my_id;
            if inner.clients_init {
                return;
            }
            let config = inner.config.clone();
            (my_id, config)
        };
        let clients = connect_to_nodes(my_id, &config).await.unwrap();

        let mut inner = self.inner.write();
        inner.clients = clients;
        inner.clients_init = true;
    }

    /// Performs a request, either using a node communicator(if sending message to ourself)
    /// or a GRPC client, in which case we can choose to use an alternative client(for hearbeats,
    /// see below)
    pub async fn do_request<
        Req: 'static + Send + Debug + TryInto<GenericMessage, Error=TypeConversionError>, 
        Res: Debug + TryFrom<GenericMessage, Error=TypeConversionError>,
        F1: Send + Future<Output = Result<Res, RaftError>>,
        F2: Send + Future<Output = Result<tonic::Response<GenericMessage>, tonic::Status>>>(&self, 
            to: Id,
            msg: Req,
            desc: &str,
            comm_handler: impl Fn(Req, NodeCommunicator<V>) -> F1,
            client_handler: impl Fn(GenericMessage, Client) -> F2,
            use_alt_client: impl Fn(&Req) -> bool
        ) -> Result<Res, RaftError> {
        let use_client = {
            let inner = self.inner.read();
            assert!(inner.clients_init, "Node connections must be attached to transport before doing requests");
            Some(to) == inner.my_id
        };
        if use_client {
            let comm = self.inner.read().my_node.clone().unwrap();
            comm_handler(msg, comm).await
        } else {
            debug!(trans=true, "(req-out) serializing {} to {}: {:?}", desc, to, msg);
            let use_alt_client = use_alt_client(&msg);
            let msg: GenericMessage =
                tokio::task::spawn_blocking(move || msg.try_into().context("While serializing message").map_err(RaftError::InternalError))
                    .await.unwrap()?;
            let client = {
                let inner = self.inner.read();
                let (client, client_alt) = inner.clients.get(&to).expect("invalid 'to' id");
                if use_alt_client { client_alt.clone() } else { client.clone() }
            };
            debug!(trans=true, "(req-out) sending {} to {} of {} bytes {}", desc, to, msg.buf.len(),
                 if use_alt_client { "via alt client" } else { "via normal client" }
            );
            let res = client_handler(msg, client).await;
            let res = tonic_to_raft(res, to);
            debug!(trans=true, "(res-in) response of sending {} to {}: {:?}", desc, to, res);
            res
        }
    }
}


#[async_trait]
impl <V: Value> Transport<V> for GRPCTransport<V> {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        /* Sending large messages(many/big log entries) may take a long time, so heartbeats(which are much smaller) are
           sent in parallel - see leader implementation.
           
           We'll send heartbeats over a separate connection(a different GRPC client), to prevent TCP flow control/congestion control
           mechanisms from preventing heartbeats from reaching the target, because otherwise large AE entries
           could hog the entire connection, causing followers to miss the heartbeats and start a new election
        */
        self.do_request(to, msg, "AE",
        |msg, comm| async move { comm.append_entries(msg).await },
        |msg, mut client| async move { client.append_entries_rpc(msg).await },
        |msg| msg.entries.is_empty()
        ).await
    }

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        self.do_request(to, msg, "RV",
        |msg, comm| async move { comm.request_vote(msg).await },
        |msg, mut client| async move { client.vote_request_rpc(msg).await },
        |_msg| false
        ).await
    }

    #[instrument]
    async fn on_node_communicator_created(&mut self, id: Id, comm: &mut NodeCommunicator<V>) {

        let addr = {
            let mut inner = self.inner.write();
            inner.my_id = Some(id);
            inner.my_node = Some(comm.clone());
            inner.config.nodes.get(&id).unwrap().parse().unwrap()
        };

        let communicator = comm.clone();
        tokio::spawn(async move {
            info!("Spawning GRPC server for node {}", id);
            let raft_service = RaftService { communicator };
            let _server = tonic::transport::Server::builder()
                .add_service(RaftServer::new(raft_service))
                .serve(addr)
                .await.unwrap();

        }.instrument(info_span!("grpc_server", id=?id)));

        tokio::time::sleep(STARTUP_TIME).await;

        self.connect_to_nodes().await;

        info!("Setup grpc transport for node {}", id);
    }

    async fn before_node_loop(&mut self, _id: Id) {

    }
}


#[async_trait(?Send)]
impl <V: Value> ClientTransport<V> for GRPCTransport<V> {
    #[instrument]
    async fn submit_value(&self, node_id: usize, value: V) -> Result<ClientWriteResponse<V>,RaftError> {
        let inner = self.inner.read();
        let value = ClientWriteRequest { value };
        trace!("submit_value, node_id: {}", node_id);

        if Some(node_id) == inner.my_id {
            return inner.my_node.as_ref().unwrap().submit_value(value).await;
        }

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").0.clone();
        let msg: GenericMessage = value.try_into().context("While serializing ClientWriteRequest").map_err(RaftError::InternalError)?;
        let res = client.client_write_request(msg).await;
        tonic_to_raft(res, node_id)
    }

    #[instrument]
    async fn request_values(&self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError> {
        let inner = self.inner.read();
        let value = ClientReadRequest { from, to };
        trace!("request_values, node_id: {}", node_id);

        if Some(node_id) == inner.my_id {
            return inner.my_node.as_ref().unwrap().request_values(value).await;
        }

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").0.clone();
        let msg: GenericMessage = value.try_into().context("While serializing ClientReadRequest").map_err(RaftError::InternalError)?;
        let res = client.client_read_request(msg).await;
        tonic_to_raft(res, node_id)
    }

    #[instrument]
    async fn force_apply(&self, node_id: usize, value: V) -> Result<ClientForceApplyResponse<V>, RaftError> {
        let inner = self.inner.read();
        let value = ClientForceApplyRequest { value };
        trace!("force_apply, node_id: {}", node_id);

        if Some(node_id) == inner.my_id {
            return inner.my_node.as_ref().unwrap().force_apply(value).await;
        }

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").0.clone();
        let msg: GenericMessage = value.try_into().context("While serializing ClientForceApplyRequest").map_err(RaftError::InternalError)?;
        let res = client.client_force_apply_request(msg).await;
        tonic_to_raft(res, node_id)
    }

    #[instrument]
    async fn get_sm_event_stream<EventType: Value>(&self, node_id: usize) -> Result<Pin<Box<dyn Stream<Item = EventType>>>, RaftError> {
        let inner = self.inner.read();
        trace!("get_sm_event_stream, node_id: {}", node_id);

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").0.clone();
        let stream = client.state_machine_updates(tonic::Request::new(GenericMessage {
            buf: Vec::new()
        })).await;
        tonic_stream_to_raft(stream)
    }
}

#[derive(Debug, Clone)]
pub struct RaftService<V: Value> { communicator: NodeCommunicator<V> }

impl <V: Value> RaftService<V> {
    pub async fn do_op<Req: Debug + Send + 'static, Res: Debug, F: Future<Output = Result<Res, RaftError>>>(&self, desc: &'static str,
        request: tonic::Request<GenericMessage>,
        op: impl Fn(Req) -> F) 
        -> Result<tonic::Response<GenericMessage>, tonic::Status>
        where Req: TryFrom<GenericMessage, Error = TypeConversionError>, Res: TryInto<GenericMessage, Error = TypeConversionError>
    {
        debug!(trans=true, "(req-in) GRPC handler for {}, got request of size {} bytes", desc, request.get_ref().buf.len());
        let request = tokio::task::spawn_blocking(move || Req::try_from(request.into_inner()).context(format!("serializing request for {}", desc)).map_err(|e|
            tonic::Status::internal(e.to_string()))).await.unwrap()?;
        debug!(trans=true, "(req-in) GRPC handler for {}, request contents are {:?}", desc, request);
        let res: Result<Res, RaftError> = op(request).await;
        debug!(trans=true, "(res-out) GRPC handler for {}, got response {:?} ", desc, res);
        raft_to_tonic(res)
    }
}

#[async_trait]
impl <V: Value> Raft for RaftService<V> {

        type StateMachineUpdatesStream = Pin<Box<dyn Stream<Item = Result<GenericMessage, tonic::Status>> + Send + Sync + 'static>>;

        #[instrument]
        async fn vote_request_rpc(
            &self,
            request: tonic::Request<GenericMessage>,
        ) -> Result<tonic::Response<GenericMessage>, tonic::Status>
        {
            self.do_op("vote_request_rpc", request, |rv| self.communicator.request_vote(rv)).await
        }

        #[instrument]
        async fn append_entries_rpc(
            &self,
            request: tonic::Request<GenericMessage>,
        ) -> Result<tonic::Response<GenericMessage>, tonic::Status> {
            self.do_op("append_entries_rpc", request, |ae| self.communicator.append_entries(ae)).await
        }
        

        #[instrument]
        async fn client_write_request(
            &self,
            request: tonic::Request<GenericMessage>,
        ) -> Result<tonic::Response<GenericMessage>, tonic::Status> {
            self.do_op("client_write_request", request, |cwr| self.communicator.submit_value(cwr)).await
        }

        #[instrument]
        async fn client_read_request(
            &self,
            request: tonic::Request<GenericMessage>,
        ) -> Result<tonic::Response<GenericMessage>, tonic::Status> {
            self.do_op("client_read_request", request, |crr| self.communicator.request_values(crr)).await
        }

        #[instrument]
        async fn client_force_apply_request(
            &self,
            request: tonic::Request<GenericMessage>,
        ) -> Result<tonic::Response<GenericMessage>, tonic::Status> {
            self.do_op("client_force_apply_request", request, |cfar| self.communicator.force_apply(cfar)).await
        }


        async fn state_machine_updates(
            &self,
            _request: tonic::Request<GenericMessage>,
        ) -> Result<tonic::Response<Self::StateMachineUpdatesStream>, tonic::Status> {
            // TODO what is the type here
            let sm_events = self.communicator.state_machine_output_channel_raw();
            let sm_events = tokio_stream::wrappers::UnboundedReceiverStream::new(sm_events);
            let sm_events = Box::pin(sm_events.map(|buf| Ok(GenericMessage { buf })));
            Ok(tonic::Response::new(sm_events))
        }
}


#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use tokio::{sync::mpsc, task::{self, JoinHandle}};
    use tracing_futures::Instrument;
    use crate::consensus::{logging::setup_logging, node_communicator::NodeCommunicator, state_machine::NoopStateMachine, transport::ThreadTransport};
    use crate::consensus::adversarial_transport::{AdversaryTransport, AdversaryClientTransport};


    /// A single threaded instance of many nodes and clients
    pub struct Scenario<V: Value> {
        pub server_transports: Vec<AdversaryTransport<V, GRPCTransport<V>>>,
        pub clients: Vec<crate::consensus::client::Client<AdversaryClientTransport<V, GRPCTransport<V>>, V>>
    }

    impl <V: Value> Scenario<V> where V::Result: Default{
        pub async fn setup(num_nodes: usize, num_clients: usize) -> Self
        {
            
            let futures = (0 .. num_nodes).map(|i| async move {
                let grpc_transport = GRPCTransport::new(Some(i), GRPCConfig::default_for_nodes(num_nodes)).await.unwrap();
                let server_transport = AdversaryTransport::new(grpc_transport, num_nodes);
                let (node, _comm) = NodeCommunicator::create_with_node(i,
                                                num_nodes,
                                                server_transport.clone(), NoopStateMachine::default()).await;
                (node, server_transport)
            }).collect::<Vec<_>>();

            let (nodes, server_transports) = futures::future::join_all(futures).await.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
            

            let mut clients = vec![];
            for i in 0 .. num_clients {
                let grpc_transport = GRPCTransport::new(None, GRPCConfig::default_for_nodes(num_nodes)).await.unwrap();
                let client_transport = AdversaryClientTransport::new(grpc_transport);
                let client = crate::consensus::client::Client::new(format!("Client {}", i), client_transport, num_nodes);
                clients.push(client);
            }

            // spawn all nodes
            for node in nodes.into_iter() {
                let id = node.id;
                tokio::task::spawn_local(async move {
                    node.run_loop()
                        .instrument(tracing::info_span!("node-loop", node.id = id))
                        .await
                        .unwrap_or_else(|e| error!("Error running node {}: {:?}", id, e))
                });
            }

            Scenario {
                server_transports,
                clients
            }
        }
    }


    #[tokio::test]
    pub async fn grpc_simple_crash() {
        let ls = task::LocalSet::new();
        setup_logging().unwrap();
        ls.run_until(async move {
            let scenario = Scenario::<u32>::setup(3, 1).await;
            let Scenario { mut clients, mut server_transports, ..} = scenario;
            
            let client_jh = task::spawn_local(async move {
                // submit first value
                clients[0].submit_value(100).await.unwrap();

                // "crash" server 0 by preventing it from sending/receiving messages to other nodes
                server_transports.get_mut(0).unwrap().set_omission_chance(0, 1.0).await;

                // submit second value
                clients[0].submit_value(200).await.unwrap();

            });

            // let res = rx.take(2).map(|e| e.value).collect::<Vec<_>>().await;
            // assert_eq!(res, vec![100, 200]);

            client_jh.await.unwrap();

        }).await;
    }
}