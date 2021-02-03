use crate::consensus::{client::ClientTransport, types::*};
use crate::consensus::transport::Transport;
use crate::consensus::node_communicator::NodeCommunicator;
use curve25519_dalek::digest::generic_array::GenericArray;
use derivative;
use async_trait::async_trait;
use futures::{Future, Stream};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{IntoRequest, Response};
use tracing_futures::Instrument;
use std::{convert::{TryInto, TryFrom}, pin::Pin, sync::Arc};

use std::collections::HashMap;
use serde::Serialize;
use serde::{Deserialize, de::DeserializeOwned};

use anyhow::Context;
use super::pb::{GenericMessage, TypeConversionError, raft_client::RaftClient, raft_server::{Raft, RaftServer}, raft_to_tonic, tonic_to_raft, tonic_stream_to_raft};

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
    #[derivative(Debug="ignore")]
    clients: HashMap<usize, Client>,

    #[derivative(Debug="ignore")]
    my_node: Option<NodeCommunicator<V>>,

    #[derivative(Debug="ignore")]
    my_id: Option<Id>,

    #[derivative(Debug="ignore")]
    config: GRPCConfig,

    clients_init: bool
}

impl <V: Value> GRPCTransportInner<V> {
    /// Attempts to connect to all raft nodes, returns with an error if any connection fails
    /// 
    /// Note that 
    #[instrument]
    pub async fn connect_to_nodes(&mut self) -> Result<(), anyhow::Error> {
        let my_id = self.my_id;
        for (id, url) in self.config.nodes.clone().into_iter().filter(|(id, _)| Some(*id) != my_id) {
            let mut attempts = 0;
            let client = loop {
                attempts += 1;
                let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", url))?;
                info!("Connecting to {}", url);
                let res = RaftClient::connect(endpoint).await;
                match res {
                    Ok(client) => break client,
                    Err(e) => {
                        error!("Attempt {}/{} - Couldn't connect to node: {:?}", attempts, MAX_RECONNECTS, e);
                        if attempts >= MAX_RECONNECTS {
                            return Err(e.into())
                        }
                        tokio::time::sleep(RECONNECT_DELAY).await;
                    }

                }
            };
            self.clients.insert(id, client);
        }
        self.clients_init = true;
        info!("Connected to all raft nodes successfully");
        Ok(())
    }
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
            inner.connect_to_nodes().await?;
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner))
        })
    }

}


#[async_trait(?Send)]
impl <V: Value> Transport<V> for GRPCTransport<V> {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let inner = self.inner.read();
        assert!(inner.clients_init, "Not initialized, didn't connect to clients yet");
        if Some(to) == inner.my_id {
           return inner.my_node.as_ref().unwrap().append_entries(msg).await;
        }

        let mut client = inner.clients.get(&to).expect("invalid 'to' id").clone();
        let msg: GenericMessage = msg.try_into().context("While serializing AppendEntries").map_err(RaftError::InternalError)?;
        let res = client.append_entries_rpc(msg).await;
        tonic_to_raft(res, to)
    }

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        let inner = self.inner.read();
        assert!(inner.clients_init, "Not initialized, didn't connect to clients yet");
        if Some(to) == inner.my_id {
           return inner.my_node.as_ref().unwrap().request_vote(msg).await;
        }

        let mut client = inner.clients.get(&to).expect("invalid 'to' id").clone();
        let msg: GenericMessage = msg.try_into().context("While serializing RequestVote").map_err(RaftError::InternalError)?;
        let res = client.vote_request_rpc(msg).await;
        tonic_to_raft(res, to)
    }

    #[instrument]
    async fn on_node_communicator_created(&mut self, id: Id, comm: &mut NodeCommunicator<V>) {

        let mut inner = self.inner.write();
        inner.my_id = Some(id);
        inner.my_node = Some(comm.clone());
        let addr = inner.config.nodes.get(&id).unwrap().parse().unwrap();

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

        inner.connect_to_nodes().await.unwrap();

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

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").clone();
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

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").clone();
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

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").clone();
        let msg: GenericMessage = value.try_into().context("While serializing ClientForceApplyRequest").map_err(RaftError::InternalError)?;
        let res = client.client_force_apply_request(msg).await;
        tonic_to_raft(res, node_id)
    }

    #[instrument]
    async fn get_sm_event_stream<EventType: Value>(&self, node_id: usize) -> Result<Pin<Box<dyn Stream<Item = EventType>>>, RaftError> {
        let inner = self.inner.read();
        trace!("get_sm_event_stream, node_id: {}", node_id);

        let mut client = inner.clients.get(&node_id).expect("invalid 'node_id'").clone();
        let stream = client.state_machine_updates(tonic::Request::new(GenericMessage {
            buf: Vec::new()
        })).await;
        tonic_stream_to_raft(stream)
    }
}

#[derive(Debug, Clone)]
pub struct RaftService<V: Value> { communicator: NodeCommunicator<V> }

impl <V: Value> RaftService<V> {
    pub async fn do_op<Req, Res, F: Future<Output = Result<Res, RaftError>>>(&self, desc: &'static str,
        request: tonic::Request<GenericMessage>,
        op: impl Fn(Req) -> F) 
        -> Result<tonic::Response<GenericMessage>, tonic::Status>
        where Req: TryFrom<GenericMessage, Error = TypeConversionError>, Res: TryInto<GenericMessage, Error = TypeConversionError>
    {
        let request = Req::try_from(request.into_inner()).context(format!("serializing request for {}", desc)).map_err(|e| 
            tonic::Status::internal(e.to_string()))?;
        let res: Result<Res, RaftError> = op(request).await;
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