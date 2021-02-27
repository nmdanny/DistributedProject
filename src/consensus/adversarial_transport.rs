use crate::consensus::transport::Transport;
use crate::consensus::client::ClientTransport;
use crate::consensus::types::*;

use parking_lot::{RwLock as PRwLoc};
use serde::__private::de;
use tokio::sync::RwLock;
use std::{collections::HashMap, pin::Pin, sync::Arc};
use std::collections::BTreeMap;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use anyhow::Error;
use rand::distributions::{Uniform, Bernoulli, Distribution};
use async_trait::async_trait;
use futures::{Future, Stream, TryFutureExt};
use tokio_stream::StreamExt;
use std::boxed::Box;
use tokio::time::Duration;
use rand::distributions::uniform::UniformFloat;
use tracing::Instrument;
use std::collections::btree_map::Entry;
use std::cell::{RefCell, Cell};
use std::rc::Rc;

use super::client::{self, EventStream};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum NodeId {
    ServerId(Id), ClientId(Id)
}

#[derive(Debug, Clone)]
pub struct AdversaryState {
    /// Maps nodes to a number in [0,1] indicating the probability of a message(request/response)
    /// that's originating from that node of being dropped
    pub omission_chance: HashMap<NodeId, f64>,

    /// Similar to above, but maps pairs. Has priority over the general
    /// omission chance
    pub pair_omission_chance: HashMap<(NodeId, NodeId), f64>,


    /// Maps nodes to a distribution of delay time in milliseconds
    pub delay_dist: HashMap<NodeId, Uniform<u64>>,

}

impl AdversaryState {
    fn get_omission_chance(&self, from: NodeId, to: NodeId) -> (f64, f64) {
        let req_chance = self.pair_omission_chance
                             .get(&(from, to))
                             .or_else(|| self.omission_chance.get(&from))
                             .unwrap_or(&0.0);

        let res_chance = self.pair_omission_chance
                             .get(&(to, from))
                             .or_else(|| self.omission_chance.get(&to))
                             .unwrap_or(&0.0);

        (*req_chance, *res_chance)

    }
}

#[derive(Debug, Clone)]
pub struct AdversaryHandle {
    inner: Arc<RwLock<AdversaryState>>
}

impl AdversaryHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(AdversaryState {
                omission_chance: Default::default(),
                pair_omission_chance: Default::default(),
                delay_dist: Default::default()
            }))
        }
    }

    pub fn wrap_server_transport<V: Value, T: Transport<V>>(&self, id: Id, transport: T) -> AdversaryTransport<V, T> {
        AdversaryTransport::new(transport, self.clone(), id)
    }

    pub fn wrap_client_transport<V: Value, T: ClientTransport<V>>(&self, id: NodeId, transport: T) -> AdversaryClientTransport<V, T> {
        AdversaryClientTransport::new(id, transport, self.clone())
    }

    pub async fn set_pair_omission_chance(&self, from: NodeId, to: NodeId, chance: f64, symmetric: bool) {
        assert!(0.0 <= chance && chance <= 1.0, "invalid chance");
        let mut state = self.inner.write().await;
        state.pair_omission_chance.insert((from, to), chance);
        if symmetric {
            state.pair_omission_chance.insert((to, from), chance);
        }
    }

    pub async fn set_server_omission_chance(&self, node_id: Id, chance: f64) {
        assert!(0.0 <= chance && chance <= 1.0, "invalid chance");
        let mut state = self.inner.write().await;
        let _ = state.omission_chance.insert(NodeId::ServerId(node_id), chance);
    }

    pub async fn set_server_delay_distribution<D>(&self, node_id: Id, delay_dist: D )
        where D: Into<Uniform<u64>>
    {
        let mut state = self.inner.write().await;
        let _ = state.delay_dist.insert(NodeId::ServerId(node_id), delay_dist.into());
    }

    pub async fn set_client_omission_chance(&self, client_id: Id, chance: f64) {
        assert!(0.0 <= chance && chance <= 1.0, "invalid chance");
        let mut state = self.inner.write().await;
        let _ = state.omission_chance.insert(NodeId::ClientId(client_id), chance);
    }

    pub async fn set_client_delay_distribution<D>(&self, node_id: Id, delay_dist: D )
        where D: Into<Uniform<u64>>
    {
        let mut state = self.inner.write().await;
        let _ = state.delay_dist.insert(NodeId::ClientId(node_id), delay_dist.into());
    }

    pub async fn wrap_delayed_operation<F, R>(&self, from: NodeId, to: NodeId, op: F) -> Result<R, RaftError>
        where F: Future<Output = Result<R, RaftError>> + Send
    {
        let (req_omission, req_delay, res_omission, res_delay) = {
            let state = self.inner.read().await;
            let (req_omission, res_omission) = state.get_omission_chance(from, to);
            let req_delay =  state.delay_dist.get(&from).map(|dist| dist.sample(&mut rand::thread_rng())).unwrap_or(0);
            let res_delay =  state.delay_dist.get(&to).map(|dist| dist.sample(&mut rand::thread_rng())).unwrap_or(0);
            (req_omission, req_delay, res_omission, res_delay)
        };


        let requester_omission = Bernoulli::new(req_omission).expect("omission chance must be in [0,1]");
        let responder_omission = Bernoulli::new(res_omission).expect("omission chance must be in [0,1]");

        tokio::time::sleep(Duration::from_millis(req_delay)).await;

        if requester_omission.sample(&mut rand::thread_rng()) {
            return Err(RaftError::NetworkError(
                anyhow::anyhow!("omission of request from {:?} to {:?}", from, to)));
        }

        let res = op.await;

        tokio::time::sleep(Duration::from_millis(res_delay)).await;

        if responder_omission.sample(&mut rand::thread_rng()) {
            return Err(RaftError::NetworkError(
                anyhow::anyhow!("omission of response from {:?} to {:?}", to, from)));
        }

        res

    }
}


#[derive(Derivative, Clone)]
#[derivative(Debug)]
/// Wraps an arbitrary transport, allowing us to simulate omission(and thus, pause/resume) and
/// message delays.
pub struct AdversaryTransport<V: Value, T: Transport<V>> {
    #[derivative(Debug="ignore")]
    transport: T,

    #[derivative(Debug="ignore")]
    handle: AdversaryHandle,

    #[derivative(Debug="ignore")]
    phantom: std::marker::PhantomData<V>,

    id: Id
}

impl <V: Value, T: Transport<V>> AdversaryTransport<V, T> {
    pub fn new(transport: T, handle: AdversaryHandle, id: Id) -> Self {
        AdversaryTransport {
            transport, phantom: Default::default(),
            handle,
            id
        }
    }

    pub fn get_inner(&self) -> &T {
        &self.transport
    }

}

#[async_trait]
impl <V: Value, T: Transport<V>> Transport<V> for AdversaryTransport<V, T> {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        assert_eq!(self.id, msg.leader_id, "AppendEntries wrong ID");
        self.handle.wrap_delayed_operation(NodeId::ServerId(msg.leader_id), NodeId::ServerId(to), async move {
            self.transport.send_append_entries(to, msg).await
        }).await
    }

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        assert_eq!(self.id, msg.candidate_id, "RequestVote wrong ID");
        self.handle.wrap_delayed_operation(NodeId::ServerId(msg.candidate_id), NodeId::ServerId(to), async move {
            self.transport.send_request_vote(to, msg).await
        }).await
    }

    async fn on_node_communicator_created(&mut self, id: usize, comm: &mut NodeCommunicator<V>) {
        self.transport.on_node_communicator_created(id, comm).await;
    }

    async fn before_node_loop(&mut self, id: Id) {
        self.transport.before_node_loop(id).await;
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct AdversaryClientTransport<V: Value, T: ClientTransport<V>>
{
    #[derivative(Debug = "ignore")]
    transport: T,

    #[derivative(Debug = "ignore")]
    handle: AdversaryHandle,

    #[derivative(Debug = "ignore")]
    phantom: std::marker::PhantomData<V>,
    id: NodeId
}

impl <V: Value, T: ClientTransport<V>> AdversaryClientTransport<V, T> {
    pub fn new(id: NodeId, transport: T, handle: AdversaryHandle) -> Self {
        Self {
            transport,
            handle,
            phantom: Default::default(),
            id
        }
    }
}

#[async_trait]
impl <V: Value, T: ClientTransport<V>> ClientTransport<V> for AdversaryClientTransport<V, T> {
    async fn submit_value(&self, node_id: usize, value: V) -> Result<ClientWriteResponse<V>, RaftError> {
        let my_id = self.id;
        self.handle.wrap_delayed_operation(my_id, NodeId::ServerId(node_id), async move {
            self.transport.submit_value(node_id, value).await
        }).await
    }

    async fn request_values(&self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError> {
        let my_id = self.id;
        self.handle.wrap_delayed_operation(my_id, NodeId::ServerId(node_id), async move {
            self.transport.request_values(node_id, from, to).await
        }).await
    }

    async fn force_apply(&self, node_id: usize, value: V) -> Result<ClientForceApplyResponse<V>, RaftError> {
        let my_id = self.id;
        self.handle.wrap_delayed_operation(my_id, NodeId::ServerId(node_id), async move {
            self.transport.force_apply(node_id, value).await
        }).await
    }


    async fn get_sm_event_stream<EventType: Value>(&self, node_id: usize) -> Result<EventStream<EventType>, RaftError> {
        self.transport.get_sm_event_stream(node_id).await
        // let stream = self.transport.get_sm_event_stream<EventType>(node_id).await?;

        // let (res_omission, res_delay) = {
        //     let state = self.handle.inner.read().await;
        //     let (_, res_omission) = state.get_omission_chance(self.id, NodeId::ClientId(node_id));
        //     let res_delay =  state.delay_dist.get(&NodeId::ServerId(node_id)).map(|dist| dist.sample(&mut rand::thread_rng())).unwrap_or(0);
        //     (res_omission, res_delay)
        // };

        // let responder_omission = Bernoulli::new(res_omission).expect("omission chance must be in [0,1]");
    }
}
