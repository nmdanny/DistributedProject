use crate::consensus::transport::Transport;
use crate::consensus::client::ClientTransport;
use crate::consensus::types::*;

use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::BTreeMap;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use anyhow::Error;
use rand::distributions::{Uniform, Bernoulli, Distribution};
use async_trait::async_trait;
use futures::{Future, TryFutureExt};
use std::boxed::Box;
use tokio::time::Duration;
use rand::distributions::uniform::UniformFloat;
use tracing::Instrument;
use std::collections::btree_map::Entry;
use std::cell::{RefCell, Cell};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct AdversaryState {
    /// Maps nodes to a number in [0,1] indicating the probability of a message(request/response)
    /// that's originating from that node of being dropped
    pub omission_chance: BTreeMap<Id, f64>,

    /// Maps nodes to a distribution of delay time in milliseconds
    pub delay_dist: BTreeMap<Id, Uniform<u64>>,

    /// Counts the number of nodes that were ever
    pub afflicted_count: usize
}




#[derive(Derivative, Clone)]
#[derivative(Debug)]
/// Wraps an arbitrary transport, allowing us to simulate omission(and thus, pause/resume) and
/// message delays.
pub struct AdversaryTransport<V: Value, T: Transport<V>> {
    #[derivative(Debug="ignore")]
    transport: T,

    #[derivative(Debug="ignore")]
    state: Arc<RwLock<AdversaryState>>,

    phantom: std::marker::PhantomData<V>,

    node_count: usize,
}

impl <V: Value, T: Transport<V>> AdversaryTransport<V, T> {
    pub fn new(transport: T, node_count: usize) -> Self {
        AdversaryTransport {
            transport, phantom: Default::default(), node_count,
            state: Arc::new(RwLock::new(AdversaryState {
                delay_dist: BTreeMap::new(),
                omission_chance: BTreeMap::new(),
                afflicted_count: 0
            }))
        }
    }

    #[instrument]
    pub async fn set_omission_chance(&self, id: Id, chance: f64) -> bool
    {
        assert!(chance >= 0.0 && chance <= 1.0, "invalid chance");
        let mut state = self.state.write().await;
        let f = state.omission_chance.len();
        let n = self.node_count;

        match state.omission_chance.entry(id) {
            Entry::Occupied(mut e) => { e.insert(chance); },
            Entry::Vacant(e) if (f + 1) * 2 < n => { e.insert(chance); },
            Entry::Vacant(_) => {
                error!("We already have the maximal number of failures (n = {}, f = {}), can't fail another node",
                       n, f);
                return false;
            }
        }
        true
    }

    /// Sets the omission chance for given node ID, even if that would break
    pub async fn force_set_omission_chance(&self, id: Id, chance: f64)
    {
        assert!(chance >= 0.0 && chance <= 1.0, "invalid chance");
        let mut state = self.state.write().await;
        state.omission_chance.insert(id, chance);
    }

    pub async fn afflict_omission_nodes(&self, count: usize, chance: f64)
    {
        assert!(chance >= 0.0 && chance <= 1.0, "invalid chance");
        use rand::seq::SliceRandom;
        let mut candidates = (0 .. self.node_count).collect::<Vec<_>>();
        candidates.shuffle(&mut rand::thread_rng());
        for i in 0 .. count {
            self.set_omission_chance(i, chance).await;
        }
    }

    pub async fn afflict_delays(&self, count: usize, range: std::ops::Range<u64>) {
        use rand::seq::SliceRandom;
        let mut candidates = (0 .. self.node_count).collect::<Vec<_>>();
        candidates.shuffle(&mut rand::thread_rng());
        let mut state = self.state.write().await;
        for i in 0 .. count {
            state.delay_dist.insert(i, Uniform::from(range.clone()));
        }

    }
}


// Since we assume synchrony, to simulate dropping requests/responses, the `send_` methods will
// return an error - this is the same as a timeout duration. In fact, I could send this

#[async_trait(?Send)]
impl <V: Value, T: Transport<V>> Transport<V> for AdversaryTransport<V, T> {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        self.adversary_request_response(msg.leader_id, to, async move {
            self.transport.send_append_entries(to, msg).await
        }).await
    }

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse, RaftError> {
        self.adversary_request_response(msg.candidate_id, to, async move {
            self.transport.send_request_vote(to, msg).await
        }).await
    }

    async fn on_node_communicator_created(&mut self, id: usize, comm: &mut NodeCommunicator<V>) {
        self.transport.on_node_communicator_created(id, comm).await;
        let mut state = self.state.write().await;
        state.delay_dist.entry(id).or_insert(Uniform::new_inclusive(0, 2));
    }

    async fn before_node_loop(&mut self, id: Id) {
        self.transport.before_node_loop(id).await;
    }
}

impl <V: Value, T: Transport<V>> AdversaryTransport<V, T> {
    async fn adversary_request_response<R>(&self, from: Id, to: Id,
                                           do_request: impl Future<Output = Result<R, RaftError>>) -> Result<R, RaftError>
    {
        let mut rng = rand::thread_rng();
        let state = self.state.read().await;
        let req_omission = state.omission_chance.get(&from).copied().unwrap_or(0.0);
        let req_delay =  state.delay_dist.get(&from).unwrap().sample(&mut rng);
        let res_omission = state.omission_chance.get(&to).copied().unwrap_or(0.0);
        let res_delay =  state.delay_dist.get(&to).unwrap().sample(&mut rng);
        std::mem::drop(state);


        let requester_omission = Bernoulli::new(req_omission).expect("omission chance must be in [0,1]");
        let responder_omission = Bernoulli::new(res_omission).expect("omission chance must be in [0,1]");

        tokio::time::sleep(Duration::from_millis(req_delay)).await;

        if requester_omission.sample(&mut rng) || responder_omission.sample(&mut rng) {
            return Err(RaftError::NetworkError(
                anyhow::anyhow!("omission of request from peer {} to peer {}", from, to)));
        }
        let res = do_request.await;

        tokio::time::sleep(Duration::from_millis(res_delay)).await;

        if responder_omission.sample(&mut rng) || responder_omission.sample(&mut rng) {
            return Err(RaftError::NetworkError(
                anyhow::anyhow!("omission of response from peer {} to peer {}", to, from)));
        }

        res

    }
}

/// The client adversary allows setting omission probabilities(between 0 to 1) per node,as well
/// as setting a default omission chance for all nodes.
/// In addition, clones of the adversary client transport retain the same omission probabilities - thus you
/// need to explicitly create a new instance of AdversaryClientTransport if you don't want the adversary to affect everyone
#[derive(Clone)]
pub struct AdversaryClientTransport<V: Value, T: ClientTransport<V>>
{
    transport: T,
    pub omission_chance: Rc<RefCell<BTreeMap<Id, f64>>>,
    pub request_omission_chance: Rc<Cell<f64>>,
    pub response_omission_chance: Rc<Cell<f64>>,
    phantom: std::marker::PhantomData<V>,
}

impl <V: Value, T: ClientTransport<V>> AdversaryClientTransport<V, T> {
    pub fn new(transport: T) -> Self {
        AdversaryClientTransport {
            transport,
            omission_chance: Rc::new(RefCell::new(BTreeMap::new())),
            request_omission_chance: Rc::new(Cell::new(0.0)),
            response_omission_chance: Rc::new(Cell::new(0.0)),
            phantom: Default::default()
        }
    }

    pub fn set_omission_chance(&self, id: Id, chance: f64) {
        *self.omission_chance.borrow_mut().entry(id).or_default() = chance;
    }

}

async fn adversary_request_response<R>(req_omission_chance: f64,
                                       res_omission_chance: f64,
                                       to: Id,
                                       do_request: impl Future<Output = Result<R, RaftError>>) -> Result<R, RaftError>
{
    let mut rng = rand::thread_rng();

    let req_ber =  Bernoulli::new(req_omission_chance).expect("Invalid omission chance");
    let res_ber =  Bernoulli::new(res_omission_chance).expect("Invalid omission chance");


    if req_ber.sample(&mut rng) {
        return Err(anyhow::anyhow!("omission of client request to node {}", to))
            .map_err(RaftError::NetworkError)
    }
    let res = do_request.await;

    if res_ber.sample(&mut rng) {
        return Err(anyhow::anyhow!("omission of node {} response to client", to))
            .map_err(RaftError::NetworkError);
    }

    res
}

#[async_trait(?Send)]
impl <V: Value, T: ClientTransport<V>> ClientTransport<V> for AdversaryClientTransport<V, T> {
    async fn submit_value(&self, node_id: usize, value: V) -> Result<ClientWriteResponse<V>, RaftError> {
        let req_chance = self.omission_chance.borrow().get(&node_id).cloned().unwrap_or(self.request_omission_chance.get());
        let res_chance = self.omission_chance.borrow().get(&node_id).cloned().unwrap_or(self.response_omission_chance.get());
        adversary_request_response(req_chance,
                                   res_chance,
                                   node_id, async move {
            self.transport.submit_value(node_id, value).await
        }).await
    }

    async fn request_values(&self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError> {
        let req_chance = self.omission_chance.borrow().get(&node_id).cloned().unwrap_or(self.request_omission_chance.get());
        let res_chance = self.omission_chance.borrow().get(&node_id).cloned().unwrap_or(self.response_omission_chance.get());
        adversary_request_response(req_chance,
                                   res_chance,
                                   node_id, async move {
            self.transport.request_values(node_id, from, to).await
        }).await
    }

    async fn force_apply(&self, node_id: usize, value: V) -> Result<ClientForceApplyResponse<V>, RaftError> {
        let req_chance = self.omission_chance.borrow().get(&node_id).cloned().unwrap_or(self.request_omission_chance.get());
        let res_chance = self.omission_chance.borrow().get(&node_id).cloned().unwrap_or(self.response_omission_chance.get());
        adversary_request_response(req_chance,
                                   res_chance,
                                   node_id, async move {
            self.transport.force_apply(node_id, value).await
        }).await
    }
}
