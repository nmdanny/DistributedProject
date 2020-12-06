use crate::consensus::transport::Transport;
use crate::consensus::types::*;

use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::BTreeMap;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use anyhow::Error;
use rand::distributions::{Uniform, Bernoulli, Distribution};
use async_trait::async_trait;
use futures::Future;
use std::boxed::Box;
use tokio::time::Duration;
use rand::distributions::uniform::UniformFloat;
use tracing::Instrument;
use std::collections::btree_map::Entry;

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
        let mut state = self.state.write().await;
        state.omission_chance.insert(id, chance);
    }


}


// Since we assume synchrony, to simulate dropping requests/responses, the `send_` methods will
// return an error - this is the same as a timeout duration. In fact, I could send this

#[async_trait(?Send)]
impl <V: Value, T: Transport<V>> Transport<V> for AdversaryTransport<V, T> {
    async fn send_append_entries(&self, to: Id, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, Error> {
        self.adversary_request_response(msg.leader_id, to, async move {
            self.transport.send_append_entries(to, msg).await
        }).await
    }

    async fn send_request_vote(&self, to: Id, msg: RequestVote) -> Result<RequestVoteResponse, Error> {
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
                                           do_request: impl Future<Output = Result<R, Error>>) -> Result<R, Error>
    {
        let mut rng = rand::thread_rng();
        let state = self.state.read().await;
        let req_omission = state.omission_chance.get(&from).copied();
        let req_delay =  state.delay_dist.get(&from).unwrap().sample(&mut rng);
        let res_omission = state.omission_chance.get(&to).copied();
        let res_delay =  state.delay_dist.get(&to).unwrap().sample(&mut rng);
        std::mem::drop(state);

        tokio::time::delay_for(Duration::from_millis(req_delay)).await;

        if req_omission.map(|p| Bernoulli::new(p)
            .expect("omission chance must be in [0,1]")
            .sample(&mut rng)
        ).unwrap_or(false) {
            return Err(anyhow::anyhow!("omission of request from peer {} to peer {}", from, to));
        }
        let res = do_request.await;

        tokio::time::delay_for(Duration::from_millis(res_delay)).await;

        if res_omission.map(|p| Bernoulli::new(p)
            .expect("omission chance must be in [0,1]")
            .sample(&mut rng)
        ).unwrap_or(false) {
            return Err(anyhow::anyhow!("omission of response from peer {} to peer {}", to, from));
        }

        res

    }
}