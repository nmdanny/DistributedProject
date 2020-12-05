use dist_lib::consensus::types::*;
use dist_lib::consensus::transport::Transport;

#[macro_use]
extern crate tracing;

#[macro_use]
pub extern crate derivative;

use anyhow::Error;
use std::collections::HashMap;
use async_trait::async_trait;
use tokio::sync::{RwLock, Barrier};
use std::sync::Arc;
use tracing_futures::Instrument;
use dist_lib::consensus::node_communicator::NodeCommunicator;
use rand::distributions::{Distribution, Uniform};
use tokio::time::Duration;
use dist_lib::consensus::node::Node;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use dist_lib::consensus::adversarial_transport::AdversaryTransport;

struct ThreadTransportState<V: Value>
{
    senders: HashMap<Id, NodeCommunicator<V>>,

}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct ThreadTransport<V: Value>
{
    #[derivative(Debug="ignore")]
    state: Arc<RwLock<ThreadTransportState<V>>>,

    #[derivative(Debug="ignore")]
    barrier: Arc<Barrier>
}

impl <V: Value> ThreadTransport<V> {
    pub fn new(expected_num_nodes: usize) -> Self {
        let barrier = Arc::new(Barrier::new(expected_num_nodes));
        let state = ThreadTransportState {
            senders: Default::default()
        };
        ThreadTransport {
            state: Arc::new(RwLock::new(state)),
            barrier
        }
    }
}

#[async_trait(?Send)]
impl <V: Value> Transport<V> for ThreadTransport<V> {
    #[instrument]
    async fn send_append_entries(&self, to: usize, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, Error> {
        let comm = {
            self.state.read().await.senders.get(&to).unwrap().clone()
        };
        Ok(comm.append_entries(msg).await?)
    }

    #[instrument]
    async fn send_request_vote(&self, to: usize, msg: RequestVote) -> Result<RequestVoteResponse, Error> {
        let comm = {
            self.state.read().await.senders.get(&to).unwrap().clone()
        };
        Ok(comm.request_vote(msg).await?)
    }

    async fn on_node_communicator_created(&mut self, id: Id, comm: &mut NodeCommunicator<V>) {
        let mut state = self.state.write().await;

        let _prev = state.senders.insert(id, comm.clone());
        assert!(_prev.is_none(), "Can't insert same node twice");
    }

    async fn before_node_loop(&mut self, _id: Id) {
        self.barrier.wait().await;
    }
}

const NUM_NODES: usize = 3;

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    // set up logging
    color_eyre::install().unwrap();
    use tracing_subscriber::FmtSubscriber;
    let subscriber = FmtSubscriber::builder()
        .pretty()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("dist_lib=debug".parse()?)
            .add_directive("raft=debug".parse()?)
        )
    .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Couldn't set up default tracing subscriber");


    // initializing all nodes and their communicators
    let transport = AdversaryTransport::new(
        ThreadTransport::<String>::new(NUM_NODES),
        NUM_NODES);
    let node_and_comms_fut = futures::future::join_all(
        (0 .. NUM_NODES).map(|i| {
            NodeCommunicator::create_with_node(i, NUM_NODES, transport.clone())
        })
    );

    let (nodes, communicators): (Vec<_>, Vec<_>) = node_and_comms_fut.await
        .into_iter().unzip();


    let mut handles = Vec::new();
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {
        for node in nodes.into_iter() {
            let id = node.id;
            let handle = tokio::task::spawn_local(async move {
                node.run_loop()
                    .instrument(tracing::info_span!("node-loop", node.id = id))
                    .await
                    .unwrap_or_else(|e| error!("Error running node {}: {:?}", id, e))
            });
            handles.push(handle);
        }

        let mut rng = rand::thread_rng();
        let posible_leaders = Uniform::from(0 .. NUM_NODES);
        let submit_delay_ms = Uniform::from(0 .. 500);
        let mut leader  = 0;
        let mut i = 10000;
        loop {
            tokio::time::delay_for(Duration::from_millis(submit_delay_ms.sample(&mut rng))).await;
            info!(">>>>> submitting value {} to peer {}", i, leader);
            let res = communicators[leader].submit_value(ClientWriteRequest { value: format!("val {}", i)}).await;
            match res {
                Ok(ClientWriteResponse::NotALeader { leader_id: Some(new_leader)}) => {
                    warn!(">>>>> got new leader: {}", new_leader);
                    leader = new_leader
                },
                Ok(ClientWriteResponse::NotALeader { leader_id: None}) => {
                    leader = posible_leaders.sample(&mut rng);
                    warn!(">>>>> leader is unknown, guessing it is {}", leader);
                },
                Ok(ClientWriteResponse::Ok { commit_index }) => {
                    info!(">>>>> submitted {} to {}, committed at {}", i, leader, commit_index);
                    i += 1;

                },
                Err(e) => {
                    error!("Raft error while submitting value: {:?}", e);
                    leader = posible_leaders.sample(&mut rng);
                }
            }
        }
    }).await;


    Ok(())
}



