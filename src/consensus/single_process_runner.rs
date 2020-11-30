use dist_lib::consensus::types::*;
use dist_lib::consensus::node::*;
use dist_lib::consensus::transport::Transport;

#[macro_use]
extern crate tracing;

use anyhow::Error;
use std::collections::HashMap;
use async_trait::async_trait;
use std::sync::Arc;
use tracing_futures::Instrument;
use dist_lib::consensus::node_communicator::NodeCommunicator;

#[derive(Debug, Clone)]
pub struct ThreadTransport<V: Value>
{
    senders: Arc<HashMap<Id, NodeCommunicator<V>>>
}

impl <V: Value> ThreadTransport<V> {
    pub fn new() -> Self {
        ThreadTransport {
            senders: Default::default()
        }
    }
}

#[async_trait]
impl <V: Value> Transport<V> for ThreadTransport<V> {
    #[instrument]
    async fn send_append_entries(&self, to: usize, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, Error> {
        Ok(self.senders.get(&to).unwrap().clone().append_entries(msg).await?)
    }

    #[instrument]
    async fn send_request_vote(&self, to: usize, msg: RequestVote) -> Result<RequestVoteResponse, Error> {
        Ok(self.senders.get(&to).unwrap().clone().request_vote(msg).await?)
    }
}

const NUM_NODES: usize = 7;

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    // set up logging
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
    let mut nodes = Vec::new();
    let mut communicators = Vec::new();
    for i in 0 .. NUM_NODES {
        let dummy_trans = ThreadTransport::<String>::new();
        let (node, comm) = NodeCommunicator::create_with_node(i, NUM_NODES, dummy_trans);
        nodes.push(node);
        communicators.push(comm);
    }

    // setting up the real transport
    let mut senders = HashMap::new();
    for (node, comm) in nodes.iter().zip(communicators) {
        senders.insert(node.id, comm.clone());

    }
    let thread_transport = ThreadTransport { senders: Arc::new(senders) };

    for node in nodes.iter_mut() {
        node.transport = thread_transport.clone();
    }

    let mut handles = Vec::new();
    for node in nodes.into_iter() {
        let id = node.id;
        let handle = tokio::spawn(async move {
            node.run_loop()
                .instrument(tracing::info_span!("node-loop", node.id = id))
                .await
                .unwrap_or_else(|e| error!("Error running node {}: {:?}", id, e))
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    Ok(())
}



