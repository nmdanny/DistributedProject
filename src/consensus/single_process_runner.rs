use dist_lib::consensus::types::*;
use dist_lib::consensus::transport::Transport;

#[macro_use]
extern crate tracing;

#[macro_use]
pub extern crate derivative;

use anyhow::Error;
use std::collections::HashMap;
use async_trait::async_trait;
use tokio::sync::{RwLock, Barrier, broadcast, watch, mpsc};
use std::sync::Arc;
use tracing_futures::Instrument;
use dist_lib::consensus::node_communicator::NodeCommunicator;
use dist_lib::consensus::client::{Client, SingleProcessClientTransport};
use rand::distributions::{Distribution, Uniform};
use tokio::time::Duration;
use dist_lib::consensus::node::Node;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use dist_lib::consensus::adversarial_transport::{AdversaryTransport, AdversaryClientTransport};
use std::collections::BTreeMap;
use tokio::task::JoinHandle;
use tokio::stream::StreamExt;
use tokio::sync::broadcast::RecvError;
use std::collections::btree_map::Entry;

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

/// Used for testing the consistency of entries committed by multiple logs
struct ConsistencyCheck<V: Value> {
    notifier: mpsc::UnboundedReceiver<CommitEntry<V>>,
    notifier_sender: mpsc::UnboundedSender<CommitEntry<V>>
}



impl <V: Value + Eq> ConsistencyCheck<V> {
    pub fn new() -> Self {
        let (notifier_sender, notifier) = mpsc::unbounded_channel();
        ConsistencyCheck {
            notifier,
            notifier_sender
        }
    }

    pub async fn subscribe(&mut self, id: Id, comm: &NodeCommunicator<V>) -> Result<JoinHandle<()>, RaftError>{
        let mut chan = comm.commit_channel().await?;
        let notifier = self.notifier_sender.clone();
        Ok(tokio::task::spawn_local(async move {
            let mut i = 0;
            loop {
                let entry = chan.recv().await;
                match entry {
                    Ok(e) => {
                        if i != e.index {
                            error!("While handling peer {}, expecting CommitEntry at Index {} - given CommitEntry has unexpected index: {:?}",
                                   id, i, e);
                        }
                        notifier.send(e).unwrap();
                        i += 1;
                    },
                    Err(err) => {
                        error!("ConsistencyCheck - commit channel receiver error: {:?}", err);
                        return;
                    }
                }
            }
        }.instrument(info_span!("ConsistencyChecks_CommitChannelReader"))))
    }

    pub async fn spawn_vec_checks(mut self) -> JoinHandle<()> {
        tokio::task::spawn_local(async move {
            let mut view = BTreeMap::<usize, LogEntry<V>>::new();
            loop {
                let res = self.notifier.next().await;
                assert!(res.is_some(), "ConsistencyCheck notifier should never be closed");
                let commit_entry = res.unwrap();
                let entry = LogEntry { term: commit_entry.term, value: commit_entry.value.clone()};
                    match view.entry(commit_entry.index) {
                    Entry::Vacant(e) => {
                        e.insert(LogEntry { value: entry.value, term: entry.term});
                    },
                    Entry::Occupied(o) => {
                        let occ_entry = o.get();
                        if &entry != occ_entry {
                            error!("Inconsistency detected at index {}, previously seen different entry {:?}, now seen entry {:?}",
                                   commit_entry.index, occ_entry, entry)
                        }
                    }
                }
            }
        }.instrument(info_span!("ConsistencyChecks_LogChecker")))
    }

}

const NUM_NODES: usize = 3;

#[tokio::main(max_threads=1)]
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


        // spawn all nodes
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


        // setup a task to ensure all nodes are consistent
        let mut consistency = ConsistencyCheck::new();
        let mut join_handles = Vec::new();
        for (id, comm) in (0..).zip(communicators.iter()) {
            join_handles.push(consistency.subscribe(id, comm).await.unwrap());
        }
        join_handles.push(consistency.spawn_vec_checks().await);

        // setup adversary and begin client messages
        transport.set_omission_chance(0, 0.5).await;

        let mut client_transport = AdversaryClientTransport::new(
            SingleProcessClientTransport::new(communicators)
        );
        client_transport.omission_chance = 0.5;
        let mut client = Client::new(client_transport, NUM_NODES);

        for i in 0 .. {
            let ix = client.submit_value(format!("value {}", i)).await.expect("Submit value failed");
            info!(">>>>>>>>>>>> client committed {}", i);
        }
    }).await;


    Ok(())
}



