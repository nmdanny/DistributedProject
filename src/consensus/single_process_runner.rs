use dist_lib::consensus::types::*;
use dist_lib::consensus::transport::Transport;

#[macro_use]
extern crate tracing;

#[macro_use]
pub extern crate derivative;

use anyhow::Error;
use std::collections::HashMap;
use async_trait::async_trait;
use tokio::sync::{RwLock, Barrier, mpsc};
use std::sync::Arc;
use tracing_futures::Instrument;
use dist_lib::consensus::node_communicator::NodeCommunicator;
use dist_lib::consensus::client::{Client, SingleProcessClientTransport, ClientTransport};
use dist_lib::consensus::adversarial_transport::{AdversaryTransport, AdversaryClientTransport};
use std::collections::BTreeMap;
use tokio::task::JoinHandle;
use tokio::stream::StreamExt;
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
    async fn send_append_entries(&self, to: usize, msg: AppendEntries<V>) -> Result<AppendEntriesResponse, RaftError> {
        let comm = {
            self.state.read().await.senders.get(&to).unwrap().clone()
        };
        Ok(comm.append_entries(msg).await?)
    }

    #[instrument]
    async fn send_request_vote(&self, to: usize, msg: RequestVote) -> Result<RequestVoteResponse, RaftError> {
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
                    Ok((e, _)) => {
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

    pub async fn spawn_vec_checks(mut self) -> (JoinHandle<()>, mpsc::UnboundedReceiver<LogEntry<V>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::task::spawn_local(async move {
            let mut view = BTreeMap::<usize, LogEntry<V>>::new();
            loop {
                let res = self.notifier.next().await;
                assert!(res.is_some(), "ConsistencyCheck notifier should never be closed");
                let commit_entry = res.unwrap();
                let entry = LogEntry { term: commit_entry.term, value: commit_entry.value.clone()};
                assert!(commit_entry.index == 0 || view.contains_key(&(commit_entry.index - 1)),
                        "Cannot have commit holes");
                match view.entry(commit_entry.index) {
                    Entry::Vacant(e) => {
                        let entry = LogEntry { value: entry.value, term: entry.term};
                        e.insert(entry.clone());
                        let _ = tx.send(entry);

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
        }.instrument(info_span!("ConsistencyChecks_LogChecker")));
        return (handle, rx)
    }

}

const NUM_NODES: usize = 3;
const NUM_CLIENTS: usize = 2;


/// A single threaded instance of many nodes and clients
pub struct Scenario<V: Value> {
    pub communicators: Vec<NodeCommunicator<V>>,
    pub server_transport: AdversaryTransport<V, ThreadTransport<V>>,
    pub clients: Vec<Client<AdversaryClientTransport<V, SingleProcessClientTransport<V>>, V>>,
    pub consistency_join_handle: JoinHandle<()>
}

impl <V: Value> Scenario<V> {
    pub async fn setup(num_nodes: usize, num_clients: usize) -> (Self, mpsc::UnboundedReceiver<LogEntry<V>>)
    {
        let server_transport = AdversaryTransport::new(ThreadTransport::new(num_nodes), num_nodes);
        let (nodes, communicators) = futures::future::join_all(
            (0 .. num_nodes).map(|i|
                NodeCommunicator::create_with_node(i,
                                                   num_nodes,
                                                   server_transport.clone()))
            )
            .await.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
        let mut clients = Vec::new();
        for i in 0 .. num_clients {
            let client_transport = AdversaryClientTransport::new(
                SingleProcessClientTransport::new(communicators.clone()));
            let client = Client::new(format!("Client {}", i), client_transport, num_nodes);
            clients.push(client);
        }

        // spawn all consistency checkers
        // setup a task to ensure all nodes are consistent
        let mut consistency = ConsistencyCheck::new();
        for (id, comm) in (0..).zip(communicators.clone()) {
            consistency.subscribe(id, &comm).await.unwrap();
        }

        let (consistency_join_handle, rx) = consistency.spawn_vec_checks().await;

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

        (Scenario {
            communicators,
            server_transport,
            clients,
            consistency_join_handle
        }, rx)
    }
}


/// A simple scenario where each client sends strings 0, 1, 2 and so on
async fn client_message_loop<T: ClientTransport<String>>(client: &mut Client<T, String>) {
    for i in 0 .. {
        let value = format!("{} value {}", client.client_name, i);
        let (ix, _) = client.submit_value(value).await.expect("Submit value failed");
        info!(">>>>>>>>>>>> client \"{}\" committed {} at index {}", client.client_name, i, ix);
    }
}

pub fn setup_logging() -> Result<(), Error> {
    color_eyre::install().unwrap();
    use tracing_subscriber::FmtSubscriber;
    let subscriber = FmtSubscriber::builder()
        .pretty()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("raft=info".parse()?)
            .add_directive("dist_lib=info".parse()?)
            .add_directive("dist_lib[{vote_granted_too_late}]=off".parse()?)
            .add_directive("dist_lib[{net_err}]=off".parse()?)
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Couldn't set up default tracing subscriber");
    Ok(())
}


/// This tests the 'Random omission of clients and server' scenario
#[tokio::main(max_threads=1)]
pub async fn main() -> Result<(), Error> {
    setup_logging()?;


    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {
        let (setup, _) = Scenario::setup(NUM_NODES, NUM_CLIENTS).await;
        /* note, a 0.5 fail probability includes both failures on the request side or response side
           of the afflicted node/client
           By inclusion/exclusion principle:
           0.5 = P(fail) = P(request fail) + P(response fail) - P(request fail)*P(response faiL)
           Suppose P(request fail) = P(response fail) for simplicity, so the answer is
           P(request fail) = P(response fail) = 0.292
         */
        setup.server_transport.set_omission_chance(0, 0.292).await;
        // setup.server_transport.afflict_delays(NUM_NODES, 10 .. 150).await;

        // spawn clients
        let mut client_handles = Vec::new();
        for mut client in setup.clients {
            let name = client.client_name.clone();
            let handle = tokio::task::spawn_local(async move {

                client.transport.request_omission_chance = 0.292;
                client.transport.response_omission_chance = 0.292;

                client_message_loop(&mut client).await;
            }.instrument(info_span!("Client-loop", name=?name)));
            client_handles.push(handle);
        }
        let clients_finish = futures::future::join_all(client_handles);
        tokio::select! {
            _ = clients_finish => {
            },
            _ = setup.consistency_join_handle => {

            }
        }
    }).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task;
    use futures::StreamExt;

    #[tokio::test]
    pub async fn simple_crash() {
        let ls = task::LocalSet::new();
        setup_logging().unwrap();
        ls.run_until(async move {
            let (scenario, rx) = Scenario::<u32>::setup(3, 1).await;
            let Scenario { mut clients, server_transport, ..} = scenario;
            
            let client_jh = task::spawn_local(async move {
                // submit first value
                clients[0].submit_value(100).await.unwrap();

                // "crash" server 0 by preventing it from sending/receiving messages to other nodes
                server_transport.set_omission_chance(0, 1.0).await;

                // submit second value
                clients[0].submit_value(200).await.unwrap();

            });

            let res = rx.take(2).map(|e| e.value).collect::<Vec<_>>().await;
            assert_eq!(res, vec![100, 200]);

            client_jh.await.unwrap();

        }).await;
        


    }
}