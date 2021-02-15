
use crate::{consensus::logging::setup_logging, grpc::transport::{GRPCConfig, GRPCTransport}};
use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use crate::consensus::client::{SingleProcessClientTransport, ClientTransport};
use crate::consensus::adversarial_transport::{AdversaryTransport, AdversaryClientTransport};
use crate::consensus::transport::{ThreadTransport};
use crate::anonymity::logic::*;
use crate::anonymity::anonymous_client::{AnonymousClient, CommitResult, combined_subscriber};
use crate::anonymity::callbacks::*;
use futures::{Future, Stream, StreamExt, future::join, future::join_all};
use parking_lot::RwLock;
use rand::distributions::{Distribution, Uniform};
use tokio_stream::StreamMap;
use tracing_futures::Instrument;
use tokio::task;
use tokio::sync::{mpsc, broadcast};
use std::{cell::RefCell, collections::HashMap, hash::Hash, pin::Pin, sync::Arc};
use std::rc::Rc;
use std::borrow::BorrowMut;
use std::cell::Cell;


pub struct Scenario<V: Value + Hash, ST: Transport<AnonymityMessage<V>>, CT: ClientTransport<AnonymityMessage<V>>> {

    pub communicators: Vec<NodeCommunicator<AnonymityMessage<V>>>,
    pub server_transport: AdversaryTransport<AnonymityMessage<V>, ST>,
    pub clients: Vec<AnonymousClient<V>>,

    pub client_transports: Vec<AdversaryClientTransport<AnonymityMessage<V>, CT>>,
    phantom: std::marker::PhantomData<V>

}

pub type SingleProcessScenario<V> = Scenario<V, ThreadTransport<AnonymityMessage<V>>, 
    SingleProcessClientTransport<AnonymityMessage<V>>>;

pub type GRPCScenario<V> = Scenario<V, GRPCTransport<AnonymityMessage<V>>, GRPCTransport<AnonymityMessage<V>>>;


pub async fn setup_grpc_scenario<V: Value + Hash>(config: Config) -> GRPCScenario<V> {

        let num_nodes = config.num_nodes;

        let config = Arc::new(config);

        let grpc_config = GRPCConfig::default_for_nodes(num_nodes);
        let (mut nodes, communicators) = futures::future::join_all(
            (0 .. num_nodes).map(|id| {
                let grpc_config = grpc_config.clone();
                async move {
                    let server_transport = GRPCTransport::new(Some(id), grpc_config).await.unwrap();
                    let server_transport = AdversaryTransport::new(server_transport, num_nodes);
                    let mut node = Node::new(id, num_nodes, server_transport.clone());
                    let comm = NodeCommunicator::from_node(&mut node).await;
                    (node, comm)
                }
            })
            ).await.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();

        let server_transport = nodes[0].transport.clone();

        info!("setup - created nodes");
            

        for node in &mut nodes {
            // used for communicating with other nodes
            let client_transport = node.transport.get_inner().clone();
            let sm = AnonymousLogSM::<V, _>::new(config.clone(), node.id, client_transport);
            node.attach_state_machine(sm);
        }

        for node in nodes {
            let id = node.id;
            tokio::task::spawn_local(async move {
                node.run_loop()
                    .instrument(tracing::info_span!("node-loop", node.id = id))
                    .await
                    .unwrap_or_else(|e| error!("Error running node {}: {:?}", id, e))
            });
        }

        info!("setup - spawned nodes");

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        
        let mut clients = Vec::new();
        let mut client_ts = Vec::new();
        for i in 0 .. config.num_clients {
            let server_transport = GRPCTransport::new(None, grpc_config.clone()).await.unwrap();
            info!("created server transport for client {}", i);
            let client_transport = AdversaryClientTransport::new(server_transport);
            client_ts.push(client_transport.clone());

            let sm_events = futures::future::join_all((0 .. num_nodes).map(|node_id| {
                let stream = client_transport.get_sm_event_stream::<NewRound<V>>(node_id);
                async move {
                    stream.await.unwrap()
                }
                
            })).await;
            info!("obtained server event streams");

            let recv = combined_subscriber(sm_events.into_iter());
            let client = AnonymousClient::new(client_transport, config.clone(), format!("AnonymClient {}", i), recv);
            clients.push(client);
        }

        info!("setup - created clients");

        info!("Starting GRPC scenario");

        Scenario {
            communicators,
            server_transport,
            clients,
            client_transports: client_ts,
            phantom: Default::default()
        }
}

pub async fn setup_test_scenario<V: Value + Hash>(config: Config) -> SingleProcessScenario<V> {
        let num_nodes = config.num_nodes;
        let server_transport = AdversaryTransport::new(ThreadTransport::new(num_nodes), num_nodes);

        let config = Arc::new(config);

        let (mut nodes, communicators) = futures::future::join_all(
            (0 .. num_nodes).map(|id| {
                let mut node = Node::new(id, num_nodes, server_transport.clone());
                async {
                    let comm = NodeCommunicator::from_node(&mut node).await;
                    (node, comm)
                }
            })
            ).await.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();

            

        for node in &mut nodes {
            // used for communicating with other nodes
            let client_transport = SingleProcessClientTransport::new(communicators.clone());
            let sm = AnonymousLogSM::<V, _>::new(config.clone(), node.id, client_transport);
            node.attach_state_machine(sm);
        }

        
        let mut clients = Vec::new();
        let mut client_ts = Vec::new();
        for i in 0 .. config.num_clients {
            let client_transport = AdversaryClientTransport::new(
                SingleProcessClientTransport::new(communicators.clone()));
            client_ts.push(client_transport.clone());

            let sm_events = futures::future::join_all((0 .. num_nodes).map(|node_id| {
                let stream = client_transport.get_sm_event_stream::<NewRound<V>>(node_id);
                async move {
                    stream.await.unwrap()
                }
                
            })).await;

            let recv = combined_subscriber(sm_events.into_iter());
            let client = AnonymousClient::new(client_transport, config.clone(), format!("AnonymClient {}", i), recv);
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
            communicators,
            server_transport,
            clients,
            client_transports: client_ts,
            phantom: Default::default()
        }
}

pub async fn setup_scenario<V: Value + Hash>(config: Config) -> GRPCScenario<V> {
    setup_grpc_scenario(config).await
}


#[tokio::test]
async fn simple_scenario() {
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {

        setup_logging().unwrap();

        let mut scenario = setup_grpc_scenario::<u64>(Config {
            num_nodes: 2,
            num_clients: 2,
            threshold: 2,
            num_channels: 2,
            phase_length: std::time::Duration::from_secs(1),

        }).await;

        let client_a = scenario.clients.pop().unwrap();
        let client_b = scenario.clients.pop().unwrap();

        let handle_a = task::spawn_local(async move {
            let _res = client_a.send_anonymously(1337u64).await.unwrap();
        });

        let handle_b = task::spawn_local(async move {
            let _res = client_b.send_anonymously(7331u64).await.unwrap();
        });

        let _ = join(handle_b, handle_a).await;

    }).await;
}

const VALS_TO_COMMIT: u64 = 20;

#[tokio::test]
async fn many_rounds() {
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {

        setup_logging().unwrap();

        let mut scenario = setup_test_scenario::<String>(Config {
            num_nodes: 5,
            threshold: 3,
            num_clients: 8,
            num_channels: 16,
            phase_length: std::time::Duration::from_millis(200),

        }).await;


        let handles = (0..).zip(scenario.clients.drain(..)).map(|(num, client)| {
            task::spawn_local(async move {
                let mut sends = 0;
                loop {
                    let res = client.send_anonymously(format!("CL{}|V={}", num, sends)).await;
                    match res {
                        Ok(CommitResult { round, channel}) => { 
                            println!("CL{}|V={}  was committed via channel {} at round {}", num, sends, channel, round)}
                        Err(e) => { panic!("Client {} failed to send shares: {}", client.client_name(), e) }
                    }
                    sends += 1;
                    if sends == VALS_TO_COMMIT {
                        return;
                    }
                }
            })
        });

        futures::future::join_all(handles).await;



    }).await;

}


#[tokio::test]
async fn client_crash_only() {
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {

        setup_logging().unwrap();

        let mut scenario = setup_test_scenario::<u64>(Config {
            num_nodes: 3,
            num_clients: 2,
            threshold: 2,
            num_channels: 2,
            phase_length: std::time::Duration::from_secs(1),

        }).await;

        let client_b = scenario.clients.pop().unwrap();
        let client_a = scenario.clients.pop().unwrap();

        let a_transport = Rc::new(scenario.client_transports[0].clone());

        // ensure 'a' always fails sending to node 2
        // technically I am simulating here some kind of omission since node 3 isn't necessarily the last one he sends a message to
        a_transport.set_omission_chance(2, 1.0);

        register_client_send_callback(Box::new(move |_name, _round, node_id| {
            // the following code prevents client 'a' from sending liveness request to simulate the fact he crashed
            if node_id.is_none() {
                a_transport.set_default_req_omission_chance(1.0);
            } else {
                a_transport.set_default_req_omission_chance(0.0);
            }


        }));

        let duration = tokio::time::Duration::from_secs(5);

        let handle_a = tokio::time::timeout(duration, task::spawn_local(async move {
            let _res = client_a.send_anonymously(1337u64).await.unwrap();
        }));


        let handle_b = tokio::time::timeout(duration, task::spawn_local(async move {
            let _res = client_b.send_anonymously(7331u64).await.unwrap();
        }));

        let (res1, res2) = futures::future::join(handle_a, handle_b).await;
        assert!(res1.is_err(), "Client A is crashed and shouldn't be able to submit his value");
        assert!(res2.is_ok(), "Client B is not faulty and should have managed to submit his value");


    }).await;
}


#[tokio::test]
async fn client_crash_server_drop() {
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {

        setup_logging().unwrap();

        let mut scenario = setup_test_scenario::<u64>(Config {
            num_nodes: 3,
            num_clients: 2,
            threshold: 2,
            num_channels: 2,
            phase_length: std::time::Duration::from_secs(1),

        }).await;

        let client_b = scenario.clients.pop().unwrap();
        let client_a = scenario.clients.pop().unwrap();

        let a_transport = Rc::new(scenario.client_transports[0].clone());

        // ensure 'a' always fails sending to node 2
        // technically I am simulating here some kind of omission since node 2 isn't necessarily the last one he sends a message to
        a_transport.set_omission_chance(2, 1.0);

        // make node 0 omit messages with 0.5 probability
        scenario.server_transport.set_omission_chance(0, 0.5).await;

        register_client_send_callback(Box::new(move |_name, _round, node_id| {
            // the following code prevents client 'a' from sending liveness request to simulate the fact he crashed
            if node_id.is_none() {
                a_transport.set_default_req_omission_chance(1.0);
            } else {
                a_transport.set_default_req_omission_chance(0.0);
            }


        }));


        let duration = tokio::time::Duration::from_secs(4);

        let handle_a = tokio::time::timeout(duration, task::spawn_local(async move {
            let _res = client_a.send_anonymously(1337u64).await.unwrap();
        }));


        let handle_b = tokio::time::timeout(duration, task::spawn_local(async move {
            let _res = client_b.send_anonymously(7331u64).await.unwrap();
        }));

        let (res1, res2) = futures::future::join(handle_a, handle_b).await;
        assert!(res1.is_err(), "Client A is crashed and shouldn't be able to submit his value");
        assert!(res2.is_ok(), "Client B is not faulty and should have managed to submit his value");


    }).await;
}


#[tokio::test]
async fn client_crash_server_crash() {
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {

        setup_logging().unwrap();

        let mut scenario = setup_test_scenario::<u64>(Config {
            num_nodes: 3,
            num_clients: 2,
            threshold: 2,
            num_channels: 5,
            phase_length: std::time::Duration::from_secs(1),

        }).await;

        let client_b = scenario.clients.pop().unwrap();
        let client_a = scenario.clients.pop().unwrap();

        let a_transport = Rc::new(scenario.client_transports[0].clone());

        // ensure 'a' always fails sending to node 2
        // technically I am simulating here some kind of omission since node 2 isn't necessarily the last one he sends a message to
        a_transport.set_omission_chance(2, 1.0);

        // make node 0 appear crashed (omit messages to everyone else)
        scenario.server_transport.set_omission_chance(0, 1.0).await;
        scenario.client_transports[0].set_omission_chance(0, 1.0);
        scenario.client_transports[1].set_omission_chance(0, 1.0);

        register_client_send_callback(Box::new(move |_name, _round, node_id| {
            // the following code prevents client 'a' from sending liveness request to simulate the fact he crashed
            if node_id.is_none() {
                a_transport.set_default_req_omission_chance(1.0);
            } else {
                a_transport.set_default_req_omission_chance(0.0);
            }


        }));

        // client 0 shouldn't be able to commit,
        // client 1 should commit since a majority of valid nodes(nodes 1, 2) should see his shares

        let duration = tokio::time::Duration::from_secs(10);

        let handle_a = tokio::time::timeout(duration, task::spawn_local(async move {
            let _res = client_a.send_anonymously(1337u64).await.unwrap();
        }));


        let handle_b = tokio::time::timeout(duration, task::spawn_local(async move {
            let _res = client_b.send_anonymously(7331u64).await.unwrap();
        }));

        info!("Starting to submit items");
        let (res1, res2) = futures::future::join(handle_a, handle_b).await;
        assert!(res1.is_err(), "Client A is crashed and shouldn't be able to submit his value");
        assert!(res2.is_ok(), "Client B is not faulty and should have managed to submit his values to [1,2]");


    }).await;
}