
use crate::{grpc::transport::{GRPCConfig, GRPCTransport}, logging::{profiler, setup_logging}};
use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use crate::consensus::client::{SingleProcessClientTransport, ClientTransport};
use crate::consensus::adversarial_transport::{AdversaryHandle, NodeId, AdversaryTransport, AdversaryClientTransport};
use crate::consensus::transport::{ThreadTransport};
use crate::anonymity::logic::*;
use crate::anonymity::anonymous_client::{AnonymousClient, CommitResult, combined_subscriber};
use crate::crypto::PKIBuilder;
use crossbeam::epoch::Pointable;
use futures::{Future, Stream, StreamExt, future::join, future::join_all, stream::{FuturesOrdered, FuturesUnordered}};
use parking_lot::{Mutex, RwLock};
use rand::distributions::{Distribution, Uniform};
use tokio_stream::StreamMap;
use tracing_futures::Instrument;
use tokio::task;
use tokio::sync::{mpsc, broadcast};
use std::{cell::RefCell, collections::HashMap, hash::Hash, pin::Pin, sync::Arc};
use std::rc::Rc;
use std::borrow::BorrowMut;
use std::cell::Cell;


pub struct Scenario<V: Value + Hash>
{
    pub communicators: Vec<NodeCommunicator<AnonymityMessage<V>>>,
    pub clients: Vec<AnonymousClient<V>>,
    phantom: std::marker::PhantomData<V>,
    pub adversary: AdversaryHandle,
    handles: Vec<task::JoinHandle<()>>

}

pub async fn setup_grpc_scenario<V: Value + Hash>(config: Config) -> Scenario<V> {

        let num_nodes = config.num_nodes;

        let config = Arc::new(config);
        let grpc_config = GRPCConfig::default_for_nodes(num_nodes, !config.insecure);
        let mut pki_builder = PKIBuilder::new(config.num_nodes, config.num_clients);
        let adversary = AdversaryHandle::new();

        let rt = tokio::runtime::Handle::current();
        let _e = rt.enter();
        let mut handles = Vec::new();
        for node_id in 0 .. num_nodes {
            let config = config.clone();
            let grpc_config = grpc_config.clone();
            let pki = Arc::new(
                    pki_builder.for_server(node_id).build()
            );
            let adversary = adversary.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                let _ = rt.enter();
                futures::executor::block_on(async move {
                    info!("Setting up node {}", node_id);
                    let ls = tokio::task::LocalSet::new();
                    ls.run_until(async move {
                    let server_transport = GRPCTransport::new(Some(node_id), grpc_config, config.timeout).await.unwrap();
                    let client_transport = server_transport.clone();

                    let server_transport = adversary.wrap_server_transport(node_id, server_transport);
                    let client_transport = adversary.wrap_client_transport(NodeId::ServerId(node_id), client_transport);

                    let mut node = Node::new(node_id, num_nodes, server_transport);
                    let _comm = NodeCommunicator::from_node(&mut node).await;
                    let sm = AnonymousLogSM::<V, _>::new(config.clone(), pki, node.id, client_transport);
                    node.attach_state_machine(sm);
                    node.run_loop()
                        .instrument(tracing::trace_span!("node-loop", node.id = node_id))
                        .await
                        .unwrap_or_else(|e| panic!("Error running node {}: {:?}", node_id, e))
                    }).await;
                });
            });
            handles.push(handle)
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        
        let mut clients = Vec::new();
        for i in 0 .. config.num_clients {
            info!("created server transport for client {}", i);
            let client_transport = adversary.wrap_client_transport(NodeId::ClientId(i), 
                GRPCTransport::new(None, grpc_config.clone(), config.timeout).await.unwrap());

            let sm_events = futures::future::join_all((0 .. num_nodes).map(|node_id| {
                let stream = client_transport.get_sm_event_stream::<NewRound<V>>(node_id);
                async move {
                    stream.await.unwrap()
                }
                
            })).await;
            info!("obtained server event streams");

            let recv = combined_subscriber(sm_events.into_iter());
            let pki = Arc::new(pki_builder.for_client(i).build());
            let client = AnonymousClient::new(client_transport, config.clone(), pki, i, recv);
            clients.push(client);
        }

        info!("setup - created clients");

        info!("Starting GRPC scenario");

        Scenario {
            communicators: Vec::new(),
            clients,
            phantom: Default::default(),
            adversary,
            handles
        }
}

pub async fn setup_test_scenario<V: Value + Hash>(config: Config) -> Scenario<V> {
        let num_nodes = config.num_nodes;
        let server_transport = ThreadTransport::new(num_nodes, config.timeout);
        let adversary = AdversaryHandle::new();

        let config = Arc::new(config);

        let (mut nodes, communicators) = futures::future::join_all(
            (0 .. num_nodes).map(|id| {
                let adversary = adversary.clone();
                let server_transport = adversary.wrap_server_transport(id, server_transport.clone());
                let mut node = Node::new(id, num_nodes, server_transport);
                async {
                    let comm = NodeCommunicator::from_node(&mut node).await;
                    (node, comm)
                }
            })
            ).await.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();

            
        let mut pki_builder = PKIBuilder::new(config.num_nodes, config.num_clients);

        for node in &mut nodes {
            // used for communicating with other nodes
            let client_transport = adversary.wrap_client_transport(NodeId::ServerId(node.id), 
                SingleProcessClientTransport::new(communicators.clone(), config.timeout));
            let pki = Arc::new(pki_builder.for_server(node.id).build());
            let sm = AnonymousLogSM::<V, _>::new(config.clone(), pki, node.id, client_transport);
            node.attach_state_machine(sm);
        }

        
        let mut clients = Vec::new();
        let mut client_ts = Vec::new();
        for i in 0 .. config.num_clients {
            
            let client_transport = adversary.wrap_client_transport(NodeId::ClientId(i),
                SingleProcessClientTransport::new(communicators.clone(), config.timeout));
            client_ts.push(client_transport.clone());

            let sm_events = futures::future::join_all((0 .. num_nodes).map(|node_id| {
                let stream = client_transport.get_sm_event_stream::<NewRound<V>>(node_id);
                async move {
                    stream.await.unwrap()
                }
                
            })).await;

            let recv = combined_subscriber(sm_events.into_iter());
            let pki = Arc::new(pki_builder.for_client(i).build());
            let client = AnonymousClient::new(client_transport, config.clone(), pki, i, recv);
            clients.push(client);
        }

        
        // spawn all nodes
        let mut handles = Vec::new();
        for node in nodes.into_iter() {
            let id = node.id;
            let handle = tokio::task::spawn_local(async move {
                node.run_loop()
                    .instrument(tracing::info_span!("node-loop", node.id = id))
                    .await
                    .unwrap_or_else(|e| panic!("Error running node {}: {:?}", id, e))
            });
            handles.push(handle);
        }

        Scenario {
            communicators,
            clients,
            phantom: Default::default(),
            adversary,
            handles
        }
}

pub async fn setup_scenario<V: Value + Hash>(config: Config) -> Scenario<V> {
    setup_grpc_scenario(config).await
}


#[tokio::test]
async fn simple_scenario() {
    let ls = tokio::task::LocalSet::new();
    let _guard = setup_logging().unwrap();
    let _profiler = profiler("simple_scenario", 1000);
    ls.run_until(async move {


        let mut scenario = setup_test_scenario::<u64>(Config {
            num_nodes: 2,
            num_clients: 2,
            threshold: 2,
            num_channels: 2,
            phase_length: std::time::Duration::from_secs(1),
            timeout: std::time::Duration::from_secs(3),
            insecure: true

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
    let _guard = setup_logging().unwrap();
    let _profiler = profiler("many_rounds", 100);
    ls.run_until(async move {
        let mut scenario = setup_test_scenario::<String>(Config {
            num_nodes: 5,
            threshold: 3,
            num_clients: 25,
            num_channels: 100,
            phase_length: std::time::Duration::from_millis(5000),
            timeout: std::time::Duration::from_millis(8000),
            insecure: true
        }).await;


        let mut handles = (0..).zip(scenario.clients.drain(..)).map(|(num, client)| {
            tokio::spawn(async move {
                let mut sends = (0 .. VALS_TO_COMMIT)
                    .map(|i| client.send_anonymously(format!("CL{}|V={}", num, i)))
                    .collect::<futures::stream::FuturesUnordered<_>>();
                let mut i = 0;
                while let Some(res) = sends.next().await {
                    match res {
                        Ok(CommitResult { round, channel}) => { 
                            info!("CL{}|V={}  was committed via channel {} at round {}", num, i, channel, round);
                            println!("CL{}|V={}  was committed via channel {} at round {}", num, i, channel, round);
                        }
                        Err(e) => { 
                            panic!("CL{}|V={} failed to be committed: {:?}", client.client_id(), i, e);
                        }
                    }
                    i += 1;
                }
            })
        }).collect::<Vec<_>>();

        loop {
            if handles.is_empty() {
                break
            }
            let (res, index, rest) = futures::future::select_all(handles).await;
            if res.is_err() {
                panic!("CL{} failed: {:?}", index, res.unwrap_err());
            } else {
                println!("CL{} finished", index);
            }
            handles = rest;
        }




    }).await;

}


#[tokio::test]
async fn client_crash_only() {
    let ls = tokio::task::LocalSet::new();
    // let _guard = setup_logging().unwrap();
    ls.run_until(async move {
        let mut scenario = setup_test_scenario::<u64>(Config {
            num_nodes: 3,
            num_clients: 2,
            threshold: 2,
            num_channels: 2,
            phase_length: std::time::Duration::from_secs(1),
            timeout: std::time::Duration::from_secs(5),
            insecure: true
        }).await;

        let client_b = scenario.clients.pop().unwrap();
        let client_a = scenario.clients.pop().unwrap();

        // ensure 'a' always fails
        scenario.adversary.set_client_omission_chance(0, 1.0).await;

        let duration = tokio::time::Duration::from_secs(10);

        let handle_a = tokio::time::timeout(duration, tokio::spawn(async move {
            let _res = loop {
                if let Ok(res) = client_a.send_anonymously(1337u64).await {
                    break res
                }
            };
        }));


        let handle_b = tokio::time::timeout(duration, tokio::spawn(async move {
            let _res = loop {
                if let Ok(res) = client_b.send_anonymously(7331u64).await {
                    break res
                }
            };
        }));

        let (res1, res2) = futures::future::join(handle_a, handle_b).await;
        assert!(res1.is_err(), "Client A is crashed and shouldn't be able to submit his value");
        assert!(res2.is_ok(), "Client B is not faulty and should have managed to submit his value");


    }).await;
}


#[tokio::test]
async fn client_omission_server_crash() {
    let ls = tokio::task::LocalSet::new();
    let _guard = setup_logging().unwrap();
    ls.run_until(async move {
        let mut scenario = setup_test_scenario::<u64>(Config {
            num_nodes: 3,
            num_clients: 2,
            threshold: 2,
            num_channels: 2,
            phase_length: std::time::Duration::from_secs(1),
            timeout: std::time::Duration::from_secs(5),
            insecure: true
        }).await;

        let client_b = scenario.clients.pop().unwrap();
        let client_a = scenario.clients.pop().unwrap();


        // ensure 'a' always fails sending to node 2
        scenario.adversary.set_pair_omission_chance(NodeId::ClientId(0), NodeId::ServerId(1), 1.0, true).await;

        // crash node 1
        scenario.adversary.set_server_omission_chance(0, 1.0).await;



        let duration = tokio::time::Duration::from_secs(10);

        let handle_a = tokio::time::timeout(duration, task::spawn(async move {
            let _res = loop {
                if let Ok(res) = client_a.send_anonymously(1337u64).await {
                    break res
                }
            };
        }));


        let handle_b = tokio::time::timeout(duration, task::spawn(async move {
            let _res = loop {
                if let Ok(res) = client_b.send_anonymously(7331u64).await {
                    break res
                }
            };
        }));

        let (res1, res2) = futures::future::join(handle_a, handle_b).await;
        assert!(res1.is_ok(), "Client A's missing share would've reached node 2 via node 3(encrypted)");
        assert!(res2.is_ok(), "Client B is not faulty and should have managed to submit his value");


    }).await;
}

