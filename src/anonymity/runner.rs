use crate::consensus::logging::setup_logging;
use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use crate::consensus::client::{SingleProcessClientTransport, ClientTransport};
use crate::consensus::adversarial_transport::{AdversaryTransport, AdversaryClientTransport};
use crate::consensus::transport::{ThreadTransport};
use crate::consensus::state_machine::StateMachine;
use crate::anonymity::logic::*;
use crate::anonymity::anonymous_client::AnonymousClient;
use rand::distributions::{Distribution, Uniform};

use tracing_futures::Instrument;
use std::hash::Hash;
use std::rc::Rc;

type ST<V: Value, S: StateMachine<V>> = AdversaryTransport<AnonymityMessage<V>, S,
    ThreadTransport<AnonymityMessage<V>, S>>;

type CV<V: Value, S: StateMachine<V>> = AdversaryClientTransport<AnonymityMessage<V>,
    SingleProcessClientTransport<AnonymityMessage<V>, S>
    >;


type S<V: Value> = AnonymousLogSM<V, CV<V, S<V>>>;

pub struct Scenario<V: Value + Hash> {
    pub communicators: Vec<NodeCommunicator<AnonymityMessage<V>, S>>,
    pub server_transport: ST<V, S>,
    pub clients: Vec<AnonymousClient<V, CV<V,S>>>,
    phantom: std::marker::PhantomData<V>

}

pub async fn setup_single_process_anonymity_nodes<V: Value + Hash>(config: Config) -> Scenario<V> {

        let num_nodes = config.num_nodes;
        let server_transport = AdversaryTransport::new(ThreadTransport::new(num_nodes), num_nodes);

        let config = Rc::new(config);

        let (mut nodes, communicators) = futures::future::join_all(
            (0 .. config.num_nodes).map(|id| {
                let mut node = Node::new(id, config.num_nodes, server_transport.clone());
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
        for i in 0 .. config.num_clients {
            let client_transport = AdversaryClientTransport::new(
                SingleProcessClientTransport::new(communicators.clone()));
            let client = AnonymousClient::new(client_transport, config.clone(), format!("AnonymClient {}", i));
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
            phantom: Default::default()
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join;
    use tokio::task;

    #[tokio::test]
    async fn simple_scenario() {
        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {

            setup_logging().unwrap();

            let mut scenario = setup_single_process_anonymity_nodes::<u64>(Config {
                num_nodes: 2,
                num_clients: 2,
                threshold: 2,
                num_channels: 3,
                phase_length: std::time::Duration::from_secs(1),

            }).await;

            let mut client_a = scenario.clients.pop().unwrap();
            let mut client_b = scenario.clients.pop().unwrap();

            let handle_a = task::spawn_local(async move {
                let _res = client_a.send_anonymously(1337u64).await;
            });

            let handle_b = task::spawn_local(async move {
                let _res = client_b.send_anonymously(7331u64).await;
            });

            let _ = join(handle_b, handle_a).await;

            loop {
                tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;
            }

        }).await;
        println!("simple_scenario done");
    }

    #[tokio::test]
    async fn with_omissions() {
        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {

            setup_logging().unwrap();

            let mut scenario = setup_single_process_anonymity_nodes::<u64>(Config {
                num_nodes: 5,
                num_clients: 2,
                threshold: 3,
                num_channels: 50,
                phase_length: std::time::Duration::from_secs(2),

            }).await;

            let handles = (0..).zip(scenario.clients.drain(..)).map(|(num, mut client)| {
                task::spawn_local(async move {
                    let mut i = num * 1000;
                    let delay_dist = Uniform::from(1000 .. 1001);
                    loop {
                        let res = client.send_anonymously(i).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => { error!("Client {} failed to send shares: {}", client.client_name, e) }
                        }
                        i += 1;
                        let delay = delay_dist.sample(&mut rand::thread_rng());
                        tokio::time::delay_for(tokio::time::Duration::from_millis(delay)).await;
                    }
                })
            });

            futures::future::join_all(handles).await;



        }).await;
        println!("simple_scenario done");

    }

}