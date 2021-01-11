use crate::consensus::logging::setup_logging;
use crate::consensus::types::*;
use crate::consensus::transport::Transport;
use crate::consensus::node::Node;
use crate::consensus::node_communicator::NodeCommunicator;
use crate::consensus::client::{SingleProcessClientTransport, ClientTransport};
use crate::consensus::adversarial_transport::{AdversaryTransport, AdversaryClientTransport};
use crate::consensus::transport::{ThreadTransport};
use crate::anonymity::logic::*;
use crate::anonymity::anonymous_client::AnonymousClient;
use callbacks::*;

use futures::Stream;
use rand::distributions::{Distribution, Uniform};
use tracing_futures::Instrument;
use tokio::sync::{mpsc, broadcast};
use std::hash::Hash;
use std::rc::Rc;
use std::borrow::BorrowMut;

use super::callbacks;

pub fn combined_subscriber<V: Value>(senders: Vec<broadcast::Sender<NewRound<V>>>) -> mpsc::UnboundedReceiver<NewRound<V>> {
    let (tx, rx) = mpsc::unbounded_channel();
    for sender in senders {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut next_round_to_send = 0;
            let mut recv = sender.subscribe();
            while let Ok(round) = recv.recv().await {
                if round.round >= next_round_to_send {
                    next_round_to_send = round.round + 1;
                    if let Err(_e) = tx.send(round) {
                        error!("Couldn't send NewRound to client");
                    }
                }
            }
        });
    }
    rx
}

pub struct Scenario<V: Value + Hash> {
    pub communicators: Vec<NodeCommunicator<AnonymityMessage<V>>>,
    pub server_transport: AdversaryTransport<AnonymityMessage<V>, ThreadTransport<AnonymityMessage<V>>>,
    pub clients: Vec<AnonymousClient<V>>,

    pub client_transports: Vec<AdversaryClientTransport<AnonymityMessage<V>, SingleProcessClientTransport<AnonymityMessage<V>>>>,
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

            

        let mut event_senders = Vec::new();
        for node in &mut nodes {
            // used for communicating with other nodes
            let client_transport = SingleProcessClientTransport::new(communicators.clone());
            let sm = AnonymousLogSM::<V, _>::new(config.clone(), node.id, client_transport);
            let event_sender = node.attach_state_machine(sm);
            event_senders.push(event_sender);
        }

        
        let mut clients = Vec::new();
        let mut client_ts = Vec::new();
        for i in 0 .. config.num_clients {
            let client_transport = AdversaryClientTransport::new(
                SingleProcessClientTransport::new(communicators.clone()));
            client_ts.push(client_transport.clone());
            let recv = combined_subscriber(event_senders.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join;
    use tokio::task;
    use crate::anonymity::callbacks::*;


    #[tokio::test]
    async fn simple_scenario() {
        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {

            setup_logging().unwrap();

            let mut scenario = setup_single_process_anonymity_nodes::<u64>(Config {
                num_nodes: 2,
                num_clients: 2,
                threshold: 2,
                num_channels: 2,
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

        }).await;
    }

    const VALS_TO_COMMIT: u64 = 20;

    #[tokio::test]
    async fn many_rounds() {
        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {

            setup_logging().unwrap();

            let mut scenario = setup_single_process_anonymity_nodes::<u64>(Config {
                num_nodes: 5,
                threshold: 3,
                num_clients: 8,
                num_channels: 8,
                phase_length: std::time::Duration::from_millis(500),

            }).await;


            let handles = (0..).zip(scenario.clients.drain(..)).map(|(num, mut client)| {
                task::spawn_local(async move {
                    let mut i = num * 1000;
                    let mut sends = 0;
                    loop {
                        let res = client.send_anonymously(i).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => { error!("Client {} failed to send shares: {}", client.client_name, e) }
                        }
                        i += 1;
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
    async fn client_crash() {
        let ls = tokio::task::LocalSet::new();
        ls.run_until(async move {

            setup_logging().unwrap();

            let mut scenario = setup_single_process_anonymity_nodes::<u64>(Config {
                num_nodes: 3,
                num_clients: 2,
                threshold: 2,
                num_channels: 2,
                phase_length: std::time::Duration::from_secs(1),

            }).await;

            let mut client_b = scenario.clients.pop().unwrap();
            let mut client_a = scenario.clients.pop().unwrap();

            let a_transport = Rc::new(scenario.client_transports[0].clone());

            // ensure 'a' always fails sending to node 3
            // technically I am simulating here some kind of omission since node 3 isn't necessarily the last one he sends a message to
            a_transport.set_omission_chance(2, 1.0);

            register_client_send_callback(Box::new(move |_name, _round, node_id| {
                let omission_chance = &a_transport.request_omission_chance;

                // the following code prevents client 'a' from sending liveness request to simulate the fact he crashed
                if node_id.is_none() {
                    omission_chance.set(1.0);
                } else {
                    omission_chance.set(0.0);
                }


            }));

            let handle_a = task::spawn_local(async move {
                let _res = client_a.send_anonymously(1337u64).await;
            });

            let handle_b = task::spawn_local(async move {
                let _res = client_b.send_anonymously(7331u64).await;
            });

            let _ = join(handle_b, handle_a).await;

        }).await;
    }


}