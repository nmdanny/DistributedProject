#![allow(unused_imports)]
#![allow(dead_code)]
#[macro_use]
extern crate tracing;

extern crate dist_lib;

use std::rc::Rc;

use dist_lib::{consensus::{node::Node, node_communicator::NodeCommunicator}, grpc::transport::{GRPCConfig, GRPCTransport}};
use dist_lib::consensus::types::*;
use dist_lib::anonymity::logic::*;
use dist_lib::anonymity::anonymous_client::{AnonymousClient, CommitResult, combined_subscriber};
use dist_lib::consensus::logging::*;
use dist_lib::consensus::client::ClientTransport;
use iced::{Application, Settings};
use rand::{distributions::{Standard, Uniform}, prelude::Distribution};
use tracing_futures::Instrument;
use clap::Clap;

mod gui;


#[derive(Clap)]
struct ServerConfig {
    #[clap(short = 'i', long = "id", about = "Server ID. IDs begin from 0 and must be consecutive")]
    node_id: Id,

}

#[derive(Clap)]
struct ClientConfig {
    #[clap(short = 'i', long = "id", about = "Client ID. IDs begin from 0 and must be consecutive")]
    client_id: Id
}

#[derive(Clap)]
enum Mode {
    #[clap(about = "Server mode")]
    Server(ServerConfig),

    #[clap(about = "Client mode")]
    Client(ClientConfig)
}

#[derive(Clap)]
struct CLIConfig {
    #[clap(flatten)]
    config: Config,

    #[clap(subcommand, about = "In which mode to run (server/client)")]
    mode: Mode
}

#[tokio::main]
#[instrument]
async fn main() -> Result<(), anyhow::Error> {
    let ls = tokio::task::LocalSet::new();
    ls.run_until(async move {
        main_in_localset().await?;
        let res: Result<(), anyhow::Error> = Ok(());
        res
    }).await?;
    Ok(())
}


async fn main_in_localset() -> Result<(), anyhow::Error> {
    setup_logging()?;
    let options = CLIConfig::parse();

    let grpc_config = GRPCConfig::default_for_nodes(options.config.num_nodes);
    // let transport: GRPCTransport<AnonymityMessage<String>> = match &options.mode {
    //     Mode::Server(cfg) => { 
    //         GRPCTransport::new(Some(cfg.node_id), grpc_config).await?
    //     }
    //     Mode::Client(_cfg) => {
    //         GRPCTransport::new(None, grpc_config).await?
    //     }
    // };

    let shared_cfg = Rc::new(options.config.clone());
    match &options.mode {
        Mode::Server(cfg) => {
            // let mut node = Node::new(cfg.node_id, options.config.num_nodes, transport.clone());
            // let _comm = NodeCommunicator::from_node(&mut node).await;
            // let sm = AnonymousLogSM::<String, _>::new(shared_cfg, cfg.node_id, transport);
            // node.attach_state_machine(sm);

            // node.run_loop()
            //     .instrument(tracing::info_span!("node-loop", node.id = cfg.node_id))
            //     .await
            //     .unwrap_or_else(|e| error!("Error running node {}: {:?}", cfg.node_id, e));
        }
        Mode::Client(cfg) => {
            // let sm_events = futures::future::join_all((0 .. shared_cfg.num_nodes).map(|node_id| {
            //     let stream = transport.get_sm_event_stream::<NewRound<String>>(node_id);
            //     async move {
            //         stream.await.unwrap()
            //     }
                
            // })).await;

            // let recv = combined_subscriber(sm_events.into_iter());
            // let anonym = AnonymousClient::new(transport, shared_cfg, format!("Client {}", cfg.client_id), recv);
            gui::App::run(Settings::default()).unwrap();
        }
    }

    Ok(())
}