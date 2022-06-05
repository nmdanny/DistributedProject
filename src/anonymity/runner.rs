#![allow(unused_imports)]
#![allow(dead_code)]
#[macro_use]
extern crate tracing;

extern crate dist_lib;

use std::rc::Rc;

use dist_lib::{anonymity::private_messaging::PrivateMessage, consensus::{node::Node, node_communicator::NodeCommunicator, timing::{RaftClientSettings, RaftServerSettings}}, crypto::{AsymEncrypted, PKIBuilder}, grpc::transport::{GRPCConfig, GRPCTransport}};
use dist_lib::consensus::types::*;
use dist_lib::anonymity::logic::*;
use dist_lib::anonymity::anonymous_client::{AnonymousClient, CommitResult, combined_subscriber};
use dist_lib::logging::*;
use dist_lib::consensus::client::ClientTransport;
use gui::AppFlags;
use iced::{Application, Settings};
use rand::{distributions::{Standard, Uniform}, prelude::Distribution};
use tracing_futures::Instrument;
use clap::Parser;
use std::sync::Arc;

mod gui;


#[derive(Parser, Clone)]
struct ServerConfig {
    /// Server ID. IDs begin from 0 and must be consecutive
    #[clap(short = 'i', long = "id")]
    node_id: Id,

    #[clap(flatten)]
    pub raft_server_settings: RaftServerSettings,

    /// A server also contains a raft client for talking with other nodes
    #[clap(flatten)]
    pub raft_client_settings: RaftClientSettings,

}

#[derive(Parser, Clone)]
struct ClientConfig {
    /// Client ID. IDs begin from 0 and must be consecutive
    #[clap(short = 'i', long = "id")]
    client_id: Id,

    #[clap(flatten)]
    pub raft_client_settings: RaftClientSettings,
}

#[derive(Parser, Clone)]
enum Mode {
    /// Server mode
    #[clap()]
    Server(ServerConfig),

    /// Client mode
    #[clap()]
    Client(ClientConfig)
}

#[derive(Parser, Clone)]
struct CLIConfig {
    #[clap(flatten)]
    config: Config,

    /// In which mode to run (server/client)
    #[clap(subcommand)]
    mode: Mode
}

fn main() -> Result<(), anyhow::Error> {
    let options = CLIConfig::parse();
    match &options.mode {
        Mode::Server(server_cfg) => {
            let options = options.clone();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let _guard = setup_logging().unwrap();
                    let ls = tokio::task::LocalSet::new();
                    ls.run_until(async move {
                        run_server(&options.config, server_cfg).await
                    }).await.unwrap();
                });
        }
        Mode::Client(client_cfg) => {
            run_client(&options.config, client_cfg)?;
        }
    }
    Ok(())
}

async fn run_server(config: &Config, server_cfg: &ServerConfig) -> Result<(), anyhow::Error> {
    let grpc_config = GRPCConfig::default_for_nodes(config.num_nodes, !config.insecure);
    let transport: GRPCTransport<AnonymityMessage<AsymEncrypted>> = 
        GRPCTransport::new(Some(server_cfg.node_id), grpc_config, config.timeout).await?;
    let shared_cfg = Arc::new(config.clone());
    let mut node = Node::new(server_cfg.node_id, config.num_nodes, transport.clone(), server_cfg.raft_server_settings.clone());

    let pki = Arc::new(
        PKIBuilder::new(config.num_nodes, config.num_clients)
            .for_server(server_cfg.node_id)
            .build()
    );
    let _comm = NodeCommunicator::from_node(&mut node).await;
    let sm = AnonymousLogSM::<_, _>::new(shared_cfg, pki, server_cfg.node_id, transport, server_cfg.raft_client_settings.clone());
    node.attach_state_machine(sm);

    node.run_loop()
        .instrument(tracing::info_span!("node-loop", node.id = server_cfg.node_id))
        .await
        .unwrap_or_else(|e| error!("Error running node {}: {:?}", server_cfg.node_id, e));
    Ok(())
}

fn run_client(config: &Config, client_cfg: &ClientConfig) -> Result<(), anyhow::Error> {
    let settings = Settings::with_flags(AppFlags {
        config: config.clone(),
        client_id: client_cfg.client_id,
        raft_client_settings: client_cfg.raft_client_settings.clone(),
        logging_guard: None
    });
    gui::App::run(settings).unwrap();
    Ok(())
}