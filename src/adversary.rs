#![feature(type_alias_impl_trait)]
#[macro_use]
extern crate log;

use tokio_compat_02::FutureExt;

use clap::{Clap, Arg};

use futures::prelude::*;
use tokio::net::TcpListener;

use tonic::{transport::Server, Request, Response, Status};
use std::collections::HashMap;
use std::sync::{Arc};
use std::borrow::BorrowMut;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use std::net::SocketAddr;
use futures::lock::Mutex;

tonic::include_proto!("chat");


const BROADCAST_CHANNEL_SIZE : usize = 32;


#[derive(Clap, Debug)]
#[clap()]
pub struct AdversarySettings {
    #[clap(long, about = "probability of dropping a message", default_value = "0.5")]
    drop_prob: f32,

    #[clap(short, long, about = "probability of duplicating a message at some time", default_value = "0.1")]
    duplicate_prob: f32,

    #[clap(short, long, about = "probability of re-ordering packets", default_value = "0")]
    reorder_prob: f32,

    #[clap(short, long, about = "port of adversary", default_value = "6343")]
    adversary_port: u16,

    #[clap(short, long, about = "port of original server", default_value = "8950")]
    server_port: u16
}

type ChatClient = chat_client::ChatClient<tonic::transport::Channel>;

#[derive(Debug)]
pub struct AdversaryState {
    pub settings: AdversarySettings,
    pub client: Mutex<ChatClient>

}

impl AdversaryState {
    pub async fn new(settings: AdversarySettings) -> Result<Self, Box<dyn std::error::Error>> {
        // let client = ChatClient::connect(format!("http://[::]:{}", settings.server_port)).compat().await?;
        let client = ChatClient::connect("http://[::1]:8950").compat().await?;
        Ok(AdversaryState {
            settings, client: Mutex::new(client)
        })
    }
}

impl  AdversaryState {
}

struct AdversaryServerImpl(Arc<AdversaryState>);


#[tonic::async_trait]
impl  chat_server::Chat for AdversaryServerImpl {
    type SubscribeStream = impl Stream<Item = Result<ChatUpdated, Status>>;
    async fn subscribe(&self, request: Request<ConnectRequest>) -> Result<Response<Self::SubscribeStream>, Status> {
        println!("subscribe");
        let mut client = self.0.client.lock().await;
        client.subscribe(request).await
    }


    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<ChatUpdated>, Status> {
        println!("write");
        let mut client = self.0.client.lock().await;
        client.write(request).await
    }


    async fn read(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        println!("read");
        let mut client = self.0.client.lock().await;
        client.read(request).await
    }

}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let settings : AdversarySettings = AdversarySettings::parse();
    let addr = format!("[::]:{}", settings.adversary_port);

    let adversary_state = AdversaryState::new(settings).await?;


    let adversary_imp= AdversaryServerImpl(Arc::new(adversary_state));
    let chat_service = chat_server::ChatServer::new(adversary_imp);

    println!("Starting adversary on {}", addr);
    Server::builder()
        .add_service(chat_service)
        .serve(addr.parse().unwrap())
        .compat()
        .await?;

    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down server");
    Ok(())
}
