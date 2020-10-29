#![feature(type_alias_impl_trait)]
#[macro_use]
extern crate log;

use tokio_compat_02::FutureExt;

use clap::{Clap, Arg};

use futures::prelude::*;
use tokio::net::TcpListener;

use tonic::{transport::Server, Request, Response, Status, IntoRequest};
use std::collections::HashMap;
use std::sync::{Arc};
use std::borrow::BorrowMut;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use std::net::SocketAddr;
use futures::lock::Mutex;
use tokio::time::Duration;
use rand::distributions::Distribution;
use std::fmt::Debug;

tonic::include_proto!("chat");


const BROADCAST_CHANNEL_SIZE : usize = 32;


#[derive(Clap, Debug)]
#[clap()]
pub struct AdversarySettings {
    #[clap(long, about = "probability of dropping a message", default_value = "0.5")]
    drop_prob: f64,

    #[clap(long, about = "probability of duplicating a message at some time", default_value = "0.3")]
    duplicate_prob: f64,

    #[clap(long, about = "probability of re-ordering packets", default_value = "0.3")]
    reorder_prob: f64,

    #[clap(long, about = "port of adversary", default_value = "6343")]
    adversary_port: u16,

    #[clap(long, about = "port of original server", default_value = "8950")]
    server_port: u16,

    #[clap(long, about = "minimum delay(in ms) until re-ordered/duplicated messages are sent", default_value = "100")]
    min_delay_ms: u64,

    #[clap(long, about = "maximum delay(in ms) until re-ordered/duplicated messages are sent", default_value = "3000")]
    max_delay_ms: u64,

    #[clap(long, about = "minimum delay(in ms) until re-ordered messages are sent", default_value = "100")]
    min_retransmit_delay: u64
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

async fn sleep(time_ms: u64) {
    // tokio::spawn(tokio::time::sleep(Duration::from_millis(time_ms))).compat().await;
    // tokio::time::sleep(Duration::from_millis(time_ms)).compat().await;

    tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(time_ms));
    }).compat().await;
}

impl  AdversaryState {
    async fn maybe_drop_request<R: Debug>(&self, r: &R) {
        let barnaval_ha_naval = rand::distributions::Bernoulli::new(self.settings.drop_prob).unwrap();
        if barnaval_ha_naval.sample(&mut rand::thread_rng()) {
            info!("Dropping request {:?}", r);
            // we can't really drop responses in this library, so just set a very high delay
            sleep(3600000).await;
        }
    }

    async fn maybe_delay<R: Debug>(&self, r: &R) {
        let barnaval_ha_naval = rand::distributions::Bernoulli::new(self.settings.reorder_prob).unwrap();
        if barnaval_ha_naval.sample(&mut rand::thread_rng()) {
            let uni  = rand::distributions::Uniform::new(self.settings.min_delay_ms, self.settings.max_delay_ms);
            let ms = uni.sample(&mut rand::thread_rng());
            info!("Delaying response {:?} by {} ms", r, ms);
            sleep(ms).await;
        }
    }

    async fn maybe_duplicate_request<R: Clone + Debug>(&self, req: &R) -> Option<R> {
        let barnaval = rand::distributions::Bernoulli::new(self.settings.duplicate_prob).unwrap();
        if barnaval.sample(&mut rand::thread_rng()) {
            info!("Making duplicate of {:?}", req);
            return Some(req.clone())
        }
        None
    }
}

struct AdversaryServerImpl(Arc<AdversaryState>);


#[tonic::async_trait]
impl  chat_server::Chat for AdversaryServerImpl {
    type SubscribeStream = impl Stream<Item = Result<ChatUpdated, Status>>;
    async fn subscribe(&self, request: Request<ConnectRequest>) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut client = self.0.client.lock().await;
        info!("Adversary detected connection, peer id: {}", request.get_ref().client_id);
        client.subscribe(request).await
    }


    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<ChatUpdated>, Status> {
        let mut client = self.0.client.lock().await;
        self.0.maybe_drop_request(request.get_ref()).await;
        self.0.maybe_delay(request.get_ref()).await;

        let rr = request.get_ref();
        if let Some(dup_req) = self.0.maybe_duplicate_request(rr).await {
            let dup_req = Request::new(dup_req);
            let mut client_dup = client.clone();
            tokio::select! {
                r1 = client_dup.write(dup_req) => r1,
                r2 = client.write(request) => r2
            }
        } else {
            client.write(request).await
        }

    }


    async fn read(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
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
