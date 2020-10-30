use clap::{Clap, Arg};

use futures::prelude::*;
use tokio::net::TcpListener;

use tonic::{transport::Server, Request, Response, Status, IntoRequest};
use std::collections::HashMap;
use std::sync::{Arc};
use std::borrow::BorrowMut;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use std::net::{SocketAddr, ToSocketAddrs};
use futures::lock::Mutex;
use tokio::time::Duration;
use rand::distributions::Distribution;
use std::fmt::Debug;
use futures::{TryFutureExt, AsyncReadExt};
use crate::types::*;
use tonic::transport::Uri;
use tonic::codegen::http::uri::Authority;


const BROADCAST_CHANNEL_SIZE : usize = 32;




#[derive(Debug)]
pub struct AdversaryState {
    pub settings: Settings,
    pub client: Mutex<ChatClient>

}

impl AdversaryState {
    pub async fn new(settings: Settings) -> anyhow::Result<Self> {
        info!("Adversary connecting to server at {:?}", settings.server_addr);
        let client = ChatClient::connect(format!("http://{}", settings.server_addr)).await?;
        Ok(AdversaryState {
            settings, client: Mutex::new(client)
        })
    }
}

async fn sleep(time_ms: u64) {
    tokio::time::delay_for(Duration::from_millis(time_ms)).await;
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


pub async fn start_adversary(settings: impl AsRef<Settings>) -> anyhow::Result<()> {
    let settings = settings.as_ref();
    let adversary_state = AdversaryState::new(settings.clone()).await?;

    info!("Started adversary on {}", settings.adversary_addr);
    let adversary_imp= AdversaryServerImpl(Arc::new(adversary_state));
    let chat_service = chat_server::ChatServer::new(adversary_imp);

    Server::builder()
        .add_service(chat_service)
        .serve(settings.adversary_addr)
        .await?;

    info!("Adversary shut down");
    Ok(())
}
