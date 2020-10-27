#![feature(type_alias_impl_trait)]
#[macro_use]
extern crate log;

use futures::prelude::*;
use tokio::net::TcpListener;

use tonic::{transport::Server, Request, Response, Status};
use std::collections::HashMap;
use std::sync::{Arc};
use parking_lot::RwLock;
use tokio::sync::broadcast;

tonic::include_proto!("chat");


const BROADCAST_CHANNEL_SIZE : usize = 32;

#[derive(Debug)]
pub struct ServerState {
    clients: RwLock<HashMap<u64, Peer>>,
    log: RwLock<Vec<String>>,
    chat_broadcast: broadcast::Sender<Result<ChatUpdated, Status>>
}

impl ServerState {
    pub fn new() -> ServerState {
        let (tx,mut rx) = broadcast::channel::<Result<ChatUpdated, Status>>(BROADCAST_CHANNEL_SIZE);
        tokio::spawn(async move {

            while let Ok(res) = rx.recv().await {
                let chat_updated = res.unwrap();
                let metadata = chat_updated.meta.unwrap();
                println!("({}) {} : {}", chat_updated.index, metadata.client_id, chat_updated.contents);
            }
        });
        ServerState {
            clients: Default::default(),
            log: Default::default(),
            chat_broadcast: tx
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub client_id: u64,
    pub next_sequence_num: u64,
    pub last_write_index: Option<u64>,
    pub to_be_scheduled: HashMap<u64, WriteRequest>
}

impl Peer {
    pub fn new(client_id: u64) -> Peer {
        Peer {
            client_id,
            next_sequence_num: 1,
            last_write_index: None,
            to_be_scheduled: HashMap::new()
        }
    }
}

struct ChatServerImp(Arc<ServerState>);

enum RequestValidity {
    Ok,
    NonRegisteredClient,
    DuplicateRequestOldByOne,
    DuplicateRequestVeryOld,
    InTheFuture,
}

impl ServerState {
    fn is_request_valid(&self, metadata: PacketMetadata) -> RequestValidity {
        let clients = self.clients.read();
        if !clients.contains_key(&metadata.client_id) {
            return RequestValidity::NonRegisteredClient;
        }
        let peer = clients.get(&metadata.client_id).unwrap();
        if metadata.sequence_num == peer.next_sequence_num {
            return RequestValidity::Ok;
        }
        if metadata.sequence_num == peer.next_sequence_num - 1 {
            return RequestValidity::DuplicateRequestOldByOne;
        }
        if metadata.sequence_num < peer.next_sequence_num - 1 {
            return RequestValidity::DuplicateRequestVeryOld;
        }
        if metadata.sequence_num > peer.next_sequence_num {
            return RequestValidity::InTheFuture;
        }
        unreachable!()
    }
}

#[tonic::async_trait]
impl chat_server::Chat for ChatServerImp {
    type SubscribeStream = impl Stream<Item = Result<ChatUpdated, Status>>;

    async fn subscribe(&self, request: Request<ConnectRequest>) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut peers = self.0.clients.write();
        let client_id = request.into_inner().client_id;
        let rx = self.0.chat_broadcast.subscribe().into_stream()
            .filter_map(|f| future::ready(f.ok()));
        peers.entry(client_id).or_insert(Peer::new(client_id));
        Ok(Response::new(rx))

    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<ChatUpdated>, Status> {
        let req = request.into_inner();
        self.0.log.write().push(req.contents.clone());
        let res = ChatUpdated {
            meta: req.meta.clone(),
            contents: req.contents.clone(),
            index: (self.0.log.read().len() - 1) as u64
        };
        let _res = self.0.chat_broadcast.send(Ok(res.clone()));
        Ok(Response::new(res))
    }

    async fn read(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        let metadata = request.into_inner().meta.unwrap();
        Ok(Response::new(LogResponse { meta: Some(metadata), contents: self.0.log.read().clone() }))
    }
}
use tokio_compat_02::FutureExt;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let server_state = ChatServerImp(Arc::new(ServerState::new()));
    let chat_service = chat_server::ChatServer::new(server_state);


    Server::builder()
        .add_service(chat_service)
        .serve("[::]:8950".parse().unwrap())
        .compat()
        .await?;

    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down server");
    Ok(())
}
