#![feature(type_alias_impl_trait)]
#[macro_use]
extern crate log;

use futures::prelude::*;
use tokio::net::TcpListener;

use tokio_compat_02::FutureExt;
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status, Code};
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

#[derive(Debug, Copy, Clone)]
enum RequestValidity {
    Ok,
    DuplicateRequestOldByOne,
    DuplicateRequestVeryOld,
    InTheFuture,
}


impl From<WriteRequest> for PacketMetadata {
    fn from(req: WriteRequest) -> Self {
        req.meta.unwrap()
    }
}

impl From<LogRequest> for PacketMetadata {
    fn from(req: LogRequest) -> Self {
        req.meta.unwrap()
    }
}

impl ServerState {
    fn is_request_valid(&self, metadata: &PacketMetadata) -> RequestValidity {
        let clients = self.clients.read();
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
        let client_id = request.into_inner().client_id;
        let mut peers = self.0.clients.write();
        peers.entry(client_id).or_insert(Peer::new(client_id));
        let rx = self.0.chat_broadcast.subscribe().into_stream()
            .filter_map(|f| future::ready(f.ok()));
        Ok(Response::new(rx))

    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<ChatUpdated>, Status> {
        let req = request.into_inner();
        let md = &req.meta.as_ref().unwrap();
        let validity = self.0.is_request_valid(md);
        let mut clients = self.0.clients.write();
        let mut peer = clients.get_mut(&md.client_id).ok_or(Status::unauthenticated("You must subscribe before sending messages"))?;
        match validity {
            RequestValidity::Ok => {
                info!("is valid");
                self.0.log.write().push(req.contents.clone());
                info!("log updated");
                let res = ChatUpdated {
                    meta: req.meta.clone(),
                    contents: req.contents.clone(),
                    index: (self.0.log.read().len() - 1) as u64
                };
                let _ = self.0.chat_broadcast.send(Ok(res.clone()));
                info!("msg broadcast");
                peer.last_write_index = Some(res.index);
                peer.next_sequence_num += 1;
                info!("return");
                Ok(Response::new(res))
            }
            RequestValidity::DuplicateRequestOldByOne => {
                let res = ChatUpdated {
                    meta: req.meta.clone(),
                    contents: req.contents.clone(),
                    index: peer.last_write_index.unwrap()
                };

                Ok(Response::new(res))
            },
            RequestValidity::DuplicateRequestVeryOld => Err(Status::already_exists("duplicate request whose response was received")),
            RequestValidity::InTheFuture => Err(Status::unimplemented("todo handle requests from the future"))
        }
    }

    async fn read(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        let metadata = request.into_inner().meta.unwrap();
        Ok(Response::new(LogResponse { meta: Some(metadata), contents: self.0.log.read().clone() }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let server_state = ChatServerImp(Arc::new(ServerState::new()));
    let chat_service = chat_server::ChatServer::new(server_state);


    Server::builder()
        .add_service(chat_service)
        .serve("[::1]:8950".parse().unwrap())
        .compat()
        .await?;

    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down server");
    Ok(())
}
