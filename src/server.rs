use futures::prelude::*;
use tokio::net::TcpListener;

use std::net::{SocketAddr, ToSocketAddrs};
use futures::channel::oneshot;
use tonic::{transport::Server, Request, Response, Status, Code};
use std::collections::HashMap;
use std::sync::{Arc};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use crate::types::*;


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

#[derive(Debug)]
pub struct Peer {
    pub client_id: u64,
    pub next_sequence_num: u64,
    pub last_write_index: Option<u64>,
    pub to_be_scheduled: HashMap<u64, oneshot::Sender<()>>
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

#[derive(Debug, Copy, Clone, PartialEq)]
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

impl AsRef<PacketMetadata> for WriteRequest {
    fn as_ref(&self) -> &PacketMetadata {
        self.meta.as_ref().unwrap()
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

    async fn wait_for_request_turn<R: AsRef<PacketMetadata>>(&self, request: &R) {
        let metadata: &PacketMetadata = request.as_ref();
        let (tx, rx) = oneshot::channel();
        {
            let mut clients = self.clients.write();
            let peer = clients.get_mut(&metadata.client_id).unwrap_or_else(|| {
                panic!("Expected peer with client_id {}, no such peer registered", metadata.client_id);
            });
            if metadata.sequence_num <= peer.next_sequence_num {
                return
            }
            peer.to_be_scheduled.insert(metadata.sequence_num, tx);
        }
        rx.await.unwrap();
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
        info!("Registered client of id {}", client_id);
        Ok(Response::new(rx))

    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<ChatUpdated>, Status> {
        let req = request.into_inner();
        self.0.wait_for_request_turn(&req).await;
        let md = &req.meta.as_ref().unwrap();
        let validity = self.0.is_request_valid(md);
        let mut clients = self.0.clients.write();
        let mut peer = clients.get_mut(&md.client_id).ok_or(Status::unauthenticated("You must subscribe before sending messages"))?;
        if validity != RequestValidity::Ok {
            warn!("Detected invalid request {:?}, reason: {:?}", req, validity);
        }
        match validity {
        RequestValidity::Ok => {
                self.0.log.write().push(req.contents.clone());
                let res = ChatUpdated {
                    meta: req.meta.clone(),
                    contents: req.contents.clone(),
                    index: (self.0.log.read().len() - 1) as u64
                };
                let _ = self.0.chat_broadcast.send(Ok(res.clone()));
                peer.last_write_index = Some(res.index);
                peer.next_sequence_num += 1;

                // if there's a message scheduled with the next sequence number, wake up
                // the task responsible for handling it
                if let Some(tx) = peer.to_be_scheduled.remove(&peer.next_sequence_num) {
                    tx.send(());
                }
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
            RequestValidity::InTheFuture => unreachable!()
        }
    }

    async fn read(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        let metadata = request.into_inner().meta.unwrap();
        Ok(Response::new(LogResponse { meta: Some(metadata), contents: self.0.log.read().clone() }))
    }
}

pub async fn start_server(settings: impl AsRef<Settings>) ->  anyhow::Result<()> {
    let settings = settings.as_ref();
    let server_state = ChatServerImp(Arc::new(ServerState::new()));
    let chat_service = chat_server::ChatServer::new(server_state);

    info!("Started server on address {}", settings.server_addr);
    Server::builder()
        .add_service(chat_service)
        .serve(settings.server_addr)
        .await?;

    info!("Server shut down");
    Ok(())
}
