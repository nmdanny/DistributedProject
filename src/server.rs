use futures::prelude::*;

use crate::types::*;
use futures::channel::oneshot;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;

use std::sync::Arc;
use tokio::sync::broadcast;

use tonic::{transport::Server, Request, Response, Status};
use std::collections::hash_map::Entry;

const BROADCAST_CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub struct ServerState {
    clients: tokio::sync::RwLock<HashMap<u64, Peer>>,
    log: RwLock<Vec<String>>,
    chat_broadcast: broadcast::Sender<Result<ChatUpdated, Status>>,
}

impl ServerState {
    pub fn new() -> ServerState {
        let (tx, mut rx) =
            broadcast::channel::<Result<ChatUpdated, Status>>(BROADCAST_CHANNEL_SIZE);
        tokio::spawn(async move {
            while let Ok(res) = rx.recv().await {
                let chat_updated = res.unwrap();
                let metadata = chat_updated.meta.unwrap();
                info!(
                    "({}) {} : {}",
                    chat_updated.index, metadata.client_id, chat_updated.contents
                );
            }
        });
        ServerState {
            clients: Default::default(),
            log: Default::default(),
            chat_broadcast: tx,
        }
    }
}

#[derive(Debug)]
pub struct Peer {
    pub client_id: u64,
    pub next_sequence_num: u64,
    pub last_write_index: Option<u64>,
    pub to_be_scheduled: HashMap<u64, broadcast::Sender<()>>
}

impl Peer {
    pub fn new(client_id: u64) -> Peer {
        Peer {
            client_id,
            next_sequence_num: 1,
            last_write_index: None,
            to_be_scheduled: HashMap::new(),
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

impl AsRef<PacketMetadata> for WriteRequest {
    fn as_ref(&self) -> &PacketMetadata {
        self.meta.as_ref().unwrap()
    }
}

impl AsRef<PacketMetadata> for LogRequest {
    fn as_ref(&self) -> &PacketMetadata {
        self.meta.as_ref().unwrap()
    }
}

impl ServerState {
    async fn is_request_valid<R: AsRef<PacketMetadata>>(&self, request: &R) -> RequestValidity {
        let clients = self.clients.read().await;
        let metadata = request.as_ref();
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

    async fn ensure_valid_peer<R: AsRef<PacketMetadata>>(&self, request: &R) -> Result<(), Status> {
        if self
            .clients
            .read()
            .await
            .contains_key(&request.as_ref().client_id)
        {
            return Ok(());
        }
        Err(Status::unauthenticated(
            "Client must subscribe before doing any commands",
        ))
    }

    async fn wait_for_request_turn<R: AsRef<PacketMetadata> + Debug>(&self, request: &R) -> Result<(), Status> {
        let metadata: &PacketMetadata = request.as_ref();
        let mut rx = {
            let mut clients = self.clients.write().await;
            let peer = clients.get_mut(&metadata.client_id).unwrap();
            if metadata.sequence_num <= peer.next_sequence_num {
                return Ok(());
            }
            let entry = peer.to_be_scheduled.entry(metadata.sequence_num);
            match entry {
                Entry::Occupied(entry) => {
                    warn!("Request {:?} is from the future and is also a duplicate", request);
                    entry.get().subscribe()

                },
                Entry::Vacant(e) => {
                    let (tx, rx) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
                    warn!("Request {:?} is from the future, waiting", request);
                    e.insert(tx);
                    rx
                }
            }
        };
        rx.recv().await.unwrap();
        Ok(())
    }

    async fn try_wake_next_handler(&self, peer: &mut Peer) -> Result<(), Status> {
        if let Some(tx) = peer.to_be_scheduled.remove(&peer.next_sequence_num) {
            info!(
                "Waking up request from the future at sequence_number {}",
                peer.next_sequence_num
            );
            tx.send(()).unwrap_or_else(|_| {
                error!("Failed to wake up request(the sender probably dropped it)");
                0
            });
        }
        Ok(())
    }

    async fn handle_request<Req: AsRef<PacketMetadata> + Debug, Res, ReqHandler, DupHandler>(
        &self,
        req: Req,
        handler: ReqHandler,
        dup_handler: DupHandler,
    ) -> Result<Response<Res>, Status>
    where
        ReqHandler: Fn(Req, &mut Peer) -> Result<Res, Status>,
        DupHandler: Fn(Req, &mut Peer) -> Result<Res, Status>,
    {
        self.ensure_valid_peer(&req).await?;
        self.wait_for_request_turn(&req).await?;
        let validity = self.is_request_valid(&req).await;
        if let RequestValidity::Ok = validity {
            info!("Server is handling OK request {:?}", req);
        }
        else {
            warn!(
                "Server is handling non-OK request {:?}, validity: {:?}",
                req, validity
            );
        }
        let mut clients = self.clients.write().await;
        let mut peer = clients.get_mut(&req.as_ref().client_id).unwrap();
        match validity {
            RequestValidity::Ok => {
                let res = handler(req, &mut peer)?;
                peer.next_sequence_num += 1;
                self.try_wake_next_handler(&mut peer).await?;
                Ok(Response::new(res))
            }
            RequestValidity::DuplicateRequestVeryOld => Err(Status::aborted(
                "Duplicate request is very old and was made by adversary",
            )),
            RequestValidity::DuplicateRequestOldByOne => {
                Ok(Response::new(dup_handler(req, &mut peer)?))
            }
            RequestValidity::InTheFuture => {
                panic!("Impossible, message from the future should've been taken care of.");
            }
        }
    }
}

#[tonic::async_trait]
impl chat_server::Chat for ChatServerImp {
    type SubscribeStream = impl Stream<Item = Result<ChatUpdated, Status>>;

    async fn subscribe(
        &self,
        request: Request<ConnectRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let client_id = request.into_inner().client_id;
        let mut peers = self.0.clients.write().await;
        peers.entry(client_id).or_insert(Peer::new(client_id));
        let rx = self
            .0
            .chat_broadcast
            .subscribe()
            .into_stream()
            .filter_map(|f| future::ready(f.ok()));
        info!("Registered client of id {}", client_id);
        Ok(Response::new(rx))
    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<ChatUpdated>, Status> {
        let request = request.into_inner();
        let state = self.0.clone();
        let res = tokio::spawn(async move {
            let write_handler = |req: WriteRequest, peer: &mut Peer| {
                state.log.write().push(req.contents.clone());
                let res = ChatUpdated {
                    meta: req.meta.clone(),
                    contents: req.contents,
                    index: (state.log.read().len() - 1) as u64,
                };
                let _ = state.chat_broadcast.send(Ok(res.clone()));
                peer.last_write_index = Some(res.index);
                Ok(res)
            };
            let dup_handler = |req: WriteRequest, peer: &mut Peer| {
                let res = ChatUpdated {
                    meta: req.meta.clone(),
                    contents: req.contents,
                    index: peer.last_write_index.unwrap(),
                };
                Ok(res)
            };
            state
                .handle_request(request, &write_handler, &dup_handler)
                .await
        });
        Ok(res.await.unwrap()?)
    }

    async fn read(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        let request = request.into_inner();
        let state = self.0.clone();
        let res = tokio::spawn(async move {
            let read_handler = |req: LogRequest, _peer: &mut Peer| {
                Ok(LogResponse {
                    meta: req.meta,
                    contents: state.log.read().clone(),
                })
            };
            state
                .handle_request(request, &read_handler, &read_handler)
                .await
        });
        Ok(res.await.unwrap()?)
    }
}

pub async fn start_server(settings: impl AsRef<Settings>) -> anyhow::Result<()> {
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
