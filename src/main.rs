use std::collections::{BTreeMap, HashMap};
use actix::prelude::*;
use actix_derive::{Message, MessageResponse};
use serde::export::fmt::Debug;
use serde::export::Formatter;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;


#[derive(Debug, Clone)]
enum ClientRequest {
    Write { contents: String },
    Read { index: usize },
}

#[derive(Message, Clone)]
#[rtype(result = "ServerResponse")]
struct NumberedClientRequest {
    request: ClientRequest,
    address: Addr<Client>,
    number: usize
}

#[derive(MessageResponse, Debug)]
enum ServerResponse {
    WriteOk,
    ReadOk { contents: String },
    DuplicateMessageDropped,
    ToBeHandledInTheFuture,
    AdversaryDroppedMessage

}

struct Client
{
    server_addr: Addr<RealServer>,
    counter: usize
}


impl Client {
    async fn send_request(&mut self, ctx: &mut <Client as Actor>::Context, request: ClientRequest) -> Request<RealServer, NumberedClientRequest> {
        let req = self.server_addr.send(NumberedClientRequest {
            address: ctx.address(),
            number: self.counter,
            request
        });
        self.counter += 1;
        req
    }
    async fn demo(&mut self, ctx: &mut <Client as Actor>::Context) {
        let mut hasher = DefaultHasher::new();
        ctx.address().hash(&mut hasher);
        let hash = hasher.finish();

        loop {
            let req = self.send_request(ctx, ClientRequest::Write { contents: format!("Client {} - counter is {}", hash, self.counter)}).await;
            let res = req.await;
            match res {
                Ok(ServerResponse::AdversaryDroppedMessage) => {},
                Ok(ServerResponse::WriteOk) => { break; },
                _ => { println!("Client got unexpected response")}
            }
        }
    }
}

impl Actor for Client
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let mut hasher = DefaultHasher::new();
        ctx.address().hash(&mut hasher);
        println!("Client with hash {} started", hasher.finish());

    }

}



trait Server : Handler<NumberedClientRequest, Result = ServerResponse> + Actor
{
}

struct PerClientState
{
    counter: usize,
    message_queue: BTreeMap<usize, ClientRequest>
}

impl PerClientState
{
    fn new() -> PerClientState
    {
        return PerClientState
        {
            counter: 1, message_queue: BTreeMap::new()
        }
    }
}

struct RealServer
{
    client_refs: HashMap<Addr<Client>, PerClientState>,
    log: Vec<String>
}

impl Default for RealServer
{
    fn default() -> Self {
        RealServer { client_refs: HashMap::new(), log: Vec::new() }
    }
}

struct Adversary
{
    real_server_ref : Addr<RealServer>
}
impl RealServer
{
    fn process_request(&mut self, req: ClientRequest) -> ServerResponse {
        match req {
            ClientRequest::Write { contents } => {
                self.log.push(contents);
                ServerResponse::WriteOk
            },
            ClientRequest::Read { index } if index >= self.log.len() => { panic!("Read invalid index") }
            ClientRequest::Read { index} => { ServerResponse::ReadOk { contents: self.log[index].clone() }}
        }
    }
}
impl Handler<NumberedClientRequest> for RealServer
{
    type Result = ServerResponse;


    fn handle(&mut self, msg: NumberedClientRequest, _ctx: &mut Context<RealServer>) -> Self::Result {
        let c_ref = self.client_refs.entry(msg.address).or_insert(PerClientState::new());

        return if msg.number == c_ref.counter {
            println!("Server processing request that arrived on time: {:?} ", msg.request);
            self.process_request(msg.request)
        } else if msg.number < c_ref.counter {
            println!("Server encountered duplicate message, dropping: {:?}", msg.request);
            ServerResponse::DuplicateMessageDropped
        } else {
            println!("Server encountered message to be processed in the future: {:?}", msg.request);
            c_ref.message_queue.insert(msg.number, msg.request);
            ServerResponse::ToBeHandledInTheFuture
        }
    }
}

impl Actor for RealServer
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!("Real server started");
    }
}

impl Server for RealServer {}

// fn main() {
//     let system = actix::System::new("test");
//     let server = RealServer::start_default();
//     let num_clients = 4;
//     for i in 0..num_clients {
//         let mut client = Client { counter: 1, server_addr: server.clone() };
//         let addr = client.start();
//     }
//     system.run().unwrap();
// }

#[actix_rt::main]
async fn main() {
    let server = RealServer::start_default();
    let num_clients = 4;
    for i in 0..num_clients {
        let client = Client { counter: 1, server_addr: server.clone() };
        client.start();
    }
}
