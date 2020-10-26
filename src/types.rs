use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientRequest {
    Write { contents: String },
    Read { index: usize },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WrappedClientRequest {
    pub request: ClientRequest,
    pub client_id: u64,
    pub sequence_num: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServerResponse {
    WriteOk { index: usize, contents: String },
    ReadOk { contents: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WrappedServerResponse {
    pub response: ServerResponse,
    pub client_id: u64,
    pub sequence_num: u64,
}

impl WrappedClientRequest {
    pub fn respond(&self, response: ServerResponse) -> WrappedServerResponse {
        WrappedServerResponse {
            response,
            client_id: self.client_id,
            sequence_num: self.sequence_num,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub client_id: u64,
    pub next_sequence_num: u64,
    pub last_response: Option<WrappedServerResponse>,
}

impl Peer {
    pub fn new(client_id: u64) -> Peer {
        Peer {
            client_id,
            next_sequence_num: 1,
            last_response: Option::None,
        }
    }

    pub fn wrap_request(&self, request: ClientRequest) -> WrappedClientRequest {
        WrappedClientRequest {
            client_id: self.client_id,
            sequence_num: self.next_sequence_num,
            request,
        }
    }

    pub fn wrap_response(&self, response: ServerResponse) -> WrappedServerResponse {
        WrappedServerResponse {
            client_id: self.client_id,
            sequence_num: self.next_sequence_num,
            response,
        }
    }
}

#[derive(Debug)]
pub struct ServerState {
    pub peers: HashMap<u64, Peer>,
    pub log: Vec<String>,
}

impl ServerState {
    fn new() -> ServerState {
        ServerState {
            peers: HashMap::new(),
            log: Vec::new(),
        }
    }
}
