#![feature(type_alias_impl_trait)]
#[macro_use]
pub extern crate log;

mod adversary;
mod client;
mod server;
mod types;

pub use adversary::start_adversary;
pub use server::start_server;
pub use types::{
    ChatClient, ChatUpdated, ConnectRequest, LogRequest, LogResponse, PacketMetadata, Settings,
    WriteRequest,
};
