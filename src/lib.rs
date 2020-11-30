#![feature(type_alias_impl_trait)]
#![allow(unused_imports)]
#![allow(dead_code)]
#[macro_use]
pub extern crate tracing;

#[macro_use]
pub extern crate derivative;

mod adversary;
mod client;
mod server;
mod types;
pub mod consensus;

pub use adversary::start_adversary;
pub use server::start_server;
pub use types::{
    ChatClient, ChatUpdated, ConnectRequest, LogRequest, LogResponse, PacketMetadata, Settings,
    WriteRequest,
};
