#![feature(type_alias_impl_trait)]
#[macro_use]
pub extern crate log;

mod types;
mod server;
mod adversary;

pub use server::start_server;
pub use adversary::start_adversary;
pub use types::{Settings, WriteRequest, LogRequest, ChatUpdated, LogResponse,
                PacketMetadata, ChatClient};