#[macro_use]
extern crate log;

use futures::prelude::*;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use dist_lib::{WrappedClientRequest, ClientRequest};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();
    let mut socket = TcpStream::connect(("127.0.0.1", 8080)).await?;
    let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());

    let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::<WrappedClientRequest>::default());
    let req = WrappedClientRequest {
        client_id: 1337,
        sequence_num: 1,
        request: ClientRequest::Write { contents: "Hello world".to_string()}
    };
    info!("sending message");
    serialized.send(req).await.unwrap();
    info!("message sent");

    tokio::signal::ctrl_c().await;
    info!("Shutting down client");
    Ok(())
}
