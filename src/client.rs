#[macro_use]
extern crate log;

use dist_lib::{ClientRequest, WrappedClientRequest, WrappedServerResponse};
use futures::prelude::*;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_serde::formats::Json;
use tokio_serde::Framed;
use tokio_util::codec::LengthDelimitedCodec;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();
    let mut socket = TcpStream::connect(("127.0.0.1", 8080)).await?;
    let length_delimited = tokio_util::codec::Framed::new(socket, LengthDelimitedCodec::new());

    let mut serialized: Framed<_, WrappedServerResponse, WrappedClientRequest, _> =
        tokio_serde::Framed::new(
            length_delimited,
            Json::<WrappedServerResponse, WrappedClientRequest>::default(),
        );
    let req = WrappedClientRequest {
        client_id: 1337,
        sequence_num: 1,
        request: ClientRequest::Write {
            contents: "Hello world".to_string(),
        },
    };
    info!("sending message");
    serialized.send(req).await.unwrap();
    info!("message sent");

    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down client");
    Ok(())
}
