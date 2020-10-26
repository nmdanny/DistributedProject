#[macro_use]
extern crate log;

use dist_lib::WrappedClientRequest;
use futures::prelude::*;
use futures::stream::{StreamExt, TryStream};
use serde_json::Value;
use std::borrow::BorrowMut;
use tokio::net::TcpListener;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::Framed;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub async fn accept_loop() -> std::io::Result<()> {
    let mut socket = TcpListener::bind(("0.0.0.0", 8080)).await?;
    info!("Server started accept loop");
    while let Some(conn) = socket.try_next().await? {
        trace!("Received connection {:?}", conn);
        let length_delimited = FramedRead::new(conn, LengthDelimitedCodec::new());
        let mut deserialized: Framed<_, WrappedClientRequest, _, _> =
            tokio_serde::SymmetricallyFramed::new(
                length_delimited,
                SymmetricalJson::<WrappedClientRequest>::default(),
            );

        // spawn a task to handle the connection
        tokio::spawn(async move {
            loop {
                match deserialized.try_next().await {
                    Err(err) => { error!("Error trying to get request: {:?}, aborting", err);
                    break;}
                    Ok(Some(msg)) => {
                        trace!("Got client request: {:?}", msg);
                    }
                    Ok(None) => {break;}
                }
            }
        });

    }
    Ok(())
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let server_loop = tokio::spawn(accept_loop());
    server_loop.await.unwrap();
    tokio::signal::ctrl_c().await;
    info!("Shutting down server");
}
