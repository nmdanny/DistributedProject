#[macro_use]
extern crate log;

use dist_lib::{ServerState, WrappedClientRequest, WrappedServerResponse};
use futures::channel::mpsc::{unbounded, Receiver, Sender};
use futures::prelude::*;
use serde_json::Value;
use std::borrow::BorrowMut;
use tokio::net::TcpListener;
use tokio_serde::formats::{Json};
use tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::Framed;

pub async fn accept_loop() -> std::io::Result<()> {
    let mut socket = TcpListener::bind(("0.0.0.0", 8080)).await?;
    info!("Server started accept loop");
    while let Some(conn) = socket.try_next().await? {
        trace!("Received connection {:?}", conn);
        let length_delimited = tokio_util::codec::Framed::new(conn, LengthDelimitedCodec::new());
        let mut deserialized : Framed<_, WrappedClientRequest, WrappedServerResponse, _>= tokio_serde::Framed::new(
            length_delimited,
            Json::<WrappedClientRequest, WrappedServerResponse>::default(),
        );

        // use channel to

        // spawn a task to handle the connection
        tokio::spawn(async move {
            loop {
                match deserialized.try_next().await {
                    Err(err) => {
                        error!("Error trying to get request: {:?}, aborting", err);
                        break;
                    }
                    Ok(Some(msg)) => {
                        trace!("Got client request: {:?}", msg);
                    }
                    Ok(None) => {
                        break;
                    }
                }
            }
        });
    }
    Ok(())
}

async fn request_handler(state: &mut ServerState, mut recv: Receiver<WrappedClientRequest>) {
    while let Some(req) = recv.next().await {
        state.handle_request(req);
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let server_loop = tokio::spawn(accept_loop());
    server_loop.await.unwrap();
    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down server");
}
