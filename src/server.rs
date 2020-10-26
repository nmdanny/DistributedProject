#[macro_use]
extern crate log;

use dist_lib::*;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender, SendError};
use futures::select;

use futures::prelude::*;
use serde_json::Value;
use std::borrow::BorrowMut;
use tokio::net::TcpListener;
use tokio_serde::formats::{Json};
use tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::Framed;

pub async fn accept_loop() -> std::io::Result<()> {
    let mut socket = TcpListener::bind(("0.0.0.0", 8080)).await?;


    // channel for communicating with core logic
    let (mut logic_sender, logic_receiver) = unbounded::<RequestWithSender>();


    // channel for communication with broadcaster
    let (mut res_sender_sender, res_sender_receiver) = unbounded::<UnboundedSender<WrappedServerResponse>>();
    let (mut broadcast_msg_sender, broadcast_msg_receiver) = unbounded::<WrappedServerResponse>();
    spawn_and_log_error(async move {
        broadcaster_loop(res_sender_receiver, broadcast_msg_receiver).await
    });

    // spawn a task to deal with the core logic(request-response/broadcast flow)
    spawn_and_log_error(async move {
        request_handler(logic_receiver, broadcast_msg_sender.clone()).await
    });

    info!("Server started on {}", socket.local_addr().unwrap());
    while let Some(conn) = socket.try_next().await? {
        trace!("Received connection {:?}", conn);
        let (mut read_h, write_h) = split_peer_stream(conn);

        // set up channels for sending responses/broadcasts to this peer
        let (res_sender, res_receiver) = unbounded::<WrappedServerResponse>();

        res_sender_sender.send(res_sender.clone()).await;

        // spawn a task to handle reading from the connection
        let logic_sender = logic_sender.clone();
        spawn_and_log_error(async move {
            connection_reader_loop(logic_sender, read_h, res_sender.clone()).await
        });

        // spawn a task to handle writing to the connection
        spawn_and_log_error(async move {
            connection_writer_loop(res_receiver, write_h).await
        });

    }
    Ok(())
}

pub type RequestWithSender = (WrappedClientRequest, UnboundedSender<WrappedServerResponse>);


// wires up the core logic with messaging
pub async fn request_handler(mut receiver: UnboundedReceiver<RequestWithSender>, mut broadcast: UnboundedSender<WrappedServerResponse>)
    -> Result<(), SendError> {
    let mut state = ServerState::new();
    while let Some((request, mut res_sender)) = receiver.next().await {
        let wrapped_response = state.handle_request(request).await;

        if wrapped_response.is_none() {
            continue;
        }
        let wrapped_response = wrapped_response.unwrap();
        match wrapped_response.response {
            ServerResponse::ReadOk { .. } => res_sender.send(wrapped_response).await?,
            ServerResponse::WriteOk { .. } => broadcast.send(wrapped_response).await?
        };
    }
    Ok(())
}

async fn broadcaster_loop(mut sender_receiver: UnboundedReceiver<UnboundedSender<WrappedServerResponse>>,
                          mut message_receiver: UnboundedReceiver<WrappedServerResponse>) -> Result<(), SendError> {
    let mut senders = Vec::<UnboundedSender<WrappedServerResponse>>::new();
    loop {
        select! {
           sender = sender_receiver.next() => senders.push(sender.unwrap()),
           msg = message_receiver.next() => {

            for sender in &mut senders {
                sender.send(msg.clone().unwrap()).await?;
            }
           }
        };
    }
}

async fn connection_reader_loop(mut logic_sender: UnboundedSender<RequestWithSender>, mut read_h: ReqReadStream,
                                res_sender: UnboundedSender<WrappedServerResponse>) -> Result<(), SendError> {
    loop {
        match read_h.try_next().await {
            Err(err) => {
                error!("Error trying to get request: {:?}, aborting", err);
                break;
            }
            Ok(Some(msg)) => {
                trace!("Got client request: {:?}", msg);
                logic_sender.send((msg, res_sender.clone())).await?;
            }
            Ok(None) => {
                break;
            }
        }
    }
    Ok(())
}

async fn connection_writer_loop(mut recv: UnboundedReceiver<WrappedServerResponse>, mut stream: ResWriteStream) -> std::io::Result<()>{
    while let Some(response) = recv.next().await {
        stream.send(response).await?;
    }
    Ok(())
}





#[tokio::main]
async fn main() -> std::io::Result<()>{
    pretty_env_logger::init();
    let server_loop = tokio::spawn(accept_loop());
    server_loop.await??;
    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down server");
    Ok(())
}
