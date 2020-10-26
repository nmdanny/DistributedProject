#[macro_use]
extern crate log;

use rand::prelude::*;
use dist_lib::{ClientRequest, WrappedClientRequest, WrappedServerResponse, create_server_stream};
use futures::prelude::*;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();
    let socket = TcpStream::connect(("127.0.0.1", 8080)).await?;
    let mut conn = create_server_stream(socket);

    let req = WrappedClientRequest {
        client_id: random(),
        sequence_num: 1,
        request: ClientRequest::Write {
            contents: "Hello world".to_string(),
        },
    };
    info!("sending message");
    conn.send(req).await.unwrap();
    info!("message sent");

    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down client");
    Ok(())
}
