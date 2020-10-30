#[macro_use]
extern crate log;

use dist_lib::{start_server, start_adversary, Settings};

use clap::Clap;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();
    let options = Arc::new(Settings::parse());
    let options2 = options.clone();
    let _ = tokio::try_join!(
        tokio::spawn(async move { start_server(options.clone()).await }),
        tokio::spawn(async move { start_adversary(options2).await }),
    );



    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down client");
    Ok(())
}
