#[macro_use]
extern crate log;

use rand::prelude::*;
use dist_lib::*;
use futures::prelude::*;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();


    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down client");
    Ok(())
}
