use clap::Clap;

use std::net::SocketAddr;

tonic::include_proto!("chat");

pub type ChatClient = chat_client::ChatClient<tonic::transport::Channel>;

#[derive(Clap, Debug, Clone)]
#[clap()]
pub struct Settings {
    #[clap(
        short = 's',
        long,
        about = "Server address",
        default_value = "[::1]:8950"
    )]
    pub server_addr: SocketAddr,

    #[clap(
        short = 'a',
        long,
        about = "Adversary address",
        default_value = "[::1]:1337"
    )]
    pub adversary_addr: SocketAddr,

    #[clap(
        long,
        about = "probability of dropping a message",
        default_value = "0.5"
    )]
    pub drop_prob: f64,

    #[clap(
        long,
        about = "probability of duplicating a message at some time",
        default_value = "0.3"
    )]
    pub duplicate_prob: f64,

    #[clap(
        long,
        about = "probability of re-ordering packets",
        default_value = "0.3"
    )]
    pub reorder_prob: f64,

    #[clap(
        long,
        about = "minimum delay(in ms) until re-ordered/duplicated messages are sent",
        default_value = "100"
    )]
    pub min_delay_ms: u64,

    #[clap(
        long,
        about = "maximum delay(in ms) until re-ordered/duplicated messages are sent",
        default_value = "3000"
    )]
    pub max_delay_ms: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            server_addr: "[::1]:8950".parse().unwrap(),
            adversary_addr: "[::1]:1337".parse().unwrap(),
            drop_prob: 0.5,
            duplicate_prob: 0.3,
            reorder_prob: 0.3,
            min_delay_ms: 100,
            max_delay_ms: 3000,
        }
    }
}
