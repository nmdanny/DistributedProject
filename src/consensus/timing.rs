use tokio::time::Duration;
use std::ops::Range;
use rand::distributions::{Distribution, Uniform};
use clap::Parser;
use crate::util::{parse_phase_length, parse_range};

use super::types::Id;

// TODO: maybe put more things into settings, such as node id and transport timeouts?

/// Settings used by any raft node
#[derive(Parser, Debug, Clone)]
pub struct RaftServerSettings {
    /// How often a leader sends heartbeat in ms
    #[clap(long = "heartbeat_interval", default_value = "200",
           parse(try_from_str = parse_phase_length))]
    pub heartbeat_interval: Duration,

    /// Range of uniformly generated times(milliseconds) for length of an election/time with no heartbeats until election
    #[clap(long = "election_range",
           default_value = "150..300",
           parse(try_from_str = parse_range))]
    pub election_timeout_ms_range: Range<u64>,

    ///  If set, disables the heartbeat loop via a separate thread, and instead relies on the replication loop.
    ///  Might improve performance, but if AEs are too big or raft node loop is starved, may cause frequent elections
    #[clap(long = "no_heartbeat_loop")]
    pub no_heartbeat_loop: bool

}

/// Settings used by any raft client
#[derive(Parser, Debug, Clone)]
pub struct RaftClientSettings {
    /// Range of uniformly generated times(milliseconds) until a client retries submitting
    /// a failed request
    #[clap(long = "client_retry_range",
           default_value = "100..200",
           parse(try_from_str = parse_range))]
    pub retry_delay_ms: Range<u64>,

    /// Maximum number of submit attempts(probably to different leaders) until the submit value is deemed failure
    #[clap(long = "max_retries",
           default_value = "1000")]
    pub max_retries: u64
}

impl Default for RaftServerSettings {
    fn default() -> Self {
        RaftServerSettings {
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_ms_range: 150 .. 300,
            no_heartbeat_loop: false
        }
    }
}

impl Default for RaftClientSettings {
    fn default() -> Self {
        RaftClientSettings {
            retry_delay_ms: 100 .. 200,
            max_retries: 1000
        }
    }
}

/// Generates an election timeout - time without heartbeat/vote until a follower becomes
/// a candidate, or time for a candidate to start a new election
pub fn generate_election_length(settings: &RaftServerSettings) -> Duration {
    let between = Uniform::from(settings.election_timeout_ms_range.clone());
    Duration::from_millis(between.sample(&mut rand::thread_rng()))
}

