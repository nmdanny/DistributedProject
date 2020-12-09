use tokio::time::Duration;
use std::ops::Range;
use rand::distributions::{Distribution, Uniform};

/// How often a leader sends heartbeats
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// Range of uniformly generated times(milliseconds) until a client retries submitting
/// a failed request
pub const CLIENT_RETRY_DELAY_RANGE: std::ops::Range<u64> = 50 .. 200;

/// Range of uniformly generated times(milliseconds) for an election timeout
pub const ELECTION_TIMEOUT_MS_RANGE: Range<u64> = 150 .. 450;

/// How long a client waits until any response from the leader, before giving up
/// This is necessary because a partitioned leader will leave the client hanging
/// until re-joining the network(if it'll ever happen)
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);

/// Generates an election timeout - time without heartbeat/vote until a follower becomes
/// a candidate, or time for a candidate to start a new election
pub fn generate_election_length() -> Duration {
    let between = Uniform::from(ELECTION_TIMEOUT_MS_RANGE);
    Duration::from_millis(between.sample(&mut rand::thread_rng()))
}

