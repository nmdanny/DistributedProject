use tokio::time::Duration;
use std::ops::Range;
use rand::distributions::{Distribution, Uniform};

/// How often a leader sends heartbeat
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// Range of uniformly generated times(milliseconds) until a client retries submitting
/// a failed request
pub const CLIENT_RETRY_DELAY_RANGE: std::ops::Range<u64> = 100 .. 200;

/// Range of uniformly generated times(milliseconds) for an election timeout
pub const ELECTION_TIMEOUT_MS_RANGE: Range<u64> = 1500 .. 3000;


/// Generates an election timeout - time without heartbeat/vote until a follower becomes
/// a candidate, or time for a candidate to start a new election
pub fn generate_election_length() -> Duration {
    let between = Uniform::from(ELECTION_TIMEOUT_MS_RANGE);
    Duration::from_millis(between.sample(&mut rand::thread_rng()))
}

