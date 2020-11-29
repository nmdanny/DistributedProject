use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

/// A value that can be stored in a log entry, must be (de)serializable, owned
pub trait Value: Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}

impl <V: Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Value for V {

}

/// Identifier for nodes, needed in order to communicate with them via the transport
pub type Id = usize;

/// An entry in the log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<V: Value> {

    #[serde(bound = "V: Value")]
    /// Value
    pub value: V,

    /// In which term was this entry received by the leader
    pub term: usize,

}

/// Invoked by candidates to gather votes(sent to all nodes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    /// Candidate's term
    pub term: usize,

    /// ID of Candidate
    pub candidate_id: Id,

    /// Index of candidate's last log entry
    pub last_log_index: usize,

    /// Term of candidate's last log entry
    pub last_log_term: usize

}

/// Response to RequestVote (sent by each node to the candidate)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Used to update the current candidate's term
    pub term: usize,

    /// Whether he candidate received a vote
    pub vote_granted: bool
}

impl RequestVoteResponse {
    pub fn vote_yes(my_term: usize) -> RequestVoteResponse {
        RequestVoteResponse {
            term: my_term, vote_granted: true
        }
    }

    pub fn vote_no(my_term: usize) -> RequestVoteResponse {
        RequestVoteResponse {
            term: my_term, vote_granted: false
        }
    }
}


/// Invoked by leader to replicate log entries, also used as heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries<V: Value> {
    /// Leader's term
    pub term: usize,

    /// Leader's ID, followers must know it in order to redirect clients
    pub leader_id: Id,

    /// Index of log entry preceding the new entries
    pub prev_log_index: usize,

    /// Term of log entry preceding the new entries
    pub prev_log_term: usize,

    #[serde(bound = "V: Value")]
    /// New entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry<V>>,

    /// Leader's commit index
    pub leader_commit: usize
}

impl <V: Value> AppendEntries<V> {
    pub fn indexed_entries(&self) -> impl Iterator<Item = (usize, &LogEntry<V>)> {
        let indices = (self.prev_log_index + 1 .. );
        return indices.zip(self.entries.iter())
    }
}

/// Invoked by each follower(or candidate, in which case they become followers)
/// upon receiving AppendEntries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The current term, used by leader to update its own term
    pub term: usize,

    /// True if the follower included an entry matching `prev_log_index`, `prev_log_term`
    /// and thus have appended the given entries
    pub success: bool
}

impl AppendEntriesResponse {
    pub fn success(my_term: usize) -> AppendEntriesResponse {
        AppendEntriesResponse { term: my_term, success: true}
    }

    pub fn failed(my_term: usize) -> AppendEntriesResponse {
        AppendEntriesResponse { term: my_term, success: false}
    }
}

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Network Error: {0}")]
    NetworkError(anyhow::Error)
}

pub type RaftResult<T> = Result<T, RaftError>;