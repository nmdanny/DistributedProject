use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

/// A value that can be stored in a log entry, must be (de)serializable, owned
pub trait Value: Eq + Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}

impl <V: Eq + Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Value for V {

}

/// Identifier for nodes, needed in order to communicate with them via the transport
pub type Id = usize;

/// An entry in the log
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct LogEntry<V: Value> {

    #[serde(bound = "V: Value")]
    /// Value
    pub value: V,

    /// In which term was this entry received by the leader
    pub term: usize,

}

impl <V: Value> LogEntry<V> {
    pub fn new(value: V, term: usize) -> Self {
        LogEntry { value, term }
    }
}

/// Invoked by candidates to gather votes(sent to all nodes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    /// Candidate's term
    pub term: usize,

    /// ID of Candidate
    pub candidate_id: Id,

    /// Index and term of candidate's last log entry
    pub last_log_index_term: IndexTerm

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

    /// Index and term of log entry preceding the new entries
    pub prev_log_index_term: IndexTerm,

    #[serde(bound = "V: Value")]
    /// New entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry<V>>,

    /// Leader's commit index (None if he hasn't committed anything)
    pub leader_commit: Option<usize>
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
    NetworkError(anyhow::Error),

    #[error("Internal Error: {0}")]
    InternalError(anyhow::Error),

    /// An error
    #[error("Communicator Error: {0}")]
    CommunicatorError(anyhow::Error)
}

pub type RaftResult<T> = Result<T, RaftError>;


// Client requests are serviced by the leader,
// Other nodes will

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientWriteRequest<V: Value> {
    #[serde(bound = "V: Value")]
    pub value: V
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientWriteResponse {
    Ok { commit_index: usize },
    NotALeader { leader_id: Option<Id> }
}

/// A request to read all committed entries in [from, to)
/// If commit_index < to, we'll return log[from, commit_index] instead
/// If commit_Index < from, an empty log will be given
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientReadRequest {
    pub from: usize,

    pub to: Option<usize>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientReadResponse<V: Value> {
    #[serde(bound = "V: Value")]
    Ok { range: Vec<V> },
    NotALeader { leader_id: Option<Id> }
}

/// Represents a committed log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitEntry<V: Value> {
    #[serde(bound = "V: Value")]
    pub value: V,
    pub index: usize,
    pub term: usize
}

/// A pair of index and term
/// Unlike
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexTerm(pub Option<(usize, usize)>);

impl std::fmt::Debug for IndexTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((index, term)) = self.0 {
            f.debug_struct("IndexTerm")
                .field("index", &index)
                .field("term", &term)
                .finish()
        } else {
            f.write_str("IndexTerm(NoEntry)")
        }


    }
}

impl IndexTerm {
    pub fn new(index: usize, term: usize) -> Self {
        IndexTerm(Some((index, term)))
    }
    pub fn no_entry() -> Self {
        IndexTerm(None)
    }


    pub fn for_entry<V: Value>(index: usize, entry: &LogEntry<V>) -> Self {
        IndexTerm(Some((index, entry.term)))
    }

    pub fn contains_entry(&self) -> bool {
        self.0.is_some()
    }

    pub fn index(&self) -> Option<usize> {
        self.0.map(|t| t.0)
    }

    pub fn term(&self) -> Option<usize> {
        self.0.map(|t| t.1)
    }
}

impl From<(usize, usize)> for IndexTerm {
    fn from((index, term) : (usize, usize)) -> Self {
        Self::new(index, term)
    }
}

#[cfg(test)]
mod tests{
    use crate::consensus::types::IndexTerm;

    #[test]
    fn test_index_term() {
        let none = IndexTerm::no_entry();

        let a = IndexTerm::new(0, 0);
        assert!(none < a);

        let b = IndexTerm::new(5, 10);
        assert!(a < b);

        let c = IndexTerm::new(6, 20);
        assert!(b < c);

        let d = IndexTerm::new(6, 25);
        assert!(c < d);
    }
}