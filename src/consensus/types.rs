use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

/// A value that can be returned from a state machine operation
pub trait OutputType: Eq + Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {}

impl <O: Eq + Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> OutputType for O {}

/// A value that can be stored in a log entry, must be (de)serializable, owned
pub trait Value: Eq + Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static {
    type Result: OutputType;
}

impl <V: Eq + Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Value for V {
    type Result = ();
}

/// Identifier for nodes, needed in order to communicate with them via the transport
pub type Id = usize;

/// An entry in the log
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
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

impl <V:Value> std::fmt::Debug for LogEntry<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LogEntry")
            .field(&self.value)
            .field(&self.term)
            .finish()
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
#[derive(Derivative, Clone, Serialize, Deserialize)]
#[derivative(Debug)]
pub struct AppendEntries<V: Value> {
    /// Leader's term
    pub term: usize,

    /// Leader's ID, followers must know it in order to redirect clients
    pub leader_id: Id,

    /// Index and term of log entry preceding the new entries
    pub prev_log_index_term: IndexTerm,

    #[serde(bound = "V: Value")]
    #[derivative(Debug="ignore")]
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

#[derive(Debug, Copy, Clone)]
pub enum AEResponseMeaning {
    Ok,
    Stale { newer_term: usize },
    Conflict
}

impl AppendEntriesResponse {
    pub fn meaning(&self, current_term: usize) -> AEResponseMeaning {
        assert!(self.term >= current_term, "An AE response cannot have a lower term than the current one");
        if self.success {
            assert_eq!(self.term, current_term, "a successful response must have the current term");
            return AEResponseMeaning::Ok;
        }
        if self.term == current_term {
            return AEResponseMeaning::Conflict;
        }
        return AEResponseMeaning::Stale { newer_term: self.term }
    }
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

    #[error("Communicator Error: {0}")]
    CommunicatorError(anyhow::Error),

    #[error("Timeout error: {0}")]
    TimeoutError(anyhow::Error),

    #[error("No longer a leader")]
    NoLongerLeader()
}

pub type RaftResult<T> = Result<T, RaftError>;



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientWriteRequest<V: Value> {
    #[serde(bound = "V: Value")]
    pub value: V
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientWriteResponse<V: Value> {
    Ok { 
        commit_index: usize,
        sm_output: V::Result
     },
    NotALeader { leader_id: Option<Id> }
}


/// Sends a value to be applied on the node's state machine, without
/// being replicated to other nodes, and without requiring the node to be a leader.
/// In essence, this completely skips the consensus mechanism (used for secret sharing, for example)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientForceApplyRequest<V: Value> {
    #[serde(bound = "V: Value")]
    pub value: V
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientForceApplyResponse<V: Value> {
    pub result: V::Result
}

/// A request to read all committed entries in `[from, to)`, or `[from, commit_index)` if 'to' isn't
/// If `commit_index +1 < to` or `commit_index < from`, an error response
/// will be given
/// If 'from' is None, then the entire committed log will be read, and 'to' will be ignored in that case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientReadRequest {
    pub from: Option<usize>,

    pub to: Option<usize>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientReadResponse<V: Value> {
    #[serde(bound = "V: Value")]
    Ok { range: Vec<V> },
    BadRange { commit_index: Option<usize> },
    NotALeader { leader_id: Option<Id> }
}

/// Represents a committed log entry
#[derive(Debug, Clone)]
pub struct CommitEntry<V: Value> {
    pub value: V,
    pub index: usize,
    pub term: usize
}

/// A pair of index and term
/// Ordering by term, tie-breaking by index. This allows checking
/// which of two logs is more up-to-date by comparing their last IndexTerm
#[derive(Clone, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum IndexTerm {
    // note that the order of the enum variants & fields matters
    NoEntry,
    SomeEntry {
        term: usize,
        index: usize
    },
}

impl std::fmt::Debug for IndexTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((index, term)) = self.as_tuple() {
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
        IndexTerm::SomeEntry {
            index, term
        }
    }
    pub fn no_entry() -> Self {
        IndexTerm::NoEntry
    }

    /// If has an entry, returns a tuple of the form (index, term)
    pub fn as_tuple(&self) -> Option<(usize, usize)> {
        if let &IndexTerm::SomeEntry { index, term} = &self {
            return Some((*index, *term));
        }
        None
    }

    pub fn contains_entry(&self) -> bool {
        if let &IndexTerm::SomeEntry { .. } = &self {
            return true;
        }
        false
    }

    pub fn index(&self) -> Option<usize> {
        self.as_tuple().map(|t| t.0)
    }

    pub fn term(&self) -> Option<usize> {
        self.as_tuple().map(|t| t.1)
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
        assert!(none < a, "An empty log is less up to date from a log with one element");

        let b = IndexTerm::new(0, 1);
        assert!(a < b, "Log with bigger term is more up-to-date");

        let c = IndexTerm::new(1, 1);
        assert!(b < c, "If two logs have the same term, the longer one is more up-to-date");
        
        let short_big_term = IndexTerm::new(50, 100);
        let long_short_term = IndexTerm::new(100000, 99);
        assert!(short_big_term > long_short_term, "A shorter log with a bigger term is considered more up-to-date");


        // same checks as above, just for sanity..
        assert!(IndexTerm::new(50, 50) < IndexTerm::new(51, 51));
        assert!(IndexTerm::no_entry() < IndexTerm::new(10, 0));
        assert!(IndexTerm::new(51, 50) < IndexTerm::new(50, 51));
    }
}