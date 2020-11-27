
use std::sync::mpsc::{Sender, Receiver, channel};
use crate::consensus::types::*;
use std::cmp::Ordering;
use std::rc::Rc;
use std::time::Duration;

const DEFAULT_TIMEOUT_MS: u64 = 1000;

pub trait Transport<T> {

}

pub struct Peer<T> {
    pub quorum_size: usize,
    pub commit_log: Log<Proposal<T>>,
    pub id: Id,
    pub peers: Vec<Id>,
    pub timeout: Duration
}

impl <T> Peer<T> {
    pub async fn propose(&mut self, value: T) -> bool {
        let slot = self.commit_log.len() - 1;
    }

    /// To be invoked upon receiving
    pub async fn on_proposal(proposal: Proposal<T>, from: Id) -> bool {

    }
}


impl <T> Peer<T> where T: Clone + std::fmt::Debug {
    pub fn new(my_id: Id, quorum_size: usize, peers: Vec<Id>) -> Peer<T> {
        Peer {
            quorum_size,
            commit_log: Default::default(),
            id: my_id,
            peers,
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS)
        }
    }

    pub async fn propose(val: T) ->


}