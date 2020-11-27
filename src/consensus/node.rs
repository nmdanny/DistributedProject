use crate::consensus::types::*;
use std::time::Instant;
use tracing::stdlib::collections::HashMap;

pub trait Transport<T> {
    fn broadcast(msg: MessagePayload<T>);
}

pub struct Node<T> {
    pub id: Id,
    pub leader_id: Id,
    pub leader_last_heartbeat: Instant,
    pub log: Log<T>,
    pub promises: HashMap<usize, >
    pub highest_proposal_number: ProposalNumber,
    pub quorum_size: usize,
    pub transport: Box<dyn Transport<T>>
}

impl Node<T> {
    pub fn new(id: Id, transport: impl Transport<T>, quorum_size: usize) -> Node<T>
    {
        Node {
            id,
            leader_id: 0,
            leader_last_heartbeat: Instant::now(),
            log: Default::default(),
            highest_proposal_number: ProposalNumber {
                leader_id: 0,
                num: 0
            },
            quorum_size,
            transport: Box::new(transport)
        }
    }

    /* Proposer */


    /// Tries to become a leader
    pub fn prepare(&mut self) {


    }
}
