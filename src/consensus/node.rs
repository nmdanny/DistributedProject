use crate::consensus::types::*;
use std::time::Instant;
use std::collections::HashMap;

pub trait Transport<T> {
    fn broadcast(msg: MessagePayload<T>);
}

#[derive(Debug)]
struct ProposeResolutionState<T> {
    accepted_proposals: Vec<Proposal<T>>,
    promise_count: usize
}

impl <T> ProposeResolutionState<T> {
    pub fn new() -> ProposeResolutionState<T> {
        ProposeResolutionState {
            accepted_proposals: Vec::new(),
            promise_count: 0
        }
    }
}

pub struct Node<T> {
    pub id: Id,
    pub leader_id: Id,
    pub leader_last_heartbeat: Instant,
    pub quorum_size: usize,
    pub transport: Box<dyn Transport<T>>,

    /* acceptor state */

    pub highest_proposal_number: ProposalNumber,
    pub log: Log<Proposal<T>>,

    /* proposer state */
    pub promise_resolution: HashMap<Slot, >

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
