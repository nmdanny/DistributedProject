use crate::consensus::types::*;
use std::rc::Rc;

pub struct Acceptor<T> {
    pub highest_promise: Option<ProposalNumber>,
    pub accepted_proposals: Vec<Proposal<T>>,
    pub log: Vec<T>,
    pub quorum_size: usize,
    pub ctx: NodeContext<T>
}

impl <T> Acceptor<T> where T: Clone {
    pub fn new(ctx: NodeContext<T>, quorum_size: usize) -> Acceptor<T> {
       Acceptor {
           log: Vec::new(),
           ctx,
           highest_promise: None,
           accepted_proposals: Vec::new(),
           quorum_size
       }
    }
    pub fn on_prepare(&mut self, proposal_number: ProposalNumber) {
        if self.highest_promise.is_none() || self.highest_promise.unwrap() < proposal_number {
            self.highest_promise = Some(proposal_number);
            self.ctx.send_broadcast(MessagePayload::Promise(proposal_number, self.accepted_proposals.clone()));
        }
        // TODO send nack
        return

    }

    pub fn on_receive_accept(&mut self, proposal: Proposal<T>) {

    }
}


