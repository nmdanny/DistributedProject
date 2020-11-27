
use std::sync::mpsc::{Sender, Receiver, channel};
use crate::consensus::types::*;
use std::cmp::Ordering;
use crate::consensus::proposer::Proposer;
use crate::consensus::acceptor::Acceptor;
use std::rc::Rc;
use crate::consensus::learner::Learner;


pub struct Peer<T> {
    pub id: Id,
    pub quorum_size: usize,
    pub proposer: Proposer<T>,
    pub acceptor: Acceptor<T>,
    pub learner: Box<dyn Learner<T>>,
    pub leader_id: Id,
}

impl <T> Peer<T> where T: Clone + std::fmt::Debug {
    pub fn new(my_id: Id, quorum_size: usize) -> (Peer<T>, Receiver<ConsensusMessage<T>>) {
        let (sender, receiver) = channel();
        let ctx = NodeContext { my_id, sender};
        let proposer = Proposer::new(ctx.clone(), quorum_size);
        let acceptor = Acceptor::new(ctx.clone(), quorum_size);
        (Peer {
            id: my_id,
            quorum_size,
            proposer,
            acceptor,
            learner: Box::new(|p: Proposal<T>| {info!("Accepted {:?}", p)}),
            leader_id: 0,
        }, receiver)
    }

    /// The server will try to become a leader and propose the given value(usually sent from some client)
    /// In case it accepts previous missed values, it will repeat the prepare process until accepting
    /// the given value.
    pub fn handle_client_request(&mut self, val: T) {
        self.proposer.to_be_proposed.push(val);
        self.proposer.prepare();
    }

    /// When a node acts as a leader,
    pub fn leader_tick(&mut self)
    {
        if self.proposer.to_be_proposed.is_empty() {
            self.proposer.send_heartbeat();
        } else {
            let _ = self.proposer.prepare();
        }
    }

    /// Handles consensus messages
    pub fn handle_message(&mut self, message: ConsensusMessage<T>) {
        match message.payload {
            MessagePayload::ClientRequest(val) => self.handle_client_request(val),
            MessagePayload::Prepare(num, slot) => self.acceptor.on_prepare(num, slot),
            MessagePayload::Promise(num, slot, accepted) => self.proposer.on_promise(num, slot, accepted),
            MessagePayload::Accept(proposal) => self.acceptor.on_accept(proposal),
            MessagePayload::Accepted(proposal) => {
                self.proposer.on_accepted(proposal.clone());
                self.learner.on_accepted(proposal);
            },
            MessagePayload::Heartbeat(_) => {},
            MessagePayload::Complain(_) => {},
            MessagePayload::LeaveView(_) => {},
            MessagePayload::ViewChange(_) => {}
        }
    }

}