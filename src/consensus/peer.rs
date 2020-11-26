
use std::sync::mpsc::{Sender, Receiver, channel};
use crate::consensus::types::*;
use std::cmp::Ordering;
use crate::consensus::proposer::Proposer;
use crate::consensus::acceptor::Acceptor;
use std::rc::Rc;


pub struct Peer<T> {
    pub quorum_size: usize,
    pub proposer: Proposer<T>,
    pub acceptor: Acceptor<T>

}

impl <T> Peer<T> where T: Clone + std::fmt::Debug {
    pub fn new(my_id: Id, quorum_size: usize) -> Peer<T> {
        let (sender, receiver) = channel();
        let ctx = NodeContext { my_id, sender};
        let proposer = Proposer::new(ctx.clone(), quorum_size);
        let acceptor = Acceptor::new(ctx.clone(), quorum_size);
        Peer {
            quorum_size,
            proposer,
            acceptor
        }
    }

    pub async fn propose(&mut self, value: T) {
        self.proposer.push_value_to_be_proposed(value);
        self.proposer.prepare();
        // TODO what if we accepted something else

    }

    pub fn handle_message(&mut self, message: MessagePayload<T>) {
        match message {
            MessagePayload::Prepare(num, slot) => self.acceptor.on_prepare(num, slot),
            MessagePayload::Promise(num, slot, accepted) => self.proposer.on_promise(num, slot, accepted),
            MessagePayload::Accept(proposal) => self.acceptor.on_accept(proposal),
            MessagePayload::Accepted(proposal) => self.proposer.on_accepted(proposal),
            MessagePayload::Heartbeat(_) => {},
            MessagePayload::Complain(_) => {},
            MessagePayload::LeaveView(_) => {},
            MessagePayload::ViewChange(_) => {}
        }
    }

}