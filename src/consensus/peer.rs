
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

impl <T> Peer<T> where T: Clone {
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

    pub fn handle_message(&mut self, message: MessagePayload<T>) {

    }
}