use crate::consensus::types::*;
use std::rc::Rc;
use tracing::{debug, error, info, span, warn, Level};

#[derive(Debug)]
pub struct Acceptor<T> {
    pub highest_promise: Option<ProposalNumber>,
    pub quorum_size: usize,
    pub ctx: NodeContext<T>,
    pub log: Log<Proposal<T>>,
}

impl <T> Acceptor<T> where T: Clone + std::fmt::Debug {
    pub fn new(ctx: NodeContext<T>, quorum_size: usize) -> Acceptor<T> {
       Acceptor {
           log: Default::default(),
           ctx,
           highest_promise: None,
           quorum_size
       }
    }

    #[tracing::instrument]
    pub fn on_prepare(&mut self, proposal_number: ProposalNumber, slot: Slot) {
        if self.highest_promise.is_none() || self.highest_promise.unwrap() < proposal_number {
            self.highest_promise = Some(proposal_number);
            let previously_accepted_proposal = self.log.get(&slot).cloned();
            self.ctx.send_broadcast(MessagePayload::Promise(proposal_number, slot, previously_accepted_proposal));
            return
        }

        warn!("proposal number is too low, ignoring");
        // TODO send nack
        return

    }

    #[tracing::instrument]
    pub fn on_accept(&mut self, proposal: Proposal<T>) {
        // TODO: what if proposal.number is higher than the highest promise?
        if Some(proposal.number) == self.highest_promise {
           info!("accepted proposal");
            let _prev = self.log.insert(proposal.slot, proposal.clone());
            assert!(_prev.is_none());
            self.ctx.send_broadcast(MessagePayload::Accepted(proposal));
        } else {
            warn!("proposal number is too low, ignoring");
        }
    }
}


