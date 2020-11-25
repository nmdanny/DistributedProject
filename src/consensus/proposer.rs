use crate::consensus::types::*;
use std::sync::mpsc::Sender;
use std::rc::Rc;
use std::collections::HashMap;
use futures::channel::oneshot;
use std::cell::RefCell;
use std::{error::Error, io};
use tracing::{debug, error, info, span, warn, Level};


#[derive(Debug)]
struct ProposeResolutionState<T> {
    promises: Vec<Proposal<T>>,
    promise_count: usize
}

impl <T> ProposeResolutionState<T> {
    pub fn new() -> ProposeResolutionState<T> {
        ProposeResolutionState {
            promises: Vec::new(),
            promise_count: 0
        }
    }
}

#[derive(Debug)]
/// Represents a proposer/primary/leader
pub struct Proposer<T> {
    pub ctx: NodeContext<T>,
    pub quorum_size: usize,
    pub to_be_proposed: Vec<T>,
    pub next_proposal_number: ProposalNumber,

    // a proposer might propose
    quorum_resolver: HashMap<(ProposalNumber, Slot), ProposeResolutionState<T>>
}

impl <T> Proposer <T> where T: Clone + std::fmt::Debug {
    pub fn id(&self) -> Id {
        self.ctx.my_id
    }

    pub fn new(ctx: NodeContext<T>, quorum_size: usize) -> Proposer<T> {
        let leader_id = ctx.my_id;
        Proposer {
            ctx,
            quorum_size,
            to_be_proposed: Vec::new(),
            next_proposal_number: ProposalNumber {
                leader_id,
                num: 0
            },
            quorum_resolver: HashMap::new()
        }
    }

    /// To be used by clients in order to propose a value. Note these values have low priority, and
    /// the proposer will adopt a different value if available when receiving promises, or upon
    /// a view change(catching up)
    pub fn push_value_to_be_proposed(&mut self, value: T) {
        self.to_be_proposed.push(value)
    }

    /// Phase 1a - sending prepare messages
    pub fn prepare(&mut self, slot: Slot) {
        if self.to_be_proposed.is_empty() {
            error!("called prepare when there are no values to propose");
            return
        }

        self.next_proposal_number.num += 1;
        let proposal_number = self.next_proposal_number;
        assert!(!self.quorum_resolver.contains_key(&(proposal_number, slot)));

        let resolution_state = ProposeResolutionState::new();
        self.quorum_resolver.insert((proposal_number, slot), resolution_state);
        self.ctx.send_broadcast(MessagePayload::Prepare(proposal_number, slot));
    }

    /// Phase 2a - sending accept message
    #[tracing::instrument]
    pub fn accept(&mut self, proposal_number: ProposalNumber, slot: Slot, accepted_proposals: &[Proposal<T>])
    {
        let result = self.quorum_resolver.remove(&(proposal_number, slot)).unwrap();
        assert_eq!(result.promise_count, self.quorum_size);

        // try accepting a value which was previously accepted at that slot, with the highest
        // proposal number, otherwise, use our own value
        let value = accepted_proposals
            .iter()
            .max_by_key(|p| p.number)
            .map(|p| p.value.clone())
            .unwrap_or_else(|| self.to_be_proposed.remove(0));

        info!("Asking to accept value {:?}", value);

        self.ctx.send_broadcast(MessagePayload::Accept(Proposal { number: proposal_number, slot, value }));
    }

    pub fn on_promise(&mut self, proposal_number: ProposalNumber, slot: Slot, accepted_proposal: Option<Proposal<T>>) {
        // ignore promises not meant for us
        if proposal_number != self.next_proposal_number {
            return
        }

        if let Some(mut entry) = self.quorum_resolver.get_mut(&(proposal_number, slot)) {
            entry.promises.extend(accepted_proposal.into_iter());
            entry.promise_count += 1;
            // we have a majority of promises
            if entry.promise_count == self.quorum_size {
                let entry = self.quorum_resolver.remove(&(proposal_number, slot)).unwrap();
                self.accept(proposal_number, slot, &*entry.promises);
            }

        } else {
            error!("Received promise for non existing proposal {:?} of slot {:?}", proposal_number, slot)
        }
    }

    pub fn on_accepted(&mut self, proposal: Proposal<T>) {
        if !self.to_be_proposed.is_empty() {
        }
    }

}

pub mod tests {
    use super::*;
    use futures::task::Poll;
    use tokio_test::{assert_pending, assert_ready};
    use futures::TryFutureExt;

    fn successful_promise_quorum() {
        let (sender, _receiver) = std::sync::mpsc::channel();
        let ctx = NodeContext {
            my_id: 0, sender
        };
        let quorum_size = 3;
        let mut proposer =  Proposer::<u16>::new(ctx, quorum_size);
        let proposal_number = proposer.next_proposal_number;
        proposer.push_value_to_be_proposed(1337);


    }
}