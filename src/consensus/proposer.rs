use crate::consensus::types::*;
use std::sync::mpsc::Sender;
use std::rc::Rc;
use std::collections::HashMap;
use futures::channel::oneshot;
use std::cell::RefCell;
use std::{error::Error, io};
use tracing::{debug, error, info, span, warn, Level};
use tokio::time::Duration;


pub const HEARTBEAT_FREQ: Duration = Duration::from_secs(1);
pub const LEADER_CHANGE_FREQ: Duration = Duration::from_secs(2);


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

#[derive(Debug)]
/// Represents a proposer/primary/leader
pub struct Proposer<T> {
    pub ctx: NodeContext<T>,
    pub quorum_size: usize,
    pub to_be_proposed: Vec<T>,
    pub next_proposal_number: ProposalNumber,
    pub log: Log<T>,
    propose_resolutions: HashMap<(ProposalNumber, Slot), ProposeResolutionState<T>>
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
            log: Default::default(),
            propose_resolutions: HashMap::new()
        }
    }

    /// To be used by clients in order to propose a value. Note these values have low priority, and
    /// the proposer will adopt a different value if available when receiving promises, or upon
    /// a view change(catching up)
    pub fn push_value_to_be_proposed(&mut self, value: T) {
        self.to_be_proposed.push(value)
    }

    /// Phase 1a - sending prepare message, essentially trying to become a leader
    pub fn prepare(&mut self) {
        self.next_proposal_number.num += 1;
        let proposal_number = self.next_proposal_number;
        let slot = next_slot(&self.log);
        assert!(!self.propose_resolutions.contains_key(&(proposal_number, slot)));

        let resolution_state = ProposeResolutionState::new();
        self.propose_resolutions.insert((proposal_number, slot), resolution_state);
        self.ctx.send_broadcast(MessagePayload::Prepare(proposal_number, slot));
    }

    pub fn send_heartbeat(&mut self) {
        self.ctx.send_broadcast(MessagePayload::Heartbeat(self.id()))
    }

    /// Phase 2a - sending accept message. In case there's nothing to accept, None will be returned
    #[tracing::instrument]
    pub fn accept(&mut self, proposal_number: ProposalNumber, slot: Slot, accepted_proposals: Vec<Proposal<T>>)
    {
        let _resolver_state = self.propose_resolutions.remove(&(proposal_number, slot)).unwrap();

        // try accepting a value which was previously accepted at that slot, with the highest
        // proposal number, otherwise, use our own value
        let previously_accepted_opt = accepted_proposals
                .iter()
                .max_by_key(|p| p.number)
                .map(|p| p.value.clone());

        let value = match previously_accepted_opt {
            Some(value) => {
                info!("asking to accept previously accepted value {:?}", value);
                value
            },
            None => {
                if self.to_be_proposed.is_empty() {
                    warn!("tried to accept my own value, but I have nothing to offer");
                    return
                }
                let value = self.to_be_proposed.remove(0);
                info!("asking to accept my own value {:?}", value);
                value
            }
        };
        self.ctx.send_broadcast(MessagePayload::Accept(Proposal { number: proposal_number, slot, value: value.clone() }));
    }

    pub fn on_promise(&mut self, proposal_number: ProposalNumber, slot: Slot, accepted_proposal: Option<Proposal<T>>) {
        // ignore promises not meant for us
        if proposal_number != self.next_proposal_number {
            return
        }

        if let Some(mut entry) = self.propose_resolutions.get_mut(&(proposal_number, slot)) {
            entry.accepted_proposals.extend(accepted_proposal.into_iter());
            entry.promise_count += 1;

            // we have a majority of promises
            if entry.promise_count == self.quorum_size {
                let entry = self.propose_resolutions.remove(&(proposal_number, slot)).unwrap();
                self.accept(proposal_number, slot, entry.accepted_proposals);
            }

        } else {
            error!("Received promise for non existing proposal {:?} of slot {:?}", proposal_number, slot)
        }
    }

    pub fn on_accepted(&mut self, proposal: Proposal<T>) {
        if !self.to_be_proposed.is_empty() {
            let _prev = self.log.insert(proposal.slot, proposal.value);
            assert!(_prev.is_none());
        }
    }

}

pub mod tests {
    use super::*;
    use futures::task::Poll;
    use tokio_test::{assert_pending, assert_ready};
    use futures::TryFutureExt;

}