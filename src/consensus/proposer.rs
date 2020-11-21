use crate::consensus::types::*;
use std::sync::mpsc::Sender;
use std::rc::Rc;
use std::collections::HashMap;
use futures::channel::oneshot;
use std::cell::RefCell;


struct ProposeResolutionState<T> {
    promises: Vec<Proposal<T>>,
    resolve: Option<oneshot::Sender<()>>,
    promise_count: usize
}

impl <T> ProposeResolutionState<T> {
    pub fn new(resolver: oneshot::Sender<()>) -> ProposeResolutionState<T> {
        ProposeResolutionState {
            promises: Vec::new(),
            resolve: Some(resolver),
            promise_count: 0
        }
    }
}

/// Represents a proposer/primary/leader
pub struct Proposer<T> {
    pub ctx: NodeContext<T>,
    pub quorum_size: usize,
    pub to_be_proposed: Vec<T>,
    pub next_slot: usize,
    pub next_proposal_number: ProposalNumber,
    quorum_resolver: HashMap<(ProposalNumber, Slot), ProposeResolutionState<T>>
}

// pub struct ProposerRef<T>(RefCell<T>);

impl <T> ProposerRef <T> where T: Clone {
    pub fn id(&self) -> Id {
        self.ctx.my_id
    }
    pub fn new(ctx: NodeContext<T>, quorum_size: usize) -> Proposer<T> {
        let leader_id = ctx.my_id;
        Proposer {
            ctx,
            quorum_size,
            to_be_proposed: Vec::new(),
            next_slot: 0,
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

    pub async fn try_propose(&mut self) {
        if self.to_be_proposed.is_empty() {
            return
        }

        let proposal_number = self.next_proposal_number;
        let slot = self.next_slot;

        self.ctx.send_broadcast(MessagePayload::Prepare(proposal_number));

        let result = self.wait_for_promise_quorum(proposal_number, slot).await;
        assert_eq!(result.promise_count, self.quorum_size);

        let value = self.to_be_proposed.remove(0);

        self.ctx.send_broadcast(MessagePayload::Accept(Proposal { number: proposal_number, slot, value }));
        self.next_proposal_number.num += 1;
    }

    async fn wait_for_promise_quorum(&mut self, proposal_number: ProposalNumber, slot: Slot) -> ProposeResolutionState<T> {
        assert!(!self.quorum_resolver.contains_key(&(proposal_number, slot)));
        let (o_sender, o_receiver) = oneshot::channel();

        let resolution_state = ProposeResolutionState::new(o_sender);
        self.quorum_resolver.insert((proposal_number, slot), resolution_state);

        o_receiver.await.unwrap();

        self.quorum_resolver.remove(&(proposal_number, slot)).unwrap()
    }

    pub fn on_promise(&mut self, proposal_number: ProposalNumber, slot: Slot, proposals: Vec<Proposal<T>>) {
        if proposal_number.leader_id != self.id() {
            return
        }
        if let Some(mut entry) = self.quorum_resolver.get_mut(&(proposal_number, slot)) {
            entry.promises.extend(proposals);
            entry.promise_count += 1;
            // we have a majority of promises
            if entry.promise_count == self.quorum_size {
                let resolver = entry.resolve.take().unwrap();
                resolver.send(()).unwrap();
            }

        } else {
            error!("Received promise for non existing proposal {:?} of slot {:?}", proposal_number, slot)
        }

    }

}

pub mod tests {
    use super::*;
    use futures::task::Poll;
    use tokio_test::{assert_pending, assert_ready};
    use futures::TryFutureExt;

    #[tokio::test]
    async fn successful_promise_quorum() {
        let (sender, _receiver) = std::sync::mpsc::channel();
        let ctx = NodeContext {
            my_id: 0, sender
        };
        let quorum_size = 3;
        let mut proposer =  Proposer::<u16>::new(ctx, quorum_size);
        let proposal_number = proposer.next_proposal_number.clone();
        proposer.push_value_to_be_proposed(1337);
        let mut proposer = RefCell::new(proposer);

        let local = tokio::task::LocalSet::new();
        let res = local.spawn_local(
            proposer.borrow_mut().try_propose()
        );

        let mut propose_fut = tokio_test::task::spawn(res);

        assert_pending!(propose_fut.poll());
        proposer.borrow_mut().on_promise(proposal_number, 0, Vec::new());
        assert_pending!(propose_fut.poll());
        proposer.borrow_mut().on_promise(proposal_number, 0, Vec::new());
        assert_ready!(propose_fut.poll());
    }
}