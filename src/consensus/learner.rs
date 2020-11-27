use crate::consensus::types::Proposal;

/// A learner handles accepted proposals
pub trait Learner<T> {
    fn on_accepted(&mut self, proposal: Proposal<T>);
}

pub struct DummyLearner;

impl <T> Learner<T> for DummyLearner {
    fn on_accepted(&mut self, _proposal: Proposal<T>) {
    }
}

impl <T,F> Learner<T> for F where F: Fn(Proposal<T>) {
    fn on_accepted(&mut self, proposal: Proposal<T>) {
        self(proposal);
    }
}
