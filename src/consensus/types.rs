
/// Id of a node
pub type Id = u64;

/// Index of log entry
pub type Slot = usize;

/// A proposal number for some slot
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProposalNumber {
    /// A strictly increasing number
    pub num: u64,

    /// Identifies the proposer/leader who initiated the vote, allowing tie-breaking in case
    /// multiple partitioned leaders send a proposal with the same number
    pub leader_id: Id
}

/// A proposal
#[derive(Debug, Clone)]
pub struct Proposal<T> {
    pub number: ProposalNumber,
    pub slot: Slot,
    pub value: T
}


/// Wraps a consensus message
pub struct ConsensusMessage<T> {
    /// ID of node that sent the message
    pub from: Id,

    /// None for broadcast
    pub to: Option<Id>,
    pub payload: MessagePayload<T>
}


/// A consensus message between nodes
pub enum MessagePayload<T> {
    /// Sent from proposer(leader) to all acceptors (replicas)
    Prepare(ProposalNumber),

    /// Also called 'ack',sent from an acceptor to the proposer in response to a prepare message
    /// whose proposal number is higher than all other proposals. In case the acceptor has accepted
    /// a previous value, it will be included in the promise
    Promise(ProposalNumber, Vec<Proposal<T>>),

    /// Sent from a proposer to all acceptors once he obtained a majority quorum of promises, includes
    /// a value either originally chosen by the proposer, or the value with the highest proposal number
    /// that was in an acceptor's promise
    Accept(Proposal<T>),

    /// Sent from an acceptor to all proposers and learners after accepting a value
    Accepted(Proposal<T>),

    /// Sent by proposer(leader) to all nodes
    Heartbeat(Id),

    /// Sent by an acceptor upon not receiving a heartbeat from the designated proposer
    Complain(Id),

    /// Sent by acceptors upon receiving a majority quorum of complaints
    LeaveView(Id),

    /// Sent by the new designated proposer
    ViewChange(Id),

    /// Sent by
    Catchup(Vec<T>)
}

impl <T> MessagePayload<T> {
    pub fn wrap_unicast(self, from: Id, to: Id) -> ConsensusMessage<T> {
       ConsensusMessage {
           from,
           to: Some(to),
           payload: self
       }
    }

    pub fn wrap_broadcast(self, from: Id) -> ConsensusMessage<T> {
        ConsensusMessage {
            from,
            to: None,
            payload: self
        }
    }
}

/// Contains fields used by all
#[derive(Clone)]
pub struct NodeContext<T> {
    pub my_id: Id,
    pub sender: std::sync::mpsc::Sender<ConsensusMessage<T>>,
}

impl <T> NodeContext<T> {
    pub fn send_unicast(&mut self, msg: MessagePayload<T>, to: Id) {
        self.sender.send(msg.wrap_unicast(self.my_id, to)).unwrap();
    }

    pub fn send_broadcast(&mut self, msg: MessagePayload<T>) {
        self.sender.send(msg.wrap_broadcast(self.my_id)).unwrap();
    }
}