use std::collections::BTreeMap;

/// Id of a node
pub type Id = u64;

/// Index of log entry
pub type Slot = usize;

/// A replica's view of the log, which will never have holes
pub type Log<T> = Vec<T>;

/// A proposal
#[derive(Debug, Clone)]
pub struct Proposal<T> {
    pub slot: Slot,
    pub value: T
}


/// Wraps a consensus message
pub struct ConsensusMessage<T> {
    /// ID of node that sent the message
    pub from: Id,

    /// None for broadcast
    pub to: Option<Id>,

    /// The actual message
    pub payload: MessagePayload<T>
}


/// A consensus message between nodes
pub enum MessagePayload<T> {
    Propose(Proposal<T>),
    Ack,
    AlreadyCommitted { next_slot: Slot }
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
#[derive(Clone, Debug)]
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