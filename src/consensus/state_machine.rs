use crate::consensus::types::*;
use crate::consensus::node::Node;
use crate::consensus::transport::Transport;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task;
use async_trait::async_trait;
use std::fmt::Debug;
use futures::stream::{Stream, StreamExt};
use tracing_futures::{Instrument};


pub type ForceApply<V> = (ClientForceApplyRequest<V>, oneshot::Sender<Result<ClientForceApplyResponse<V>, RaftError>>);

#[async_trait]
pub trait StateMachine<V: Value, T: Transport<V>>: Debug + 'static + Sized + Send {
    /// Updates the state machine state.
    fn apply(&mut self, entry: &V) -> V::Result;

    type HookEvent: Send;
    type HookStream: Stream<Item = Self::HookEvent> + Send;
    type PublishedEvent: Clone + Send + Debug + Serialize;

    /// Allows hooking into the state machine lifecycle and applying operations
    /// independently of commit entries
    fn create_hook_stream(&mut self) -> Self::HookStream;

    fn handle_hook_event(&mut self, _event: Self::HookEvent) {
    }

    fn get_event_stream(&mut self) -> broadcast::Sender<Self::PublishedEvent> {
        let (tx, _rx) = broadcast::channel(1);
        tx
    }



    // Spawns the state machine loop, setting up communication
    fn spawn(mut self,
             node_id: usize,
             mut entry_rx: mpsc::UnboundedReceiver<CommitEntry<V>>,
             mut force_apply_rx: mpsc::UnboundedReceiver<ForceApply<V>>,
             res_tx: broadcast::Sender<(CommitEntry<V>, V::Result)>) {
        let hook_stream = self.create_hook_stream();
        let _ = crate::util::spawn_on_new_runtime(async move {
            let mut last_applied: Option<usize> = None;
            tokio::pin!(hook_stream);
            loop {
                tokio::select! {
                    Some(event) = hook_stream.next() => {
                        self.handle_hook_event(event);
                    },
                    Some(entry) = entry_rx.recv() => {
                        let new_last_applied = last_applied.map(|i| i + 1).unwrap_or(0);
                        assert_eq!(new_last_applied, entry.index, "Commit entry index should equal new last applied");
                        let out = self.apply(&entry.value);
                        res_tx.send((entry, out)).unwrap_or_else(|_e| {
                            // this is normal for a non-leader node
                            trace!("Couldn't broadcast result of state machine, no one is subscribed");
                            0
                        });
                        last_applied = Some(new_last_applied);
                    },
                    Some(force_apply) = force_apply_rx.recv() => {
                        let result = self.apply(&force_apply.0.value);
                        let resp = ClientForceApplyResponse { result };
                        force_apply.1.send(Ok(resp)).unwrap_or_else(|_e| {
                            error!("Couldn't send force apply result to client");
                        });
                    },
                    else => {}
                }
            }
        }.instrument(info_span!("SM-loop", node.id=?node_id)));
    }
}

#[derive(Debug)]
pub struct NoopStateMachine();



#[async_trait]
impl <V: Value, T: Transport<V>> StateMachine<V, T> for NoopStateMachine  where V::Result : Default {

    type HookEvent = ();
    type HookStream = futures::stream::Empty<()>;
    type PublishedEvent = ();



    fn apply(&mut self, _entry: &V) -> V::Result {
       Default::default() 
    }

    fn create_hook_stream(&mut self) -> Self::HookStream {
        futures::stream::empty()
    }
}

impl Default for NoopStateMachine {
    fn default() -> Self {
        NoopStateMachine()
    }
}