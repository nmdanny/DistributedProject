use crate::consensus::types::*;
use crate::consensus::node::Node;
use crate::consensus::transport::Transport;
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

#[async_trait(?Send)]
pub trait StateMachine<V: Value, T: Transport<V>>: Debug + 'static + Sized {
    /// Updates the state machine state. While this is async for convenience, updates 
    /// are performed sequentially - the state machine won't process the next value before finishing with
    /// the current one
    ///
    /// Be careful not to trigger deadlocks, for example, if the state machines submits a value to the log
    /// and awaits a result(in the same task of `apply`) - the result will never arrive, as the value needs
    /// to be processed by the state machine - the same machine waiting for the result.
    async fn apply(&mut self, entry: &V) -> V::Result;

    type HookEvent;
    type HookStream: Stream<Item = Self::HookEvent>;
    type PublishedEvent: Clone + Send + Debug;

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
    fn spawn(mut self, mut entry_rx: mpsc::UnboundedReceiver<CommitEntry<V>>,
             mut force_apply_rx: mpsc::UnboundedReceiver<ForceApply<V>>,
             res_tx: broadcast::Sender<(CommitEntry<V>, V::Result)>) -> JoinHandle<()> {
        let jh = task::spawn_local(async move {
            let mut last_applied: Option<usize> = None;
            let hook_stream = self.create_hook_stream();
            tokio::pin!(hook_stream);
            loop {
                tokio::select! {
                    Some(event) = hook_stream.next() => {
                        self.handle_hook_event(event);
                    },
                    // TODO: don't await in body, you dense motherfucker
                    Some(entry) = entry_rx.recv() => {
                        info!("handling SM change {:?}", entry);
                        let new_last_applied = last_applied.map(|i| i + 1).unwrap_or(0);
                        assert_eq!(new_last_applied, entry.index, "Commit entry index should equal new last applied");
                        let out = self.apply(&entry.value).await;
                        res_tx.send((entry, out)).unwrap_or_else(|_e| {
                            // this is normal for a non-leader node
                            trace!("Couldn't broadcast result of state machine, no one is subscribed");
                            0
                        });
                        last_applied = Some(new_last_applied);
                    },
                    Some(force_apply) = force_apply_rx.recv() => {
                        let result = self.apply(&force_apply.0.value).await;
                        let resp = ClientForceApplyResponse { result };
                        force_apply.1.send(Ok(resp)).unwrap_or_else(|_e| {
                            error!("Couldn't send force apply result to client");
                        });
                    }
                }
            }
        }.instrument(info_span!("state machine")).instrument(info_span!("state machine")));
        jh
    }
}

#[derive(Debug)]
pub struct NoopStateMachine();



#[async_trait(?Send)]
impl <V: Value, T: Transport<V>> StateMachine<V, T> for NoopStateMachine  where V::Result : Default {

    type HookEvent = ();
    type HookStream = futures::stream::Empty<()>;
    type PublishedEvent = ();



    async fn apply(&mut self, _entry: &V) -> V::Result {
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