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
use tracing_futures::{Instrument};

/// Size of applied commit notification channel. Note that in case of lagging receivers(clients), they will never block
/// the node from sending values, but they might lose some commit notifications - see https://docs.rs/tokio/0.3.5/tokio/sync/broadcast/index.html#lagging
const BROADCAST_CHAN_SIZE: usize = 1024;

#[async_trait]
pub trait StateMachine<V: Value, T: Transport<V>>: Default + Debug + 'static + Send {
    /// Updates the state machine state, done asynchronously
    async fn apply(&mut self, entry: &V) -> V::Result;


    /// Spawns the state machine loop, setting up communication
    fn spawn(mut self, mut entry_rx: mpsc::UnboundedReceiver<CommitEntry<V>>) -> (JoinHandle<()>, broadcast::Sender<(CommitEntry<V>, V::Result)>) {
        let (res_tx, res_rx) = broadcast::channel(BROADCAST_CHAN_SIZE); 
        let res_tx2 = res_tx.clone();
        let jh = tokio::spawn(async move {
            let mut last_applied: Option<usize> = None;
            while let Some(entry) = entry_rx.recv().await {
                let new_last_applied = last_applied.map(|i| i + 1).unwrap_or(0);
                assert_eq!(new_last_applied, entry.index, "Commit entry index should equal new last applied");
                let out = self.apply(&entry.value).await;
                res_tx.send((entry, out)).unwrap_or_else(|_e| {
                    error!("Couldn't broadcast result of state machine, no one is subscribed");
                    0
                });
                last_applied = Some(new_last_applied);

            }
        }.instrument(info_span!("state machine")).instrument(info_span!("state machine")));
        (jh, res_tx2)
    }
}

#[derive(Debug)]
pub struct NoopStateMachine();



#[async_trait]
impl <V: Value, T: Transport<V>> StateMachine<V, T> for NoopStateMachine  {
    async fn apply(&mut self, _entry: &V) -> V::Result  {
        unimplemented!();
    }
}

impl Default for NoopStateMachine {
    fn default() -> Self {
        NoopStateMachine()
    }
}