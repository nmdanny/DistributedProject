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

const BROADCAST_CHAN_SIZE: usize = 1024;

#[async_trait]
pub trait StateMachine<V: Value, T: Transport<V>>: Default + Debug + 'static + Send {

    type Output: Send;

    /// Updates the state machine state, done asynchronously
    async fn apply(&mut self, entry: &V) -> Self::Output;


    /// Spawns the state machine loop, setting up communication
    fn spawn(mut self, mut entry_rx: mpsc::UnboundedReceiver<V>) -> (JoinHandle<()>, broadcast::Receiver<(usize, Self::Output)>) {
        let (res_tx, res_rx) = broadcast::channel(BROADCAST_CHAN_SIZE); 
        let jh = tokio::spawn(async move {
            let mut last_applied: Option<usize> = None;
            while let Some(val) = entry_rx.recv().await {
                let new_last_applied = last_applied.map(|i| i + 1).unwrap_or(0);
                let out = self.apply(&val).await;
                res_tx.send((new_last_applied, out)).unwrap_or_else(|_e| {
                    error!("Couldn't broadcast result of state machine, no one is subscribed");
                    0
                });
                last_applied = Some(new_last_applied);

            }
        }.instrument(info_span!("state machine")).instrument(info_span!("state machine")));
        (jh, res_rx)
    }
}

#[derive(Debug)]
pub struct NoopStateMachine();



#[async_trait]
impl <V: Value, T: Transport<V>> StateMachine<V, T> for NoopStateMachine {
    type Output = ();
    async fn apply(&mut self, _entry: &V) {
    }
}

impl Default for NoopStateMachine {
    fn default() -> Self {
        NoopStateMachine()
    }
}