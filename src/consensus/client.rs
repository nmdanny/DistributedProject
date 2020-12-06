use crate::consensus::types::*;
use anyhow::Error;
use crate::consensus::node_communicator::NodeCommunicator;
use tokio::time;
use rand::distributions::{Distribution, Uniform};
use tracing::Instrument;
use tokio::time::Duration;


/// Implementation of a client that lives in the same process as the Raft Node and uses
/// the `NodeCommunicator` to communicate with him
pub struct SingleProcessClient<V: Value> {

    /// Used to communicate with the Node
    communicators: Vec<NodeCommunicator<V>>,

    /// Current leader
    leader: Id,

    /// The lowest possible index at which the value might occur.
    min_commit_index: usize,

    /// Maximal number of retries before giving up
    pub max_retries: usize,

    /// The number of time(in ms) to wait between re-try attempts
    pub retry_delay_ms: Uniform<u64>
}

impl <V: Value + Eq> SingleProcessClient<V>
{
    pub fn new(communicators: Vec<NodeCommunicator<V>>) -> Self {
        SingleProcessClient {
            communicators,
            leader: 0,
            min_commit_index: 0,
            max_retries: 100,
            retry_delay_ms: Uniform::new(300, 800)
        }
    }

    fn num_nodes(&self) -> usize {
        self.communicators.len()
    }

    /// To be called when redirected to a different leader,
    /// in case the leader is unkown, it will guess randomly.
    fn set_leader(&mut self, leader: Option<Id>)
    {
        if let Some(leader) = leader {
            self.leader = leader;
        } else {
            self.leader = Uniform::from(0 .. self.num_nodes()).sample(&mut rand::thread_rng());
        }

    }

    /// Ensures that a given value is not committed
    #[instrument(skip(self))]
    async fn is_value_already_committed(&mut self, value: &V) -> Result<bool, Error> {

        let mut attempt = 0;

        while attempt <= self.max_retries {
            let values = self.communicators[self.leader].request_values(
                ClientReadRequest { from: self.min_commit_index, to: None}
            ).await;

            match values {
                Ok(ClientReadResponse::Ok { range} ) => {
                    let val_index_in_range = range.iter().position(|val|  val == value);
                    // now we know that 'min_commit_index' (index of range start) + range.len()
                    // gives us the index of one element beyond the end of the committed log
                    self.min_commit_index = self.min_commit_index + range.len() - 1;

                    return match val_index_in_range {
                        Some(index) => {
                            Ok(true)
                        }
                        None => {
                            Ok(false)
                        }
                    }

                }
                Ok(ClientReadResponse::NotALeader { leader_id }) => {
                    self.set_leader(leader_id);
                },
                Ok(ClientReadResponse::BadRange { commit_index }) => {
                    warn!(commit_index = ?commit_index, ">>> client read request - bad range, how is this possible?");
                    self.min_commit_index = self.min_commit_index.max(commit_index.unwrap_or(0));
                    return Ok(true);
                }
                Err(e) => {
                    error!(">>> client encountered error: {}", e);
                }
            }
            time::delay_for(Duration::from_millis(self.retry_delay_ms.sample(
                &mut rand::thread_rng()))).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't ensure value is not committed after maximal number of retries"))
    }

    #[instrument(skip(self))]
    /// Submits a value, waits for it to be committed and returns the insertion index.
    /// In case of failure, it might retry up to `max_retries` before responding with an error.
    /// Moreover, in case of failure it ensures that its value wasn't already proposed
    pub async fn submit_value(&mut self, value: V) -> Result<(), Error>
    {
        let mut attempt = 0;

        while attempt <= self.max_retries {

            if attempt > 0 && self.is_value_already_committed(&value).await? {
                return Ok(())
            }

            let res  = self.communicators[self.leader].submit_value(
                ClientWriteRequest { value: value.clone()}
            ).await;
            match res {
                Ok(ClientWriteResponse::Ok { commit_index} ) => {
                    self.min_commit_index = commit_index + 1;
                    return Ok(());
                },
                Ok(ClientWriteResponse::NotALeader {leader_id }) => {
                    if let Some(leader) = leader_id {
                        self.set_leader(leader_id);
                    }
                }
                Err(e) => {
                    error!(">>> client encountered error: {}", e);
                }
            }
            time::delay_for(Duration::from_millis(self.retry_delay_ms.sample(
                &mut rand::thread_rng()))).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't submit value after maximal number of retries"))
    }
}