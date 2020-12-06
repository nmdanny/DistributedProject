use crate::consensus::types::*;
use anyhow::Error;
use crate::consensus::node_communicator::NodeCommunicator;
use tokio::time;
use rand::distributions::{Distribution, Uniform};
use tracing::Instrument;
use tokio::time::Duration;
use async_trait::async_trait;
use color_eyre::eyre::ContextCompat;

/// Responsible for communicating between a client and a `NodeCommunicator`
#[async_trait(?Send)]
pub trait ClientTransport<V: Value> {
    async fn submit_value(&mut self, node_id: usize, value: V) -> Result<ClientWriteResponse, RaftError>;

    async fn request_values(&mut self, node_id: usize, from: usize, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError>;
}

pub struct SingleProcessClientTransport<V: Value>
{
    communicators: Vec<NodeCommunicator<V>>
}

impl <V: Value> SingleProcessClientTransport<V> {
    pub fn new(communicators: Vec<NodeCommunicator<V>>) -> Self {
        SingleProcessClientTransport {
            communicators
        }
    }
}

#[async_trait(?Send)]
impl <V: Value> ClientTransport<V> for SingleProcessClientTransport<V> {
    async fn submit_value(&mut self, node_id: usize, value: V) -> Result<ClientWriteResponse, RaftError> {
        self.communicators.get(node_id).expect("Invalid node ID").submit_value(ClientWriteRequest {
            value
        }).await
    }

    async fn request_values(&mut self, node_id: usize, from: usize, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError> {
        self.communicators.get(node_id).expect("Invalid node ID").request_values(ClientReadRequest {
            from, to
        }).await
    }
}


/// A generic client implementation that ensures it doesn't send duplicate requests when re-sending
/// values(by using their equality definition)
pub struct Client<T: ClientTransport<V>, V: Value> {

    /// Used to communicate with the Node
    pub transport: T,

    /// Current leader
    leader: Id,

    /// Numbers of nodes
    num_nodes: usize,

    /// The lowest possible index at which the value might occur.
    min_commit_index: usize,

    /// Maximal number of retries before giving up
    pub max_retries: usize,

    /// The number of time(in ms) to wait between re-try attempts
    pub retry_delay_ms: Uniform<u64>,

    phantom: std::marker::PhantomData<V>
}

impl <V: Value + PartialEq, T: ClientTransport<V>> Client<T, V>
{
    pub fn new(transport: T, num_nodes: usize) -> Self {
        Client {
            transport,
            leader: 0,
            num_nodes,
            min_commit_index: 0,
            max_retries: 100,
            retry_delay_ms: Uniform::new(300, 800),
            phantom: Default::default()
        }
    }

    fn num_nodes(&self) -> usize {
        self.num_nodes
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
            let values = self.transport.request_values(self.leader,
                                                       self.min_commit_index, None).await;

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

            let res = self.transport.submit_value(self.leader, value.clone()).await;
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