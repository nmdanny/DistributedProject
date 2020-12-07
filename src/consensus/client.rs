use crate::consensus::types::*;
use anyhow::Error;
use crate::consensus::node_communicator::NodeCommunicator;
use tokio::time;
use rand::distributions::{Distribution, Uniform};
use tracing::Instrument;
use tokio::time::Duration;
use async_trait::async_trait;
use color_eyre::eyre::ContextCompat;
use crate::consensus::timing::CLIENT_RETRY_DELAY_RANGE;
use derivative;

/// Responsible for communicating between a client and a `NodeCommunicator`
#[async_trait(?Send)]
pub trait ClientTransport<V: Value> {
    async fn submit_value(&mut self, node_id: usize, value: V) -> Result<ClientWriteResponse, RaftError>;

    async fn request_values(&mut self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError>;
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

    async fn request_values(&mut self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError> {
        self.communicators.get(node_id).expect("Invalid node ID").request_values(ClientReadRequest {
            from, to
        }).await
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
/// A generic client implementation that ensures it doesn't send duplicate requests when re-sending
/// values(by using their equality definition)
pub struct Client<T: ClientTransport<V>, V: Value> {

    #[derivative(Debug="ignore")]
    /// Used to communicate with the Node
    pub transport: T,

    /// Current leader
    leader: Id,

    #[derivative(Debug="ignore")]
    /// Numbers of nodes
    num_nodes: usize,

    /// The last commit index of a value that the client has submitted
    last_commit_index: Option<usize>,

    #[derivative(Debug="ignore")]
    /// Maximal number of retries before giving up
    pub max_retries: usize,

    #[derivative(Debug="ignore")]
    /// The number of time(in ms) to wait between re-try attempts
    pub retry_delay_ms: Uniform<u64>,

    #[derivative(Debug="ignore")]
    phantom: std::marker::PhantomData<V>
}

impl <V: Value + PartialEq, T: ClientTransport<V>> Client<T, V>
{
    pub fn new(transport: T, num_nodes: usize) -> Self {
        Client {
            transport,
            leader: 0,
            num_nodes,
            last_commit_index: None,
            max_retries: 100,
            retry_delay_ms: Uniform::from(CLIENT_RETRY_DELAY_RANGE),
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
    #[instrument]
    async fn is_value_already_committed(&mut self, value: &V) -> Result<bool, Error> {

        let mut attempt = 0;

        while attempt <= self.max_retries {


            let values = self.transport.request_values(self.leader,
                                                       self.last_commit_index, None).await;

            match values {
                Ok(ClientReadResponse::Ok { range} ) => {
                    let val_index_in_range = range.iter().position(|val|  val == value);

                    return match val_index_in_range {
                        Some(index) => {
                            self.last_commit_index = Some(self.last_commit_index
                                .map(|i| i + 1)
                                .unwrap_or(0) + index);
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
                    self.last_commit_index = None;
                }
                Err(RaftError::NetworkError(e)) => {
                    error!(net_err=true, ">>> client encountered networking error: {}", e);
                    self.set_leader(None);
                }
                Err(e) =>
                {
                    error!(">>> Client has determined that {} has an error: {}",
                           self.leader, e);
                    self.set_leader(None);
                }
            }
            time::delay_for(Duration::from_millis(self.retry_delay_ms.sample(
                &mut rand::thread_rng()))).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't ensure value is not committed after maximal number of retries"))
    }

    #[instrument]
    /// Submits a value, waits for it to be committed and returns the insertion index.
    /// In case of failure, it might retry up to `max_retries` before responding with an error.
    /// Moreover, in case of failure it ensures that its value wasn't already proposed
    pub async fn submit_value(&mut self, value: V) -> Result<usize, Error>
    {
        let mut attempt = 0;

        while attempt <= self.max_retries {

            if attempt > 0 && self.is_value_already_committed(&value).await? {
                return Ok(self.last_commit_index.unwrap())
            }

            let res = self.transport.submit_value(self.leader, value.clone()).await;
            match res {
                Ok(ClientWriteResponse::Ok { commit_index} ) => {
                    self.last_commit_index = Some(commit_index);
                    return Ok(commit_index);
                },
                Ok(ClientWriteResponse::NotALeader {leader_id }) => {
                    self.set_leader(leader_id);
                }
                Err(RaftError::NetworkError(e)) => {
                    error!(net_err=true, ">>> client encountered network error: {}", e);
                    self.set_leader(None);
                },
                Err(e) => {
                    error!(">>> client received error: {}", e);
                    self.set_leader(None);
                }
            }
            time::delay_for(Duration::from_millis(self.retry_delay_ms.sample(
                &mut rand::thread_rng()))).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't submit value after maximal number of retries"))
    }
}