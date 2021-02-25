use crate::consensus::types::*;
use anyhow::Error;
use serde::de::DeserializeOwned;
use crate::consensus::node_communicator::NodeCommunicator;
use tokio::time;
use rand::distributions::{Distribution, Uniform};
use tracing::Instrument;
use tokio::time::Duration;
use async_trait::async_trait;
use color_eyre::eyre::ContextCompat;
use crate::consensus::timing::{CLIENT_RETRY_DELAY_RANGE, CLIENT_TIMEOUT};
use tokio::time::timeout;
use derivative;
use futures::{Stream, TryFutureExt};
use std::{pin::Pin, rc::Rc};
use std::sync::Arc;

pub type EventStream<EventType> = Pin<Box<dyn Send + Stream<Item = EventType>>>;

/// Responsible for communicating between a client and a `NodeCommunicator`
#[async_trait]
pub trait ClientTransport<V: Value> : 'static + Clone + Send + Sync {
    async fn submit_value(&self, node_id: usize, value: V) -> Result<ClientWriteResponse<V>,RaftError>;

    async fn request_values(&self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError>;

    async fn force_apply(&self, node_id: usize, value: V) -> Result<ClientForceApplyResponse<V>, RaftError>;

    async fn get_sm_event_stream<EventType: Value>(&self, _node_id: usize) -> Result<EventStream<EventType>, RaftError>;
}

#[derive(Clone)]
pub struct SingleProcessClientTransport<V: Value>
{
    communicators: Arc<Vec<NodeCommunicator<V>>>,
    timeout_duration: Duration
}

impl <V: Value> SingleProcessClientTransport<V> {
    pub fn new(communicators: Vec<NodeCommunicator<V>>) -> Self {
        SingleProcessClientTransport {
            communicators: Arc::new(communicators),
            timeout_duration: CLIENT_TIMEOUT
        }
    }
}

#[async_trait]
impl <V: Value> ClientTransport<V> for SingleProcessClientTransport<V> {
    async fn submit_value(&self, node_id: usize, value: V) -> Result<ClientWriteResponse<V>,RaftError> {
        let fut = self.communicators.get(node_id).expect("Invalid node ID").submit_value(ClientWriteRequest {
            value
        });
        timeout(self.timeout_duration, fut).map_err(|_| RaftError::TimeoutError(
            anyhow::anyhow!("client did not receive response to submit_value in enough time")
        )).await?
    }

    async fn request_values(&self, node_id: usize, from: Option<usize>, to: Option<usize>) -> Result<ClientReadResponse<V>, RaftError> {
        let fut = self.communicators.get(node_id).expect("Invalid node ID").request_values(ClientReadRequest {
            from, to
        });
        timeout(self.timeout_duration, fut).map_err(|_| RaftError::TimeoutError(
            anyhow::anyhow!("client did not receive response to request_values in enough time")
        )).await?

    }

    async fn force_apply(&self, node_id: usize, value: V) -> Result<ClientForceApplyResponse<V>, RaftError> {
        let fut = self.communicators.get(node_id).expect("Invalid node ID").force_apply(ClientForceApplyRequest {
            value
        });
        timeout(self.timeout_duration, fut).map_err(|_| RaftError::TimeoutError(
            anyhow::anyhow!("client did not receive response to force_apply in enough time")
        )).await?
    }

    async fn get_sm_event_stream<EventType: Value>(&self, node_id: usize) -> Result<EventStream<EventType>, RaftError> {
        let event_rec = self.communicators.get(node_id).expect("Invlid node ID").state_machine_output_channel::<EventType>();
        let event_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(event_rec);
        Ok(Box::pin(event_stream))
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
/// A generic client implementation that ensures it doesn't send duplicate requests when re-sending
/// values(by using their equality definition)
pub struct Client<T: ClientTransport<V>, V: Value> {

    /// Used to identify the client, for debugging purposes
    pub client_name: String,

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
    pub fn new(client_name: String, transport: T, num_nodes: usize) -> Self {
        Client {
            client_name,
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
                                .map(|i| i + index)
                                .unwrap_or(index));
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
                    warn!(commit_index = ?commit_index, last_commit_index = ?self.last_commit_index,
                          ">>> client read request - bad range, how is this possible?");
                    self.last_commit_index = None.max(commit_index);
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
            let delay_ms = self.retry_delay_ms.sample(&mut rand::thread_rng());
            time::sleep(Duration::from_millis(delay_ms)).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't ensure value is not committed after maximal number of retries"))
    }

    #[instrument]
    /// Submits a value, waits for it to be applied to the state machine and returns the insertion index along with the machine result
    /// (In case the value was already applied before, None is returned instead of a result)
    /// In case of failure, it might retry up to `max_retries` before responding with an error.
    /// Moreover, in case of failure it ensures that its value wasn't already proposed
    pub async fn submit_value(&mut self, value: V) -> Result<(usize, Option<V::Result>), Error>
    {
        let mut attempt = 0;

        while attempt <= self.max_retries {

            if attempt > 0 && self.is_value_already_committed(&value).await? {
                return Ok((self.last_commit_index.unwrap(), None))
            }

            info!("Sending {:?} to suspected leader {:?}", value, self.leader);
            let res = self.transport.submit_value(self.leader, value.clone()).await;
            match res {
                Ok(ClientWriteResponse::Ok { commit_index, sm_output} ) => {
                    // TODO return value maybe
                    self.last_commit_index = Some(commit_index);
                    return Ok((commit_index, Some(sm_output)));
                },
                Ok(ClientWriteResponse::NotALeader {leader_id }) => {
                    warn!("Not a leader, setting new leader {:?}", leader_id);
                    self.set_leader(leader_id);
                }
                Err(RaftError::NetworkError(e)) => {
                    error!(net_err=true, ">>> client encountered network error: {}", e);
                    self.set_leader(None);
                },
                Err(RaftError::TimeoutError(e)) => {
                    error!(">>> client encountered timeout error: {}", e);
                    self.set_leader(None);
                }
                Err(e) => {
                    error!(">>> client received error: {}", e);
                    self.set_leader(None);
                }
            }
            let delay_ms = self.retry_delay_ms.sample(&mut rand::thread_rng());
            time::sleep(Duration::from_millis(delay_ms)).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't submit value after maximal number of retries"))
    }

    pub async fn submit_without_commit(&self, node_id: usize, value: V) -> Result<V::Result, Error> {
        let mut attempt = 0;
        while attempt <= self.max_retries {
            let res = self.transport.force_apply(node_id, value.clone()).await;
            match res {
                Err(RaftError::NetworkError(e)) => {
                    trace!(net_err=true, ">>> client encountered network error: {}", e);
                },
                Err(e) => {
                    error!(">>> client received error: {}, giving up request.", e);
                    return Err(e.into());
                },
                Ok(res) => {
                    return Ok(res.result)
                }
            }
            let retry_duration = self.retry_delay_ms.sample(&mut rand::thread_rng());
            time::sleep(Duration::from_millis(retry_duration)).await;
            attempt += 1;
        }
        Err(anyhow::anyhow!("Couldn't apply value after maximal number of retries"))
    }
}