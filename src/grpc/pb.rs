use std::{convert::{TryFrom, TryInto}, pin::Pin};

use futures::{Stream};
use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;
use tonic;
use serde_json::to_vec;
use serde_json::from_slice;
use anyhow::Context;
mod inner {
    std::include!(concat!(env!("OUT_DIR"),concat!("/","raft",".rs")));
}

use crate::consensus::types::*;
use crate::consensus::client::EventStream;

pub use inner::*;

pub type TypeConversionError = serde_json::Error;

macro_rules! gen_generic {
    ($typee:ty, $($par:tt)?) => {

impl <$($par: Value)?> TryFrom<inner::GenericMessage> for $typee {
    type Error = serde_json::Error;

    fn try_from(value: inner::GenericMessage) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value.buf)
    }
}

impl <$($par: Value)?> TryFrom<$typee> for inner::GenericMessage {
    type Error = serde_json::Error;

    fn try_from(value: $typee) -> Result<Self, Self::Error> {
        Ok(inner::GenericMessage {
            buf: serde_json::to_vec_pretty(&value)?
        })
    }
}
        
    };
}

pub fn raft_to_tonic<T: TryInto<inner::GenericMessage, Error = TypeConversionError>>(raft_result: Result<T, RaftError>) -> Result<tonic::Response<inner::GenericMessage>, tonic::Status> {
    match raft_result {
        Ok(raft_result) => { 
            let ser = raft_result.try_into().map_err(|se| tonic::Status::internal(format!("serialization error: {:?}", se)))?;
            Ok(tonic::Response::new(ser))
        },
        Err(RaftError::NetworkError(err)) => {
            let res = tonic::Status::unavailable(format!("network error: {:?}", err));
            Err(res)
        },
        Err(RaftError::NoLongerLeader()) => {
            let res = tonic::Status::aborted("Not a leader");
            Err(res)
        },
        Err(RaftError::InternalError(err)) => {
            let res = tonic::Status::internal(format!("internal error: {:?}", err));
            Err(res)
        },
        Err(RaftError::TimeoutError(err)) => {
            let res = tonic::Status::deadline_exceeded(format!("timeout error: {:?}", err));
            Err(res)
        },
        Err(RaftError::CommunicatorError(err)) => {
            let res = tonic::Status::internal(format!("communicator error: {:?}", err));
            Err(res)
        }
    }
}

fn tonic_status_to_raft(status: tonic::Status) -> RaftError {
    match status.code() {
        tonic::Code::Unavailable => RaftError::NetworkError(anyhow::anyhow!("{}", status.message())),
        tonic::Code::Aborted=> RaftError::NoLongerLeader(),
        tonic::Code::Internal if status.message().starts_with("communicator") => RaftError::CommunicatorError(anyhow::anyhow!("{}", status.message())),
        tonic::Code::Internal => RaftError::InternalError(anyhow::anyhow!("{}", status.message())),
        tonic::Code::DeadlineExceeded => RaftError::TimeoutError(anyhow::anyhow!("{}", status.message())),
        _ if status.message().contains("tcp") => RaftError::NetworkError(anyhow::anyhow!("{}", status.message())),
        _ if status.message().contains("timed out") => RaftError::TimeoutError(anyhow::anyhow!("{}", status.message())),
        c => RaftError::InternalError(anyhow::anyhow!("unknown error, code: {}, message: {}", c, status.message()))
    }
}

pub fn tonic_to_raft<T: TryFrom<inner::GenericMessage, Error = TypeConversionError>>(tonic_result: Result<tonic::Response<inner::GenericMessage>, tonic::Status>, to: Id) -> Result<T, RaftError> {
    match tonic_result {
        Ok(res) => {
            let res = res.into_inner().try_into().context("deserialization error").map_err(|se| RaftError::InternalError(se))?;
            Ok(res)
        }
        Err(status) => {
            let mut raft_err = tonic_status_to_raft(status);
            if let RaftError::NetworkError(e) = raft_err {
                raft_err = RaftError::NetworkError(e.context(format!("from request to {}", to)));
            }
            Err(raft_err)
        }
    }
}


pub fn tonic_stream_to_raft<EventType: Value>(tonic_result: Result<tonic::Response<tonic::Streaming<inner::GenericMessage>>, tonic::Status>) 
    -> Result<EventStream<EventType>, RaftError> {
    match tonic_result {
        Ok(res) => {
            Ok(Box::pin(res.into_inner().filter_map(|res| {
                match res {
                    Ok(generic_message) => {
                        let deser = serde_json::from_slice::<EventType>(&generic_message.buf);
                        match deser {
                            Ok(event) => { Some(event) }
                            Err(err) => {
                                error!("Couldn't deserialize during tonic_stream_to_raft: {:?}", err);
                                None
                            }
                        }
                    }
                    Err(err) => { 
                        error!("There was an error while receiving stream-element during tonic_stream_to_raft: {:?}", err);
                        None
                    }
                }
                })))
            },
            Err(status) => {
                Err(tonic_status_to_raft(status))
            }
    }
}


gen_generic!(RequestVote,);
gen_generic!(RequestVoteResponse,);
gen_generic!(AppendEntries<V>, V);
gen_generic!(AppendEntriesResponse,);
gen_generic!(ClientWriteRequest<V>, V);
gen_generic!(ClientWriteResponse<V>, V);
gen_generic!(ClientReadRequest,);
gen_generic!(ClientReadResponse<V>, V);
gen_generic!(ClientForceApplyRequest<V>, V);
gen_generic!(ClientForceApplyResponse<V>, V);
