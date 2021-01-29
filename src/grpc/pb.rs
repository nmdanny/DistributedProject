use std::convert::{TryFrom, TryInto};

use tonic;
use serde_json::to_vec;
use serde_json::from_slice;
use anyhow::Context;
mod inner {
    include!(concat!(env!("OUT_DIR"),concat!("/","raft",".rs")));
}

use crate::consensus::types::*;

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
            let res = tonic::Status::unavailable(format!("network error: {}", err));
            Err(res)
        },
        Err(RaftError::NoLongerLeader()) => {
            let res = tonic::Status::aborted("Not a leader");
            Err(res)
        },
        Err(RaftError::InternalError(err)) => {
            let res = tonic::Status::internal(format!("internal error: {}", err));
            Err(res)
        },
        Err(RaftError::TimeoutError(err)) => {
            let res = tonic::Status::deadline_exceeded(format!("timeout error: {}", err));
            Err(res)
        },
        Err(RaftError::CommunicatorError(err)) => {
            let res = tonic::Status::internal(format!("communicator error: {}", err));
            Err(res)
        }
    }
}

pub fn tonic_to_raft<T: TryFrom<inner::GenericMessage, Error = TypeConversionError>>(tonic_result: Result<tonic::Response<inner::GenericMessage>, tonic::Status>) -> Result<T, RaftError> {
    match tonic_result {
        Ok(res) => {
            let res = res.into_inner().try_into().context("deserialization error").map_err(|se| RaftError::InternalError(se))?;
            Ok(res)
        }
        Err(status) => {
            Err(match status.code() {
                tonic::Code::Unavailable => RaftError::NetworkError(anyhow::anyhow!("{}", status.message())),
                tonic::Code::Aborted=> RaftError::NoLongerLeader(),
                tonic::Code::Internal if status.message().starts_with("communicator") => RaftError::CommunicatorError(anyhow::anyhow!("{}", status.message())),
                tonic::Code::Internal => RaftError::InternalError(anyhow::anyhow!("{}", status.message())),
                tonic::Code::DeadlineExceeded => RaftError::TimeoutError(anyhow::anyhow!("{}", status.message())),
                _ => RaftError::InternalError(anyhow::anyhow!("{}", status.message()))
            })
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
