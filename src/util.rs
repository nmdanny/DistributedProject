
use tokio_serde::formats::{Json, SymmetricalJson};
use tokio_util::codec::{LengthDelimitedCodec, FramedWrite, FramedRead};
use tokio_serde::{Framed, SymmetricallyFramed};
use tokio::net::TcpStream;
use crate::{WrappedClientRequest, WrappedServerResponse};
use serde::{Serialize, Deserialize};
use tokio::net::tcp::{OwnedWriteHalf, OwnedReadHalf};
use futures::prelude::*;

pub type PeerStream = tokio_serde::Framed<tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec>, WrappedClientRequest, WrappedServerResponse, Json::<WrappedClientRequest, WrappedServerResponse>>;

pub type ServerStream = tokio_serde::Framed<tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec>, WrappedServerResponse, WrappedClientRequest, Json::<WrappedServerResponse, WrappedClientRequest>>;

pub fn create_peer_stream(conn: TcpStream) -> PeerStream {
    let length_delimited = tokio_util::codec::Framed::new(conn, LengthDelimitedCodec::new());
    let deserialized : Framed<_, WrappedClientRequest, WrappedServerResponse, _>= tokio_serde::Framed::new(
        length_delimited,
        Json::<WrappedClientRequest, WrappedServerResponse>::default(),
    );
    deserialized
}

pub fn create_server_stream(conn: TcpStream) -> ServerStream {
    let length_delimited = tokio_util::codec::Framed::new(conn, LengthDelimitedCodec::new());
    let serialized: Framed<_, WrappedServerResponse, WrappedClientRequest, _> =
        tokio_serde::Framed::new(
            length_delimited,
            Json::<WrappedServerResponse, WrappedClientRequest>::default(),
        );
    serialized
}

type FramedJsonRead<T> = SymmetricallyFramed<FramedRead<OwnedReadHalf, LengthDelimitedCodec>, T, Json<T, T>>;

type FramedJsonWrite<T> = SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>, T, Json<T, T>>;

fn framed_json_read<'de, T: Deserialize<'de>>(sock: OwnedReadHalf) -> FramedJsonRead<T> {
    let length_delimited = FramedRead::new(sock, LengthDelimitedCodec::new());
    tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::<T>::default())
}

fn framed_json_write<T: Serialize>(sock: OwnedWriteHalf) -> FramedJsonWrite<T> {
    let length_delimited = FramedWrite::new(sock, LengthDelimitedCodec::new());
    tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::<T>::default())
}


pub type ReqReadStream = FramedJsonRead<WrappedClientRequest>;
pub type ReqWriteStream = FramedJsonWrite<WrappedClientRequest>;

pub type ResReadStream = FramedJsonRead<WrappedServerResponse>;
pub type ResWriteStream = FramedJsonWrite<WrappedServerResponse>;

pub fn split_peer_stream(stream: TcpStream) -> (ReqReadStream, ResWriteStream)
{
    let (read, write) = stream.into_split();
    (framed_json_read(read), framed_json_write(write))
}

pub fn split_server_stream(stream: TcpStream) -> (ResReadStream, ReqWriteStream)
{
    let (read, write) = stream.into_split();
    (framed_json_read(read), framed_json_write(write))
}


pub fn spawn_and_log_error<F, E>(fut: F) -> tokio::task::JoinHandle<()>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display
{
    tokio::spawn(async move {
        if let Err(e) = fut.await {
            error!("{}", e)
        }
    })
}