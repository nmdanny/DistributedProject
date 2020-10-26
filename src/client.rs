#[macro_use]
extern crate log;

use rand::prelude::*;
use dist_lib::*;
use futures::prelude::*;
use tokio::net::TcpStream;
use tokio::io::{stdin, BufReader};
use tokio::io::{AsyncBufRead,AsyncBufReadExt};
use std::io::BufRead;
use futures::select;

type ResponseTx = futures::channel::oneshot::Sender<WrappedServerResponse>;

struct Client {
    client_id: u64,
    sequence_num: u64,
    cur_log_index: i64, // allows client to ignore duplicate write messages
    next_response: Option<ResponseTx>, // set if we're waiting for another response,
    reader: ResReadStream,
    writer: ReqWriteStream
}

impl Client {
    fn new(reader: ResReadStream, writer: ReqWriteStream) -> Client {
        Client {
            client_id: random(),
            sequence_num: 1,
            cur_log_index: -1,
            next_response: None,
            reader,
            writer
        }
    }

    async fn send_and_await_response(&mut self, req: ClientRequest) -> std::io::Result<WrappedServerResponse> {
        let wrapped = WrappedClientRequest {
            client_id: self.client_id,
            sequence_num: self.sequence_num,
            request: req
        };
        let (tx, rx) = futures::channel::oneshot::channel();
        self.next_response = Some(tx);
        self.writer.send(wrapped).await?;
        let res = rx.await.unwrap();
        self.sequence_num += 1;
        self.next_response = None;
        Ok(res)
    }

    async fn handle_response(&mut self, res: WrappedServerResponse) {
        trace!("got response {:?}", res);
        match &res.response {
            ServerResponse::WriteOk { index, contents} => {
                if *index as i64 <= self.cur_log_index {
                    warn!("received duplicate chat message that was already seen {:?}", res);
                } else {
                    println!("{}: {}", res.client_id, contents);
                    self.cur_log_index += 1;
                }
            },
            ServerResponse::ReadOk { contents } => {
                unimplemented!()
            }
        }
        if res.client_id == self.client_id && res.sequence_num == self.sequence_num {
            if let Some(tx) = self.next_response.take() {
                tx.send(res).unwrap();
            }
        }
    }

}



#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();
    let conn = TcpStream::connect(("127.0.0.1", 8080)).await?;
    let (mut read_h, mut write_h) = split_server_stream(conn);

    let mut client = Client::new(read_h, write_h);

    let mut stdin_reader = BufReader::new(stdin());
    let mut stdin_buf = String::new();

    let mut i = 10;
    loop {
        tokio::select! {
            always = future::ready(i) => {
                let req = ClientRequest::Write { contents: format!("num is {}", i)};
                info!("sending request, num is {}", i);
                let res = client.send_and_await_response(req).await?;
                info!("got response {:?}", res);
                i += 1;
            }
            // line = stdin_reader.read_line(&mut stdin_buf) => {
            //     let line = line?;
            //     let req = ClientRequest::Write { contents: stdin_buf.clone()};
            //     client.send_and_await_response(req).await?;
            //     println!("finished send{}", line);
            //     stdin_buf.clear();
            // },
            res = client.reader.next() => {
                let res = res.unwrap()?;
                info!("got res {:?}", res);
                client.handle_response(res).await;
            }
        };
    }


    tokio::signal::ctrl_c()
        .await
        .expect("couldn't listen to ctrl-c");
    info!("Shutting down client");
    Ok(())
}
