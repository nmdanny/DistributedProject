use crate::{ChatClient, ChatUpdated, WriteRequest, ConnectRequest, PacketMetadata};

use std::time::Duration;
use futures::channel::mpsc;

use tonic::{Request, Response, Status};
use std::net::SocketAddr;
use std::sync::Arc;
use futures_retry::{FutureRetry, RetryPolicy};
use tokio::time::{Timeout, Elapsed};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ClientState {
    client: ChatClient,
    sequence_number: u64,
    client_id: u64,
    timeout_ms: u64,
}

impl ClientState {
    pub async fn new(addr: SocketAddr, timeout_ms: u64) -> anyhow::Result<(Self, tonic::Streaming<ChatUpdated>)> {
        let mut client = ChatClient::connect(format!("http://{}", addr))
            .await?;
        let client_id = rand::random();
        let message_stream = client.subscribe(Request::new(ConnectRequest { client_id }))
            .await?
            .into_inner();
        Ok((ClientState {
            client,
            sequence_number: 1,
            client_id,
            timeout_ms
        }, message_stream))
    }

    pub async fn send_write_request(&self, msg: String, sequence_number: u64) -> Result<Response<ChatUpdated>, Status> {
        // let retry = FutureRetry::new(|| async {
        //     let mut client = self.client.clone();
        //     let write_fut = client.write(WriteRequest {
        //         contents: msg.clone(),
        //         meta: Some(PacketMetadata {
        //             client_id: self.client_id,
        //             sequence_num: self.sequence_number.load(Ordering::SeqCst)
        //         })
        //     });
        //     tokio::time::timeout(Duration::from_millis(self.timeout_ms) , write_fut).await
        // }, |_: Elapsed| RetryPolicy::<Status>::Repeat);
        // let res = retry.await.map_err(|(status, size)| status)?;
        let mut client = self.client.clone();
        loop {
            let write_fut = client.write(WriteRequest {
                contents: msg.clone(),
                meta: Some(PacketMetadata {
                    client_id: self.client_id,
                    sequence_num: sequence_number
                })
            });
            let timeout = tokio::time::timeout(Duration::from_millis(self.timeout_ms), write_fut);
            let res = timeout.await;
            if let Ok(res) = res {
                return Ok(res?);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::ClientState;
    use crate::{
        start_adversary, start_server, ChatClient, ConnectRequest, PacketMetadata, Settings,
        WriteRequest,
    };
    use futures::stream::{FuturesUnordered, FuturesOrdered};

    use futures::{StreamExt, TryFutureExt, FutureExt};
    use log::LevelFilter;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tonic::{IntoRequest, Request};

    const NUM_REQUESTS: usize = 10000;

    // When this is set to 1, requests are made serially (await a response before proceeding with the next request)
    // This can also be set higher, at the cost of filling the server with requests from the future,
    // which fills up its queue, or worse, when adversary is active - the client might make
    const MAX_REQS_PER_TIME: usize = 16;



    async fn test_base(address: SocketAddr, timeout_ms: u64) {
        let (mut client, mut broadcasts) = ClientState::new(address, timeout_ms).await.unwrap();

        let check_broadcasts_proper_order = tokio::spawn(async move {
            let mut last_chat_response = 0;
            while let Some(res) = broadcasts.message().await.unwrap() {
                let response = res.contents.parse().unwrap();
                assert!(response == last_chat_response + 1);
                println!("> {}", response);
                if response != last_chat_response + 1 {
                    eprintln!("\t^mismatch");
                }
                if response % (NUM_REQUESTS/100 + 1) == 0 {
                    println!("Handled {} responses", response);
                }
                last_chat_response = response;
                if response == NUM_REQUESTS {
                    break;
                }
            }
            assert_eq!(last_chat_response, NUM_REQUESTS);
        });

        let _futures = futures::stream::iter((1 .. NUM_REQUESTS + 1)
            .map(|i| client.send_write_request(i.to_string(), i as u64)))
            .buffered(MAX_REQS_PER_TIME)
            .enumerate()
            .for_each(|(i,c)| async move {
                if c.is_err() {
                    panic!("Request {} failed: {}", i, c.err().unwrap());
                }
            })
            .await;

    check_broadcasts_proper_order.await.unwrap();


    }

    #[tokio::test(threaded_scheduler)]
    async fn test_with_server() {
        let _ = pretty_env_logger::formatted_builder()
            .filter_level(LevelFilter::Error)
            .is_test(false)
            .init();
        let settings = Arc::new(Settings::default());
        let settings2 = settings.clone();
        let _server = tokio::spawn(async move { start_server(settings2).await });
        tokio::time::delay_for(Duration::from_secs(2)).await;

        test_base(settings.server_addr,  1000 * 3600).await;
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_with_adversary() {
        let _ = pretty_env_logger::formatted_builder()
            .filter_module("dist_lib::server", LevelFilter::Info)
            .filter_module("dist_lib::adversary", LevelFilter::Info)
            .is_test(true)
            .init();

        info!("wot");
        let mut options = Settings::default();
        options.server_addr = "[::1]:8330".parse().unwrap();
        options.adversary_addr = "[::1]:8331".parse().unwrap();
        options.min_delay_ms = 10;
        options.max_delay_ms = 200;
        options.duplicate_prob = 0.2;
        options.reorder_prob = 0.3;
        options.drop_prob = 0.2;
        let timeout_ms = options.min_delay_ms.max(options.max_delay_ms) * 4;
        let options = Arc::new(options);
        let options2 = options.clone();
        let options3 = options.clone();
        loop {
            tokio::select! {
                _ = tokio::spawn(async move { start_server(options).await }) => panic!("server ended too soon"),
                _ = tokio::spawn(async move { start_adversary(options2).await }) => panic!("adversary ended too soon"),
                _ = tokio::spawn(async move { test_base(options3.adversary_addr, timeout_ms).await }) => return,
            };
        }
    }
}
