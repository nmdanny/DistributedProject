use crate::{Settings, ChatClient, WriteRequest, ChatUpdated};
use futures::Future;
use tonic::{Status, Request, Response};
use futures_retry::{RetryPolicy, FutureFactory};
use std::time::Duration;
use tokio::time::Elapsed;
use std::marker::PhantomData;
use std::rc::Rc;

pub async fn client_talks_with_server(settings: impl AsRef<Settings>) -> anyhow::Result<ChatClient> {
    let settings = settings.as_ref();
    Ok(ChatClient::connect(format!("http://{}", settings.server_addr)).await?)
}

pub async fn client_talks_with_adversary(settings: impl AsRef<Settings>) -> anyhow::Result<ChatClient> {
    let settings = settings.as_ref();
    Ok(ChatClient::connect(format!("http://{}", settings.adversary_addr)).await?)
}

const MAX_RETRIES: usize = 5;

pub async fn write_with_retransmit(client: &mut ChatClient, msg: WriteRequest, time: Duration) -> Result<Response<ChatUpdated>, Status> {
    let mut retries = 0;
    loop {
        let mut client = client.clone();
        if let Ok(res) = tokio::time::timeout(time, client.write(Request::new(msg.clone()))).await {
            return res;
        }
        retries += 1;
        if retries == MAX_RETRIES {
            // return Err(Status::deadline_exceeded("repeat attempts failed"))
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Clap;
    use std::net::SocketAddr;
    use crate::{ChatClient, WriteRequest, PacketMetadata, Settings, start_server, start_adversary, ConnectRequest};
    use tonic::Request;
    use futures::task::SpawnExt;
    use std::sync::Arc;
    use futures::{StreamExt, Stream};
    use futures::stream::FuturesUnordered;
    use crate::client::write_with_retransmit;
    use std::time::Duration;
    use log::LevelFilter;

    const NUM_REQUESTS: usize = 10;

    async fn test_base(address: SocketAddr, timeout_ms: u64) {
        let client = ChatClient::connect(format!("http://{}", address)).await.unwrap();

        let mut broadcasts = client.clone().subscribe(Request::new(ConnectRequest { client_id: 1337})).await
            .unwrap()
            .into_inner();


        let write_requests = (1..NUM_REQUESTS + 1).map(|i| {
            let mut client = client.clone();
            let req = WriteRequest {
                meta: Some(PacketMetadata {
                    client_id: 1337,
                    sequence_num: i as u64
                }),
                contents: i.to_string()
            };
            async move {
                write_with_retransmit(&mut client, req, Duration::from_millis(timeout_ms)).await
            }
        }).collect::<FuturesUnordered<_>>();

        let responses = write_requests.collect::<Vec<_>>().await;


        let mut last_chat_response = 0;
        while let Some(res) = broadcasts.message().await.unwrap() {
            let response = res.contents.parse().unwrap();
            assert!(response > last_chat_response);
            println!("Handled request {}", response);
            if response == NUM_REQUESTS {
                break;
            }
            last_chat_response = response;
        }

    }

    #[tokio::test()]
    async fn test_with_server() {
        let settings = Arc::new(Settings::default());
        let settings2 = settings.clone();
        let server = tokio::spawn(async move {
            start_server(settings2).await
        });
        test_base(settings.server_addr, 10).await;
    }

    #[tokio::test()]
    async fn test_with_adversary() {
        let _ = pretty_env_logger::formatted_builder()
            .filter_level(LevelFilter::Info)
            .is_test(false).init();

        info!("wot");
        let mut options = Settings::default();
        options.server_addr = "[::1]:8330".parse().unwrap();
        options.adversary_addr = "[::1]:8331".parse().unwrap();
        options.min_delay_ms = 10;
        options.max_delay_ms = 200;
        options.duplicate_prob = 0.2;
        options.reorder_prob = 0.5;
        options.drop_prob = 0.5;
        let timeout_ms = options.min_delay_ms.max(options.max_delay_ms);
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