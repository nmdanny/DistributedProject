use crate::{ChatClient, ChatUpdated, WriteRequest};

use std::time::Duration;

use tonic::{Request, Response, Status};

pub async fn write_with_retransmit(
    client: &mut ChatClient,
    msg: WriteRequest,
    time: Duration,
) -> Result<Response<ChatUpdated>, Status> {
    loop {
        let mut client = client.clone();
        if let Ok(res) = tokio::time::timeout(time, client.write(Request::new(msg.clone()))).await {
            return res;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::write_with_retransmit;
    use crate::{
        start_adversary, start_server, ChatClient, ConnectRequest, PacketMetadata, Settings,
        WriteRequest,
    };
    use futures::stream::FuturesUnordered;

    use futures::StreamExt;
    use log::LevelFilter;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tonic::Request;

    const NUM_REQUESTS: usize = 100;

    async fn test_base(address: SocketAddr, timeout_ms: u64) {
        let client = ChatClient::connect(format!("http://{}", address))
            .await
            .unwrap();

        let mut broadcasts = client
            .clone()
            .subscribe(Request::new(ConnectRequest { client_id: 1337 }))
            .await
            .unwrap()
            .into_inner();

        let write_requests = (1..NUM_REQUESTS + 1)
            .map(|i| {
                let mut client = client.clone();
                let req = WriteRequest {
                    meta: Some(PacketMetadata {
                        client_id: 1337,
                        sequence_num: i as u64,
                    }),
                    contents: i.to_string(),
                };
                async move {
                    write_with_retransmit(&mut client, req, Duration::from_millis(timeout_ms)).await
                }
            })
            .collect::<FuturesUnordered<_>>();

        let _responses = write_requests.collect::<Vec<_>>().await;

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
        let _server = tokio::spawn(async move { start_server(settings2).await });
        tokio::time::delay_for(Duration::from_secs(5)).await;
        test_base(settings.server_addr, 10).await;
    }

    #[tokio::test()]
    async fn test_with_adversary() {
        let _ = pretty_env_logger::formatted_builder()
            .filter_level(LevelFilter::Info)
            .is_test(false)
            .init();

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
