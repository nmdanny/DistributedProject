use std::{convert::TryInto, ops::Range};
use anyhow::Context;

use futures::Future;

pub fn parse_phase_length(src: &str) -> Result<std::time::Duration, anyhow::Error> {
    let millis = src.parse::<u64>()?;
    Ok(std::time::Duration::from_millis(millis))
}

pub fn parse_range(src: &str) -> Result<Range<u64>, anyhow::Error> {
    let mut parts = src.split("..").map(str::to_owned).collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Expected 2 parts separated by '..', got {} instead", src.len()));
    }
    parts[0].retain(|c| !c.is_whitespace());
    parts[1].retain(|c| !c.is_whitespace());
    let from = parts[0].parse::<u64>().context("from")?;
    let to = parts[1].parse::<u64>().context("to")?;
    Ok(from .. to)
}

/// Similar to `tokio::task::spawn_nonblocking`, but uses the rayon thread-pool instead,
/// useful for CPU heavy computations.
pub async fn spawn_cpu<F, T>(fun: F) -> T
    where F: FnOnce() -> T + Send + 'static, T: Send + 'static
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let res = fun();
        let _ = tx.send(res);
    });
    rx.await.expect("Panic in rayon::spawn")
}


/// Spawns a future on a new thread and a different async runtime,
/// allowing the future to block that runtime, without blocking the original runtime
pub fn spawn_on_new_runtime<F>(f: F) -> impl Future<Output = F::Output>
    where F: Future + Send + 'static, F::Output: Send + 'static
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new().name("new_runtime-scheduler".to_owned()).spawn(move || {
        let res = tokio::runtime::Builder::new_current_thread()
            .thread_name("new_runtime-tokio-runtime-worker")
            .enable_all()
            .build()
            .unwrap()
            .block_on(f);
        let _ = tx.send(res);
    }).expect("Couldn't spawn new thread");
    async move {
        rx.await.expect("tokio runtime panicked")
    }

}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use tracing_futures::Instrument;
    use std::sync::{Arc, Barrier};

    use super::*;

    #[test]
    pub fn can_parse_range() {
        assert_eq!(parse_range("13 ..   15").unwrap(), 13 .. 15);
        assert_eq!(parse_range("13..15").unwrap(), 13 .. 15);
        assert_eq!(parse_range("100 .. 200").unwrap(), 100 .. 200);
    }

    #[tokio::test]
    pub async fn test_spawn_cpu_works() {
        let barrier_1 = Arc::new(Barrier::new(2)); 
        let barrier_2 = barrier_1.clone();

        let a = spawn_cpu(move || {
            barrier_1.wait();
            1
        });
        let b = spawn_cpu(move || {
            barrier_2.wait();
            2
        });

        let apb = futures::future::join(a, b).map(|(a,b)| a + b).await;

        assert_eq!(apb, 3);
    }   


    #[tokio::test]
    pub async fn test_spawn_cpu_async_works() {

        let _guard = crate::logging::setup_logging().unwrap();

        let sync_barrier_1 = Arc::new(Barrier::new(2)); 
        let sync_barrier_2 = sync_barrier_1.clone();

        let async_barrier_1 = Arc::new(tokio::sync::Barrier::new(2));
        let async_barrier_2 = async_barrier_1.clone();

        let a = spawn_on_new_runtime(async move {
            info!("a waiting for async barrier");
            async_barrier_1.wait().await;

            info!("a waiting for sync barrier");
            sync_barrier_1.wait();

            info!("a done");
            1
        }.instrument(info_span!("spawn_1")));
        let b = spawn_on_new_runtime(async move {
            println!("b waiting for async barrier");
            async_barrier_2.wait().await;

            info!("b done");
            2
        }.instrument(info_span!("spawn_2")));

        info!("main sleeping");
        tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;


        info!("main waiting sync barrier");
        sync_barrier_2.wait();

        info!("main waiting for a,b to be done");
        let apb = futures::future::join(a, b).map(|(a,b)| a + b).await;

        assert_eq!(apb, 3);
    }   
    
}