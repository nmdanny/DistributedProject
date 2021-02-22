use futures::Future;

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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use std::sync::{Arc, Barrier};

    use super::*;

    #[tokio::test]
    pub async fn test_spawn_cpu_works() {
        let barrier = Arc::new(Barrier::new(2)); 
        let barrier_1 = barrier.clone();
        let barrier_2 = barrier.clone();
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
    
}