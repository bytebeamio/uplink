use std::sync::Arc;
use std::time::Duration;
use arc_swap::ArcSwap;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let shared = Arc::new(ArcSwap::from_pointee(String::from("initial value")));

    let worker_shared = Arc::clone(&shared);
    let worker = tokio::spawn(async move {
        loop {
            let value = worker_shared.load();
            println!("worker sees: {}", value);
            sleep(Duration::from_millis(700)).await;
        }
    });

    sleep(Duration::from_secs(2)).await;
    shared.store(Arc::new(String::from("first update")));
    sleep(Duration::from_secs(2)).await;
    shared.store(Arc::new(String::from("second update")));
    sleep(Duration::from_secs(2)).await;
    shared.store(Arc::new(String::from("final update")));
    sleep(Duration::from_secs(2)).await;

    worker.abort();
    let _ = worker.await;
}
