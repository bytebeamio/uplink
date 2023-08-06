use futures_util::SinkExt;
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LinesCodec};

#[derive(Debug, Serialize)]
struct ShadowPayload {
    stream: String,
    sequence: u32,
    timestamp: u64,
    a: bool,
    b: bool,
    c: bool,
    d: String,
}

#[tokio::main]
async fn main() {
    let port = std::env::args().nth(2).unwrap_or_else(|| "127.0.0.1:5050".to_string());
    let mut framed = Framed::new(TcpStream::connect(port).await.unwrap(), LinesCodec::new());
    let mut idx = 0;
    loop {
        idx += 1;
        // calculate and send consecutive squares
        let data = ShadowPayload {
            stream: "device_shadow".to_string(),
            sequence: idx,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            a: idx % 2 == 0,
            b: idx % 3 == 0,
            c: idx % 5 == 0,
            d: idx.to_string(),
        };
        let data_s = serde_json::to_string(&data).unwrap();
        println!("Sending: {}", data_s);
        framed.send(data_s).await.unwrap();
        sleep(Duration::from_secs(3)).await;
    }
}
