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
}

#[tokio::main]
async fn main() {
    let port = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5555".to_string());
    let mut framed = Framed::new(TcpStream::connect(port).await.unwrap(), LinesCodec::new());
    let mut idx = 0;
    let mut start = SystemTime::now();
    loop {
        idx += 1;
        // calculate and send consecutive squares
        let data = ShadowPayload {
            stream: "test_stream_12".to_string(),
            sequence: idx,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            a: idx % 2 == 0,
            b: idx % 3 == 0,
            c: idx % 5 == 0,
        };
        let data_s = serde_json::to_string(&data).unwrap();
        framed.send(data_s).await.unwrap();
        if SystemTime::now().duration_since(start).unwrap() > Duration::from_secs(1) {
            println!("{idx} msg/s");
            idx = 0;
            start = SystemTime::now();
        }
    }
}
