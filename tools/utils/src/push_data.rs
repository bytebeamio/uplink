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
    can_id: u32,
    byte1: u8,
    byte2: u8,
    byte3: u8,
    byte4: u8,
    byte5: u8,
    byte6: u8,
    byte7: u8,
    byte8: u8,
}

#[tokio::main]
async fn main() {
    let stream = std::env::args().nth(1).unwrap_or_else(|| "c2c_can".to_string());
    let port = std::env::args().nth(2).unwrap_or_else(|| "127.0.0.1:5050".to_string());
    let rate = std::env::args().nth(3)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1000);
    let interval = Duration::from_micros(1_000_000 / rate);
    let mut framed = Framed::new(TcpStream::connect(port).await.unwrap(), LinesCodec::new());
    let mut idx = 0;
    loop {
        idx += 1;
        // calculate and send consecutive squares
        let data = ShadowPayload {
            stream: stream.clone(),
            sequence: idx,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            can_id: idx % 1024,
            byte1: ((idx + 0) % 256) as u8,
            byte2: ((idx + 1) % 256) as u8,
            byte3: ((idx + 2) % 256) as u8,
            byte4: ((idx + 3) % 256) as u8,
            byte5: ((idx + 4) % 256) as u8,
            byte6: ((idx + 5) % 256) as u8,
            byte7: ((idx + 6) % 256) as u8,
            byte8: ((idx + 7) % 256) as u8,
        };
        let data_s = serde_json::to_string(&data).unwrap();
        framed.send(data_s).await.unwrap();
        sleep(interval).await;
    }
}
