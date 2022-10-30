use std::time::{SystemTime, UNIX_EPOCH};
use futures_util::SinkExt;
use tokio::net::{TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use uplink::{Action};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    stream: String,
    sequence: u32,
    timestamp: u64,
    action_id: String,
    state: String,
    progress: u8,
    errors: Vec<String>,
}

#[tokio::main]
async fn main() {
    let port = std::env::args().nth(1).unwrap_or("127.0.0.1:5555".to_string());
    let stream = TcpStream::connect(port).await.unwrap();
    let mut framed = Framed::new(stream, LinesCodec::new());
    let mut idx = 1;
    loop {
        let action_s = framed.next().await.unwrap().unwrap();
        println!("Received: {}", action_s);
        let action = serde_json::from_str::<Action>(action_s.as_str()).unwrap();
        idx += 1;
        let response = Response {
            stream: "action_status".to_string(),
            sequence: idx,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            action_id: action.action_id,
            state: "Completed".to_string(),
            progress: 100,
            errors: vec![],
        };
        let resp = serde_json::to_string(&response).unwrap();
        println!("Sending: {}", resp);
        framed.send(resp).await.unwrap();
    }
}