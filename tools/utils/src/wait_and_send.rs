use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use uplink::Action;

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    stream: String,
    sequence: u32,
    timestamp: u64,
    id: String,
    state: String,
    progress: u8,
    errors: Vec<String>,
}

#[tokio::main]
async fn main() {
    let final_state = std::env::args().nth(1).unwrap_or_else(|| {
        println!("Using default value \"Completed\"");
        "Completed".to_string()
    });
    let port = std::env::args().nth(2).unwrap_or_else(|| "127.0.0.1:5050".to_string());
    let stream = TcpStream::connect(port).await.unwrap();
    let mut framed = Framed::new(stream, LinesCodec::new());
    async fn respond<'a>(
        framed: &'a mut Framed<TcpStream, LinesCodec>,
        idx: &mut u32,
        action_id: &str,
        state: &str,
        progress: u8,
    ) {
        *idx += 1;
        let response = Response {
            stream: "action_status".to_string(),
            sequence: *idx,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            id: action_id.to_string(),
            state: state.to_string(),
            progress,
            errors: vec![],
        };
        let resp = serde_json::to_string(&response).unwrap();
        println!("Sending: {}", resp);
        framed.send(resp).await.unwrap();
    }
    let mut idx = 0;
    loop {
        let action_s = framed.next().await.unwrap().unwrap();
        println!("Received: {}", action_s);
        let action = serde_json::from_str::<Action>(action_s.as_str()).unwrap();
        sleep(Duration::from_secs(3));
        respond(&mut framed, &mut idx, action.action_id.as_str(), "Working", 33).await;
        sleep(Duration::from_secs(3));
        respond(&mut framed, &mut idx, action.action_id.as_str(), "Working", 66).await;
        sleep(Duration::from_secs(3));
        respond(&mut framed, &mut idx, action.action_id.as_str(), final_state.as_str(), 100).await;
    }
}
