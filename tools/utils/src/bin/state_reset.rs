use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use futures_util::{SinkExt, TryFutureExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

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
        action_id: action_id.to_string(),
        state: state.to_string(),
        progress,
        errors: vec![],
    };
    let resp = serde_json::to_string(&response).unwrap();
    println!("Sending: {resp}");
    framed.send(resp).await.unwrap();
}

struct ActionState {
    id: String,
    response_counter: u32,
}

#[tokio::main]
async fn main() {
    let port = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5050".to_string());
    let stream = TcpStream::connect(port.as_str()).await.unwrap();
    let mut framed = Framed::new(stream, LinesCodec::new());

    let mut idx = 0;
    respond(&mut framed, &mut idx, "device-state-reset", "", 0).await;
}
