use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio::select;
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

#[derive(StructOpt, Debug)]
pub struct CommandLine {
    #[structopt(short = "s", default_value = "Completed")]
    pub final_status: String,
    #[structopt(short = "w", default_value = "3")]
    pub wait_time: u64,
    #[structopt(short = "p")]
    pub port: String,
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
        id: action_id.to_string(),
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
    let args: CommandLine = StructOpt::from_args();
    let port = std::env::args().nth(2).unwrap_or_else(|| "127.0.0.1:5050".to_string());
    let mut stream = TcpStream::connect(port.as_str()).await.unwrap();
    let mut framed = Framed::new(stream, LinesCodec::new());

    let mut curr_action: Option<ActionState> = None;

    loop {
        select! {
            action_t = framed.next() => {
                let action_s = if let Some(Ok(action_s)) = action_t {
                    action_s
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if let Ok(s) = TcpStream::connect(port.as_str()).await {
                        stream = s;
                        framed = Framed::new(stream, LinesCodec::new());
                    }
                    continue;
                };
                println!("Received: {action_s}");
                let action = match serde_json::from_str::<Action>(action_s.as_str()) {
                    Err(e) => {
                        println!("invalid payload: {e}");
                        continue;
                    }
                    Ok(s) => s,
                };
                if curr_action.is_some() {
                    let curr_action_ref = curr_action.as_mut().unwrap();
                    if action.name == "cancel_action" {
                        respond(&mut framed, &mut curr_action_ref.response_counter, action.action_id.as_str(), "Completed", 100).await;
                        respond(&mut framed, &mut curr_action_ref.response_counter, curr_action_ref.id.as_str(), "Failed", 100).await;
                        curr_action = None;
                    } else {
                        respond(&mut framed, &mut curr_action_ref.response_counter, action.action_id.as_str(), "Failed", 100).await;
                    }
                } else {
                    curr_action = Some(ActionState {
                        id: action.action_id.clone(),
                        response_counter: 0,
                    });
                    let curr_action = curr_action.as_mut().unwrap();
                    respond(&mut framed, &mut curr_action.response_counter, action.action_id.as_str(), "ReceivedByClient", 0).await;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(args.wait_time)), if curr_action.is_some() => {
                let curr_action_ref = curr_action.as_mut().unwrap();
                if curr_action_ref.response_counter == 2 {
                    respond(&mut framed, &mut curr_action_ref.response_counter, curr_action_ref.id.as_str(), args.final_status.as_str(), 100).await;
                } else {
                    let progress=  (33 * curr_action_ref.response_counter) as u8;
                    respond(&mut framed, &mut curr_action_ref.response_counter, curr_action_ref.id.as_str(), "Working", progress).await;
                }
                if curr_action_ref.response_counter == 3 {
                    curr_action = None;
                }
            }
        }
    }
}
