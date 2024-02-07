use futures_util::SinkExt;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() {
    let argv = std::env::args().collect::<Vec<_>>();
    let addr = argv.get(1).unwrap();
    let payload = argv_to_payload(&argv[2..]);

    let mut framed = Framed::new(TcpStream::connect(addr).await.unwrap(), LinesCodec::new());
    framed.send(payload.to_string()).await.unwrap();
    dbg!(payload);
}

fn argv_to_payload(pairs: &[String]) -> Value {
    // nlici
    let stream = pairs.first().unwrap();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let kv_count = pairs.len() - 1;
    assert_eq!(kv_count % 2, 0);
    let k_count = kv_count / 2;
    let mut payload = json!({
        "stream": stream,
        "sequence": 1,
        "timestamp": timestamp,
    });
    for idx in 0..k_count {
        let k = pairs.get(1 + idx * 2).unwrap();
        let v = pairs.get(1 + idx * 2 + 1).unwrap();
        payload.as_object_mut().unwrap().insert(k.clone(), Value::String(v.clone()));
    }
    payload
}
