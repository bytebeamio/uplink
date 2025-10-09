pub mod delaymap;

use std::time::{SystemTime, UNIX_EPOCH};
use flume::{Receiver, Sender};
use rumqttc::{Publish, Request};

pub fn byte_offset_to_position(
    content: &str,
    offset: usize,
) -> Result<(usize, usize), &'static str> {
    let mut line = 1;
    let mut column = 1;
    let mut current_byte = 0;

    for c in content.chars() {
        if current_byte < offset {
            if c == '\n' {
                line += 1;
                column = 1;
            } else if c != '\r' {
                column += 1;
            }
        }

        current_byte += c.len_utf8();
    }

    Ok((line, column))
}

pub fn clock() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub mod path_parser {
    use serde::Serialize;
    use serde::de::Deserialize;
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use std::path::PathBuf;

    pub fn serialize<S>(p: &PathBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        p.as_os_str().serialize(serializer)
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(PathBuf::deserialize(deserializer)?)
    }
}

// TODO: this might drop a buffer on shutdown
pub async fn chain(rx: Receiver<Publish>, tx: Sender<Request>) {
    while let Ok(item) = rx.recv_async().await {
        if tx.send_async(Request::Publish(item)).await.is_err() {
            break;
        }
    }
}

pub fn num_cores() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}
