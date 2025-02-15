use std::io::{stdin, BufRead};
use log::{debug, error, info};
use crate::base::bridge::{BridgeTx, Payload};

pub fn stdin_collector(bridge: BridgeTx) {
    let mut stdin = stdin().lock();
    let mut line_buffer = String::new();
    loop {
        line_buffer.clear();
        if let Err(e) = stdin.read_line(&mut line_buffer) {
            info!("stopping stdin collector: {e:?}");
            break;
        }

        if let Err(e) = queue_payload(&bridge, line_buffer.as_str()) {
            error!("stdin: invalid payload: {line_buffer:?}, error: {e:?}");
        }
    }
}

pub fn queue_payload(bridge: &BridgeTx, line_buffer: &str) -> anyhow::Result<()> {
    debug!("stdin: received data = {line_buffer:?}");
    let data = serde_json::from_str::<Payload>(line_buffer)?;
    bridge.send_payload_sync(data);
    Ok(())
}