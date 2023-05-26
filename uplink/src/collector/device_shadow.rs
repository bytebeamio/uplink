use std::time::Duration;

use log::{error, trace};
use serde::Serialize;

use crate::base::{bridge::BridgeTx, clock};
use crate::Payload;

pub const UPLINK_VERSION: &str = env!("VERGEN_BUILD_SEMVER");

#[derive(Debug, Serialize)]
pub struct DeviceShadow {
    uplink_version: String,
    latency: u64,
}

pub struct DeviceShadowHandler {
    bridge: BridgeTx,
    sequence: u32,
    state: DeviceShadow,
}

impl DeviceShadowHandler {
    pub fn new(bridge: BridgeTx) -> Self {
        Self {
            bridge,
            sequence: 0,
            state: DeviceShadow { uplink_version: UPLINK_VERSION.to_owned(), latency: 1000000 },
        }
    }

    pub fn snapshot(&mut self) -> Result<Payload, serde_json::Error> {
        self.sequence += 1;
        let payload = serde_json::to_value(&self.state)?;

        Ok(Payload {
            stream: "device_shadow".to_owned(),
            device_id: None,
            sequence: self.sequence,
            timestamp: clock() as u64,
            payload,
        })
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        let ping_addr = "8.8.8.8".parse().unwrap();
        let ping_payload = [0; 64];

        let mut device_shadow_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            _ = device_shadow_interval.tick().await;
            match surge_ping::ping(ping_addr, &ping_payload).await {
                Ok((_, duration)) => {
                    trace!("Ping took {:.3?}", duration);
                    self.state.latency = duration.as_millis() as u64;
                }
                Err(e) => {
                    error!("pinger: {e}");
                    continue;
                }
            }

            let payload = match self.snapshot() {
                Ok(v) => v,
                Err(e) => {
                    error!("snapshot: {e}");
                    continue;
                }
            };
            self.bridge.send_payload(payload).await;
        }
    }
}