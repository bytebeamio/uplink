use std::time::Duration;

use log::{error, trace};
use serde::Serialize;

use crate::base::DeviceShadowConfig;
use crate::base::{bridge::BridgeTx, clock};
use crate::Payload;

pub const UPLINK_VERSION: &str = env!("VERGEN_BUILD_SEMVER");

#[derive(Debug, Serialize)]
struct State {
    uplink_version: String,
    latency: u64,
}

pub struct DeviceShadow {
    config: DeviceShadowConfig,
    bridge: BridgeTx,
    sequence: u32,
    state: State,
}

impl DeviceShadow {
    pub fn new(config: DeviceShadowConfig, bridge: BridgeTx) -> Self {
        Self {
            config,
            bridge,
            sequence: 0,
            state: State { uplink_version: UPLINK_VERSION.to_owned(), latency: 1000000 },
        }
    }

    pub fn snapshot(&mut self) -> Result<Payload, serde_json::Error> {
        self.sequence += 1;
        let payload = serde_json::to_value(&self.state)?;

        Ok(Payload {
            stream: "device_shadow".to_owned(),
            sequence: self.sequence,
            timestamp: clock() as u64,
            payload,
        })
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        let ping_addr = "8.8.8.8".parse().unwrap();
        let ping_payload = [0; 64];

        let mut device_shadow_interval =
            tokio::time::interval(Duration::from_secs(self.config.interval));

        loop {
            _ = device_shadow_interval.tick().await;
            self.state.latency = match surge_ping::ping(ping_addr, &ping_payload).await {
                Ok((_, duration)) => {
                    trace!("Ping took {:.3?}", duration);
                    duration.as_millis() as u64
                }
                Err(e) => {
                    error!("pinger: {e}");
                    // NOTE: 10s time is very large for ping time, denotes network failure,
                    // will go into persistence and be used by platform for incident reporting
                    10000
                }
            };

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
