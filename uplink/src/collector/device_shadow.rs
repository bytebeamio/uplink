use serde::Serialize;

use crate::base::{bridge::BridgeTx, clock};
use crate::uplink_config::DeviceShadowConfig;
use crate::Payload;

pub const UPLINK_VERSION: &str = env!("VERGEN_BUILD_SEMVER");

#[derive(Debug, Serialize)]
struct State {
    uplink_version: &'static str,
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
            state: State { uplink_version: UPLINK_VERSION },
        }
    }

    pub fn snapshot(&mut self) -> Payload {
        self.sequence += 1;
        let payload = serde_json::to_value(&self.state).unwrap();

        Payload {
            stream: "device_shadow".to_owned(),
            sequence: self.sequence,
            timestamp: clock() as u64,
            payload,
        }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        let mut device_shadow_interval = tokio::time::interval(self.config.interval);

        loop {
            _ = device_shadow_interval.tick().await;
            let payload = self.snapshot();
            self.bridge.send_payload(payload).await;
        }
    }
}
