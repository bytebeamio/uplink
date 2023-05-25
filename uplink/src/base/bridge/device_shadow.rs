use serde::Serialize;

use crate::base::clock;

use super::Payload;

pub const UPLINK_VERSION: &str = env!("VERGEN_BUILD_SEMVER");

#[derive(Debug, Serialize)]
pub struct DeviceShadow {
    uplink_version: String,
}

pub struct DeviceShadowHandler {
    sequence: u32,
    state: DeviceShadow,
}

impl DeviceShadowHandler {
    pub fn new() -> Self {
        Self { sequence: 0, state: DeviceShadow { uplink_version: UPLINK_VERSION.to_owned() } }
    }

    pub fn next_payload(&mut self) -> Result<Payload, serde_json::Error> {
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
}
