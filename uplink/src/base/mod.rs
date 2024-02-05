use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::join;

use self::bridge::{ActionsLaneCtrlTx, DataLaneCtrlTx};
use self::mqtt::CtrlTx as MqttCtrlTx;
use self::serializer::CtrlTx as SerializerCtrlTx;

pub mod actions;
pub mod bridge;
pub mod monitor;
pub mod mqtt;
pub mod serializer;

pub fn clock() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

/// Send control messages to the various components in uplink. Currently this is
/// used only to trigger uplink shutdown. Shutdown signals are sent to all
/// components simultaneously with a join.
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub actions_lane: ActionsLaneCtrlTx,
    pub data_lane: DataLaneCtrlTx,
    pub mqtt: MqttCtrlTx,
    pub serializer: SerializerCtrlTx,
}

impl CtrlTx {
    pub async fn trigger_shutdown(&self) {
        join!(
            self.actions_lane.trigger_shutdown(),
            self.data_lane.trigger_shutdown(),
            self.mqtt.trigger_shutdown(),
            self.serializer.trigger_shutdown()
        );
    }
}
