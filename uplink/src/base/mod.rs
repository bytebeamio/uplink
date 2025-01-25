use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};
use flume::Sender;
use tokio::join;

use self::bridge::DataLaneCtrlTx;
use self::mqtt::CtrlTx as MqttCtrlTx;
use crate::collector::downloader::CtrlTx as DownloaderCtrlTx;

pub mod actions;
pub mod bridge;
pub mod monitor;
pub mod mqtt;
pub mod serializer;
pub mod events;

pub fn clock() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

/// Send control messages to the various components in uplink. Currently this is
/// used only to trigger uplink shutdown. Shutdown signals are sent to all
/// components simultaneously with a join.
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub data_lane: DataLaneCtrlTx,
    pub mqtt: MqttCtrlTx,
    pub serializer: Sender<()>,
    pub downloader: DownloaderCtrlTx,
}

impl CtrlTx {
    pub fn trigger_shutdown(&self) {
        let _ = join!(
            self.data_lane.trigger_shutdown(),
            self.mqtt.trigger_shutdown(),
            self.serializer.send(()),
            self.downloader.trigger_shutdown()
        );
    }
}
