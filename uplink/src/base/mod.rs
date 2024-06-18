use std::fmt::Debug;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

use bridge::Payload;
use tokio::join;

use self::bridge::{ActionsLaneCtrlTx, DataLaneCtrlTx};
use self::mqtt::CtrlTx as MqttCtrlTx;
use self::serializer::CtrlTx as SerializerCtrlTx;
use crate::collector::downloader::CtrlTx as DownloaderCtrlTx;
use crate::{Action, ActionResponse};

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
    pub downloader: DownloaderCtrlTx,
}

impl CtrlTx {
    pub async fn trigger_shutdown(&self) {
        join!(
            self.actions_lane.trigger_shutdown(),
            self.data_lane.trigger_shutdown(),
            self.mqtt.trigger_shutdown(),
            self.serializer.trigger_shutdown(),
            self.downloader.trigger_shutdown()
        );
    }
}

pub trait ServiceBusRx<T> {
    fn recv(&mut self) -> Option<T>;
    fn recv_async(&mut self) -> impl Future<Output = Option<T>>;
}

pub trait ServiceBusTx {
    type Error;

    fn publish_data(&mut self, data: Payload) -> Result<(), Self::Error>;
    fn update_action_status(&mut self, status: ActionResponse) -> Result<(), Self::Error>;
    fn subscribe_to_streams(
        &mut self,
        streams: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), Self::Error>;
    fn register_action(&mut self, name: impl Into<String>) -> Result<(), Self::Error>;
    fn deregister_action(&mut self, action: impl Into<String>) -> Result<(), Self::Error>;
    fn push_action(&mut self, action: Action) -> Result<(), Self::Error>;
}
