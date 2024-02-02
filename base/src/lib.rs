use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

mod actions;
mod data;

pub use actions::{Action, ActionResponse};
pub use data::Payload;
use serde::Serialize;

pub fn clock() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

pub trait Point: Send + Debug + Serialize + 'static {
    fn stream_name(&self) -> &str;
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

#[async_trait::async_trait]
pub trait CollectorTx: Send + 'static {
    async fn send_action_response(&mut self, status: ActionResponse);
    async fn send_payload(&mut self, payload: Payload);
    fn send_payload_sync(&mut self, payload: Payload);
}


#[async_trait::async_trait]
pub trait CollectorRx: Send + 'static {
    async fn recv_action(&mut self) -> Option<Action>;
}