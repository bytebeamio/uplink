use serde::Serialize;

use crate::collector::utils::clock;

#[derive(Debug, Serialize, Clone)]
pub struct MqttMetrics {
    timestamp: u128,
    sequence: u32,
    inflight_len: usize,
    publish_count: usize,
    puback_count: usize,
    ping_request_count: usize,
    ping_response_count: usize,
    inflight_count: u16,
    action_count: usize,
    connection_count: usize,
    disconnection_count: usize,
}

impl MqttMetrics {
    pub fn new() -> Self {
        MqttMetrics {
            timestamp: clock(),
            sequence: 1,
            inflight_len: 0,
            publish_count: 0,
            puback_count: 0,
            ping_request_count: 0,
            ping_response_count: 0,
            inflight_count: 0,
            action_count: 0,
            connection_count: 0,
            disconnection_count: 0,
        }
    }

    pub fn add_publish(&mut self) {
        self.publish_count += 1;
    }

    pub fn add_puback(&mut self) {
        self.puback_count += 1;
    }

    pub fn add_pingreq(&mut self) {
        self.ping_request_count += 1;
    }

    pub fn add_pingresp(&mut self) {
        self.ping_response_count += 1;
    }

    pub fn add_connection(&mut self) {
        self.connection_count += 1;
    }

    pub fn add_disconnection(&mut self) {
        self.disconnection_count += 1;
    }

    pub fn add_action(&mut self) {
        self.action_count += 1;
    }

    pub fn update_inflight(&mut self, inflight: u16) {
        self.inflight_count = inflight;
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.inflight_len = 0;
        self.publish_count = 0;
        self.puback_count = 0;
        self.ping_request_count = 0;
        self.connection_count = 0;
    }
}
