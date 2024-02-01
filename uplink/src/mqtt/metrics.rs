use serde::Serialize;

use base::clock;

#[derive(Debug, Serialize, Clone)]
pub struct MqttMetrics {
    pub timestamp: u128,
    pub sequence: u32,
    pub publishes: usize,
    pub pubacks: usize,
    pub ping_requests: usize,
    pub ping_responses: usize,
    pub inflight: u16,
    pub actions_received: usize,
    pub connections: usize,
    pub connection_retries: usize,
}

impl MqttMetrics {
    pub fn new() -> Self {
        MqttMetrics {
            timestamp: clock(),
            sequence: 1,
            publishes: 0,
            pubacks: 0,
            ping_requests: 0,
            ping_responses: 0,
            inflight: 0,
            actions_received: 0,
            connections: 0,
            connection_retries: 0,
        }
    }

    pub fn add_publish(&mut self) {
        self.publishes += 1;
    }

    pub fn add_puback(&mut self) {
        self.pubacks += 1;
    }

    pub fn add_pingreq(&mut self) {
        self.ping_requests += 1;
    }

    pub fn add_pingresp(&mut self) {
        self.ping_responses += 1;
    }

    pub fn add_connection(&mut self) {
        self.connections += 1;
    }

    pub fn add_reconnection(&mut self) {
        self.connection_retries += 1;
    }

    pub fn add_action(&mut self) {
        self.actions_received += 1;
    }

    pub fn update_inflight(&mut self, inflight: u16) {
        self.inflight = inflight;
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.publishes = 0;
        self.pubacks = 0;
        self.ping_requests = 0;
        self.ping_responses = 0;
        self.connections = 0;
        self.connection_retries = 0;
        self.inflight = 0;
    }
}

impl Default for MqttMetrics {
    fn default() -> Self {
        Self::new()
    }
}
