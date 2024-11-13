use serde::Serialize;

use crate::base::clock;

/// `MqttMetrics` tracks metrics for MQTT operations such as publishes, acknowledgments,
/// pings, and connections. It provides methods to increment each metric and reset for the next cycle.
#[derive(Debug, Serialize, Clone)]
pub struct MqttMetrics {
    /// Timestamp of the latest metrics update
    pub timestamp: u128,
    /// Sequence number for each metrics snapshot
    pub sequence: u32,
    /// Number of publish messages sent
    pub publishes: usize,
    /// Number of publish acknowledgments received
    pub pubacks: usize,
    /// Number of ping requests sent
    pub ping_requests: usize,
    /// Number of ping responses received
    pub ping_responses: usize,
    /// Number of inflight messages (unacknowledged publishes)
    pub inflight: u16,
    /// Number of actions received
    pub actions_received: usize,
    /// Number of successful connections established
    pub connections: usize,
    /// Number of connection retries attempted
    pub connection_retries: usize,
}

impl MqttMetrics {
    /// Creates a new `MqttMetrics` instance with all metrics initialized to zero.
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

    /// Increments the count of publish messages sent.
    pub fn add_publish(&mut self) {
        self.publishes += 1;
    }

    /// Increments the count of publish acknowledgments received.
    pub fn add_puback(&mut self) {
        self.pubacks += 1;
    }

    /// Increments the count of ping requests sent.
    pub fn add_pingreq(&mut self) {
        self.ping_requests += 1;
    }

    /// Increments the count of ping responses received.
    pub fn add_pingresp(&mut self) {
        self.ping_responses += 1;
    }

    /// Increments the count of successful connections.
    pub fn add_connection(&mut self) {
        self.connections += 1;
    }

    /// Increments the count of connection retry attempts.
    pub fn add_reconnection(&mut self) {
        self.connection_retries += 1;
    }

    /// Increments the count of actions received.
    pub fn add_action(&mut self) {
        self.actions_received += 1;
    }

    /// Updates the count of inflight messages.
    pub fn update_inflight(&mut self, inflight: u16) {
        self.inflight = inflight;
    }

    /// Prepares the metrics for the next snapshot cycle by resetting counts and updating the timestamp.
    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.publishes = 0;
        self.pubacks = 0;
        self.ping_requests = 0;
        self.ping_responses = 0;
        self.actions_received = 0;
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
