//! Various configuration details associated with the uplink instance.

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub topic: String,
    pub buf_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Persistence {
    pub path: String,
    pub max_file_size: usize,
    pub max_file_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Authentication {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ota {
    pub enabled: bool,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<Authentication>,
    pub bridge_port: u16,
    pub max_packet_size: usize,
    pub max_inflight: u16,
    pub actions: Vec<String>,
    pub persistence: Persistence,
    pub streams: HashMap<String, StreamConfig>,
    pub ota: Ota,
}