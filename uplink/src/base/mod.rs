use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fmt::Debug};

use serde::{Deserialize, Serialize};

#[cfg(any(target_os = "linux"))]
use crate::collector::journalctl::JournalCtlConfig;
#[cfg(any(target_os = "android"))]
use crate::collector::logcat::LogcatConfig;

pub mod actions;
pub mod bridge;
pub mod monitor;
pub mod mqtt;
pub mod serializer;

pub const DEFAULT_TIMEOUT: u64 = 60;

#[inline]
fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT
}

fn default_file_size() -> usize {
    104857600 // 100MB
}

fn default_file_count() -> usize {
    3
}

fn default_as_true() -> bool {
    true
}

pub fn clock() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StreamConfig {
    pub topic: String,
    pub buf_size: usize,
    #[serde(default = "default_timeout")]
    /// Duration(in seconds) that bridge collector waits from
    /// receiving first element, before the stream gets flushed.
    pub flush_period: u64,
    #[serde(default = "default_as_true")]
    pub persistance: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Persistence {
    pub path: String,
    #[serde(default = "default_file_size")]
    pub max_file_size: usize,
    #[serde(default = "default_file_count")]
    pub max_file_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Authentication {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Stats {
    pub enabled: bool,
    pub process_names: Vec<String>,
    pub update_period: u64,
    pub stream_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SimulatorConfig {
    /// number of devices to be simulated
    pub num_devices: u32,
    /// path to directory containing files with gps paths to be used in simulation
    pub gps_paths: String,
    /// actions that are to be routed to simulator
    pub actions: Vec<ActionRoute>,
    #[serde(skip)]
    pub actions_subscriptions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DownloaderConfig {
    pub path: String,
    #[serde(default)]
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct InstallerConfig {
    pub path: String,
    #[serde(default)]
    pub actions: Vec<ActionRoute>,
    pub uplink_port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StreamMetricsConfig {
    pub enabled: bool,
    pub topic: String,
    pub blacklist: Vec<String>,
    pub timeout: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SerializerMetricsConfig {
    pub enabled: bool,
    pub topic: String,
    pub timeout: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MqttMetricsConfig {
    pub enabled: bool,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    pub port: u16,
    #[serde(default)]
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TracingConfig {
    pub enabled: bool,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MqttConfig {
    pub max_packet_size: usize,
    pub max_inflight: u16,
    pub keep_alive: u64,
    pub network_timeout: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ActionRoute {
    pub name: String,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

impl From<&ActionRoute> for ActionRoute {
    fn from(value: &ActionRoute) -> Self {
        value.clone()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    #[serde(default)]
    pub apis: TracingConfig,
    pub authentication: Option<Authentication>,
    pub tcpapps: HashMap<String, AppConfig>,
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub processes: Vec<ActionRoute>,
    #[serde(skip)]
    pub actions_subscription: String,
    pub persistence: Option<Persistence>,
    pub streams: HashMap<String, StreamConfig>,
    pub action_status: StreamConfig,
    pub stream_metrics: StreamMetricsConfig,
    pub serializer_metrics: SerializerMetricsConfig,
    pub mqtt_metrics: MqttMetricsConfig,
    pub downloader: DownloaderConfig,
    pub system_stats: Stats,
    pub simulator: Option<SimulatorConfig>,
    pub ota_installer: Option<InstallerConfig>,
    #[serde(default)]
    pub action_redirections: HashMap<String, String>,
    #[serde(default)]
    pub ignore_actions_if_no_clients: bool,
    #[cfg(any(target_os = "linux"))]
    pub logging: Option<JournalCtlConfig>,
    #[cfg(any(target_os = "android"))]
    pub logging: Option<LogcatConfig>,
}
