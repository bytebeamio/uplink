use std::env::current_dir;
use std::hash::Hash;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fmt::Debug};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use tokio::join;

#[cfg(target_os = "linux")]
use crate::collector::journalctl::JournalCtlConfig;
#[cfg(target_os = "android")]
use crate::collector::logcat::LogcatConfig;

use self::bridge::stream::MAX_BUFFER_SIZE;
use self::bridge::{ActionsLaneCtrlTx, DataLaneCtrlTx};
use self::mqtt::CtrlTx as MqttCtrlTx;
use self::serializer::CtrlTx as SerializerCtrlTx;

pub mod actions;
pub mod bridge;
pub mod monitor;
pub mod mqtt;
pub mod serializer;

pub const DEFAULT_TIMEOUT: u64 = 60;

#[inline]
fn default_timeout() -> Duration {
    Duration::from_secs(DEFAULT_TIMEOUT)
}

#[inline]
fn max_buf_size() -> usize {
    MAX_BUFFER_SIZE
}

fn default_file_size() -> usize {
    10485760 // 10MB
}

fn default_persistence_path() -> PathBuf {
    let mut path = current_dir().expect("Couldn't figure out current directory");
    path.push(".persistence");
    path
}

fn default_download_path() -> PathBuf {
    let mut path = current_dir().expect("Couldn't figure out current directory");
    path.push(".downloads");
    path
}

// Automatically assigns port 5050 for default main app, if left unconfigured
fn default_tcpapps() -> HashMap<String, AppConfig> {
    let mut apps = HashMap::new();
    apps.insert("main".to_string(), AppConfig { port: 5050, actions: vec![] });

    apps
}

pub fn clock() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, Hash, PartialEq, Eq)]
pub enum Compression {
    #[default]
    Disabled,
    Lz4,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Eq)]
pub struct StreamConfig {
    pub topic: String,
    #[serde(default = "max_buf_size")]
    pub buf_size: usize,
    #[serde(default = "default_timeout")]
    #[serde_as(as = "DurationSeconds<u64>")]
    /// Duration(in seconds) that bridge collector waits from
    /// receiving first element, before the stream gets flushed.
    pub flush_period: Duration,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default)]
    pub persistence: Persistence,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            topic: "".to_string(),
            buf_size: MAX_BUFFER_SIZE,
            flush_period: default_timeout(),
            compression: Compression::Disabled,
            persistence: Persistence::default(),
        }
    }
}

impl Hash for StreamConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic.hash(state)
    }
}

impl PartialEq for StreamConfig {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
    }
}

#[derive(Debug, Clone, Deserialize, Hash, PartialEq, Eq)]
pub struct Persistence {
    #[serde(default = "default_file_size")]
    pub max_file_size: usize,
    #[serde(default)]
    pub max_file_count: usize,
}

impl Default for Persistence {
    fn default() -> Self {
        Persistence { max_file_size: default_file_size(), max_file_count: 0 }
    }
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
    /// path to directory containing files with gps paths to be used in simulation
    pub gps_paths: String,
    /// actions that are to be routed to simulator
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloaderConfig {
    #[serde(default = "default_download_path")]
    pub path: PathBuf,
    pub actions: Vec<ActionRoute>,
}

impl Default for DownloaderConfig {
    fn default() -> Self {
        Self { path: default_download_path(), actions: vec![] }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct InstallerConfig {
    pub path: String,
    pub actions: Vec<ActionRoute>,
    pub uplink_port: u16,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StreamMetricsConfig {
    pub enabled: bool,
    pub bridge_topic: String,
    pub serializer_topic: String,
    pub blacklist: Vec<String>,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub timeout: Duration,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SerializerMetricsConfig {
    pub enabled: bool,
    pub topic: String,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub timeout: Duration,
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
pub struct ConsoleConfig {
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

#[serde_as]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ActionRoute {
    pub name: String,
    #[serde(default = "default_timeout")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub timeout: Duration,
}

impl From<&ActionRoute> for ActionRoute {
    fn from(value: &ActionRoute) -> Self {
        value.clone()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeviceShadowConfig {
    pub interval: u64,
}

impl Default for DeviceShadowConfig {
    fn default() -> Self {
        Self { interval: DEFAULT_TIMEOUT }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    #[serde(default)]
    pub console: ConsoleConfig,
    pub authentication: Option<Authentication>,
    #[serde(default = "default_tcpapps")]
    pub tcpapps: HashMap<String, AppConfig>,
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub processes: Vec<ActionRoute>,
    #[serde(default)]
    pub script_runner: Vec<ActionRoute>,
    #[serde(skip)]
    pub actions_subscription: String,
    pub streams: HashMap<String, StreamConfig>,
    #[serde(default = "default_persistence_path")]
    pub persistence_path: PathBuf,
    pub action_status: StreamConfig,
    pub stream_metrics: StreamMetricsConfig,
    pub serializer_metrics: SerializerMetricsConfig,
    pub mqtt_metrics: MqttMetricsConfig,
    #[serde(default)]
    pub downloader: DownloaderConfig,
    pub system_stats: Stats,
    pub simulator: Option<SimulatorConfig>,
    #[serde(default)]
    pub ota_installer: InstallerConfig,
    #[serde(default)]
    pub device_shadow: DeviceShadowConfig,
    #[serde(default)]
    pub action_redirections: HashMap<String, String>,
    #[serde(default)]
    pub ignore_actions_if_no_clients: bool,
    #[cfg(target_os = "linux")]
    pub logging: Option<JournalCtlConfig>,
    #[cfg(target_os = "android")]
    pub logging: Option<LogcatConfig>,
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
