use std::cmp::Ordering;
use std::env::current_dir;
use std::path::PathBuf;
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};
use config::{File, FileFormat};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};

pub use crate::base::bridge::stream::MAX_BATCH_SIZE;
#[cfg(target_os = "linux")]
use crate::collector::journalctl::JournalCtlConfig;
#[cfg(target_os = "android")]
use crate::collector::logcat::LogcatConfig;
use crate::DEFAULT_CONFIG;

#[inline]
fn default_timeout() -> Duration {
    Duration::from_secs(60)
}

#[inline]
fn max_batch_size() -> usize {
    MAX_BATCH_SIZE
}

pub fn default_file_size() -> usize {
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq, PartialOrd)]
pub enum Compression {
    #[default]
    Disabled,
    Lz4,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StreamConfig {
    #[serde(default)]
    #[serde(skip_deserializing)]
    pub name: String, // TODO: serializer uses this, remove it
    #[serde(default)]
    pub topic: String,
    #[serde(default = "max_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_timeout")]
    #[serde_as(as = "DurationSeconds<u64>")]
    /// Duration(in seconds) that bridge collector waits from
    /// receiving first element, before the stream gets flushed.
    pub flush_period: Duration,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default)]
    pub persistence: Persistence,
    #[serde(default)]
    pub priority: u8,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            name: "".to_owned(),
            topic: "".to_owned(),
            batch_size: MAX_BATCH_SIZE,
            flush_period: default_timeout(),
            compression: Compression::Disabled,
            persistence: Persistence::default(),
            priority: 0,
        }
    }
}

impl Ord for StreamConfig {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.priority.cmp(&other.priority), self.topic.cmp(&other.topic)) {
            (Ordering::Equal, o) => o,
            (o, _) => o.reverse(),
        }
    }
}

impl PartialOrd for StreamConfig {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq, PartialOrd)]
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

#[derive(Debug, Clone, Deserialize)]
pub struct ConsoleConfig {
    pub enabled: bool,
    pub port: u16,
    pub enable_events: bool,
}

#[derive(Debug, Clone, Deserialize)]
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
}

impl From<&ActionRoute> for ActionRoute {
    fn from(value: &ActionRoute) -> Self {
        value.clone()
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct DeviceShadowConfig {
    pub enabled: bool,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PreconditionCheckerConfig {
    pub path: PathBuf,
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeviceConfig {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<Authentication>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub console: ConsoleConfig,
    pub tcpapps: HashMap<String, AppConfig>,
    pub mqtt: MqttConfig,
    pub max_stream_count: usize,
    pub processes: Vec<ActionRoute>,
    pub script_runner: Vec<ActionRoute>,
    pub actions_subscription: String,
    pub streams: HashMap<String, StreamConfig>,
    #[serde(default = "default_persistence_path")]
    pub persistence_path: PathBuf,
    pub stream_metrics: StreamMetricsConfig,
    pub serializer_metrics: SerializerMetricsConfig,
    pub mqtt_metrics: MqttMetricsConfig,
    #[serde(default)]
    pub downloader: DownloaderConfig,
    pub system_stats: Stats,
    pub simulator: Option<SimulatorConfig>,
    #[serde(default)]
    pub ota_installer: InstallerConfig,
    pub device_shadow: DeviceShadowConfig,
    pub action_redirections: HashMap<String, String>,
    #[cfg(target_os = "linux")]
    pub logging: Option<JournalCtlConfig>,
    #[cfg(target_os = "android")]
    pub logging: Option<LogcatConfig>,
    pub precondition_checks: Option<PreconditionCheckerConfig>,
    pub prioritize_live_data: bool,
    pub enable_remote_shell: bool,
    pub wait_for_disk: bool,
    pub enable_stdin_collector: bool,
}

impl Default for Config {
    fn default() -> Self {
        config::Config::builder()
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml))
            .build().unwrap()
            .try_deserialize::<Config>().unwrap()
    }
}
