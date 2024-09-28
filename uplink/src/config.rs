use std::cmp::Ordering;
use std::env::current_dir;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use std::{collections::HashMap, fmt};

use serde::de::{self, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DurationSeconds};

pub use crate::base::bridge::stream::MAX_BATCH_SIZE;
#[cfg(target_os = "linux")]
use crate::collector::journalctl::JournalCtlConfig;
#[cfg(target_os = "android")]
use crate::collector::logcat::LogcatConfig;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
pub const MAX_STREAM_COUNT: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Serde Error")]
    Serde(#[from] serde_json::Error),
    #[error("Io Error")]
    Io(#[from] std::io::Error),
    #[error("Toml Error")]
    Toml(#[from] toml::ser::Error),
}

#[inline]
fn default_timeout() -> Duration {
    DEFAULT_TIMEOUT
}

#[inline]
fn default_stream_count() -> usize {
    MAX_STREAM_COUNT
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

// Automatically assigns port 5050 for default main app, if left unconfigured
fn default_tcpapps() -> HashMap<String, AppConfig> {
    let mut apps = HashMap::new();
    apps.insert("main".to_string(), AppConfig { port: 5050, actions: vec![] });

    apps
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq, PartialOrd)]
pub enum Compression {
    #[default]
    Disabled,
    Lz4,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct StreamConfig {
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
            topic: "".to_string(),
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

#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Authentication {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Stats {
    pub enabled: bool,
    pub process_names: Vec<String>,
    pub update_period: u64,
    pub stream_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SimulatorConfig {
    /// path to directory containing files with gps paths to be used in simulation
    pub gps_paths: String,
    /// actions that are to be routed to simulator
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AppConfig {
    pub port: u16,
    #[serde(default)]
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ConsoleConfig {
    pub enabled: bool,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MqttConfig {
    pub max_packet_size: usize,
    pub max_inflight: u16,
    pub keep_alive: u64,
    pub network_timeout: u64,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ActionRoute {
    pub name: String,
    #[serde(default = "default_timeout")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub timeout: Duration,
    // Can the action handler cancel actions mid execution?
    #[serde(default)]
    pub cancellable: bool,
}

impl From<&ActionRoute> for ActionRoute {
    fn from(value: &ActionRoute) -> Self {
        value.clone()
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeviceShadowConfig {
    #[serde_as(as = "DurationSeconds<u64>")]
    pub interval: Duration,
}

impl Default for DeviceShadowConfig {
    fn default() -> Self {
        Self { interval: DEFAULT_TIMEOUT }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PreconditionCheckerConfig {
    pub path: PathBuf,
    pub actions: Vec<ActionRoute>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub original: String,
    pub renamed: Option<String>,
}

struct FieldVisitor;

impl<'de> Visitor<'de> for FieldVisitor {
    type Value = Field;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(r#"a string or a map with a single key-value pair"#)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Field { original: value.to_string(), renamed: None })
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let entry = map.next_entry::<String, String>()?;
        if let Some((renamed, original)) = entry {
            Ok(Field { original, renamed: Some(renamed) })
        } else {
            Err(de::Error::custom("Expected a single key-value pair in the map"))
        }
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(FieldVisitor)
    }
}

impl Serialize for Field {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.renamed {
            Some(renamed) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(renamed, &self.original)?;
                map.end()
            }
            None => serializer.serialize_str(&self.original),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SelectConfig {
    All,
    Fields(Vec<Field>),
}

struct SelectVisitor;

impl<'de> Visitor<'de> for SelectVisitor {
    type Value = SelectConfig;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(r#"the string "all" or a list of `Field`s"#)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "all" => Ok(SelectConfig::All),
            _ => Err(de::Error::custom(r#"Expected the string "all""#)),
        }
    }

    fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
    where
        S: serde::de::SeqAccess<'de>,
    {
        let mut fields = vec![];
        while let Some(field) = seq.next_element()? {
            fields.push(field);
        }

        Ok(SelectConfig::Fields(fields))
    }
}

impl<'de> Deserialize<'de> for SelectConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(SelectVisitor)
    }
}

impl Serialize for SelectConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            SelectConfig::All => serializer.serialize_str("all"),
            SelectConfig::Fields(fields) => {
                let mut seq = serializer.serialize_seq(Some(fields.len()))?;
                for field in fields {
                    seq.serialize_element(field)?;
                }
                seq.end()
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputConfig {
    pub input_stream: String,
    pub select_fields: SelectConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NoDataAction {
    #[default]
    Null,
    PreviousValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PushInterval {
    OnNewData,
    OnTimeout(Duration),
}

struct PushVisitor;

impl<'de> Visitor<'de> for PushVisitor {
    type Value = PushInterval;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(r#"the string "on_new_data" or a unsigned integer"#)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "on_new_data" => Ok(PushInterval::OnNewData),
            _ => Err(de::Error::custom(r#"Expected the string "on_new_data""#)),
        }
    }

    fn visit_u64<E>(self, secs: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(PushInterval::OnTimeout(Duration::from_secs(secs)))
    }

    fn visit_i64<E>(self, secs: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_u64(secs as u64)
    }
}

impl<'de> Deserialize<'de> for PushInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(PushVisitor)
    }
}

impl Serialize for PushInterval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PushInterval::OnNewData => serializer.serialize_str("on_new_data"),
            PushInterval::OnTimeout(duration) => serializer.serialize_u64(duration.as_secs()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinConfig {
    pub name: String,
    pub construct_from: Vec<InputConfig>,
    pub no_data_action: NoDataAction,
    pub push_interval_s: PushInterval,
    pub publish_on_service_bus: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinerConfig {
    pub output_streams: Vec<JoinConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BusConfig {
    pub port: u16,
    pub console_port: u16,
    pub joins: JoinerConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeviceConfig {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<Authentication>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct EventsConfig {
    pub enabled: bool,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Config {
    #[serde(default)]
    pub console: ConsoleConfig,
    #[serde(default = "default_tcpapps")]
    pub tcpapps: HashMap<String, AppConfig>,
    pub mqtt: MqttConfig,
    #[serde(default = "default_stream_count")]
    pub max_stream_count: usize,
    #[serde(default)]
    pub processes: Vec<ActionRoute>,
    #[serde(default)]
    pub script_runner: Vec<ActionRoute>,
    #[serde(skip)]
    pub actions_subscription: String,
    pub streams: HashMap<String, StreamConfig>,
    #[serde(default = "default_persistence_path")]
    pub persistence_path: PathBuf,
    #[serde(default = "default_file_size")]
    pub default_buf_size: usize,
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
    pub precondition_checks: Option<PreconditionCheckerConfig>,
    pub bus: Option<BusConfig>,
    #[serde(default)]
    pub events: EventsConfig,
}

impl Config {
    /// Saves updated config onto disk
    pub fn write_file(&self, path: &PathBuf) -> Result<(), Error> {
        let mut writer = File::create(path)?;
        let toml = toml::to_string(&self)?;
        write!(&mut writer, "{toml}")?;

        Ok(())
    }
}
