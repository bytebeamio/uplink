use std::env::current_dir;
use std::path::PathBuf;
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};
use config::{File, FileFormat};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};

// #[cfg(target_os = "linux")]
// use crate::collector::journalctl::JournalCtlConfig;
// #[cfg(target_os = "android")]
// use crate::collector::logcat::LogcatConfig;
use crate::{hashmap, ActionCallback, DEFAULT_CONFIG};
use crate::uplink_config::ActionKind::download;

#[serde_as]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StreamConfig {
    pub topic: String,
    pub batch_size: usize,
    /// Duration(in seconds) that bridge collector waits from
    /// receiving first element, before the stream gets flushed.
    pub flush_period_secs: u32,
    pub compress_data: bool,
    pub persistence: Persistence,
    pub priority: u8,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            topic: "".to_owned(),
            batch_size: 128,
            flush_period_secs: Duration::from_secs(60),
            compress_data: false,
            persistence: Persistence::default(),
            priority: 0,
        }
    }
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq, Eq, PartialOrd)]
pub struct Persistence {
    pub max_file_size: usize,
    pub max_file_count: usize,
}

impl Default for Persistence {
    fn default() -> Self {
        Persistence { max_file_size: 1024000, max_file_count: 0 }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SystemStats {
    pub enabled: bool,
    pub process_names: Vec<String>,
    pub update_period: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct InstallerConfig {
    pub path: String,
    pub actions: Vec<String>,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StreamMetricsConfig {
    pub enabled: bool,
    pub interval_secs: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SerializerMetricsConfig {
    pub enabled: bool,
    pub interval_secs: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MqttMetricsConfig {
    pub enabled: bool,
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

#[derive(Clone, Debug, Deserialize)]
pub struct DeviceShadowConfig {
    pub enabled: bool,
    pub interval_seconds: u32,
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
pub struct Authentication {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub max_stream_count: usize,
    pub prioritize_live_data: bool,
    pub enable_remote_shell: bool,
    pub enable_script_runner: bool,
    #[serde(default = "default_persistence_path")]
    pub persistence_path: PathBuf,
    #[serde(default = "default_download_path")]
    pub download_path: PathBuf,
    // pub tcpapps: HashMap<String, AppConfig>,
    // pub stdin_collector_config: StdinCollectorConfig,
    // pub console: ConsoleConfig,
    // pub ota_installer: InstallerConfig,
    pub mqtt: MqttConfig,
    pub streams: HashMap<String, StreamConfig>,
    pub stream_metrics: StreamMetricsConfig,
    pub serializer_metrics: SerializerMetricsConfig,
    pub mqtt_metrics: MqttMetricsConfig,
    pub system_stats: SystemStats,
    pub device_shadow: DeviceShadowConfig,
    #[cfg(target_os = "linux")]
    pub journalctl_args: Option<Vec<String>>,
    #[cfg(target_os = "android")]
    pub logcat_args: Option<Vec<String>>,
    pub actions: HashMap<String, ActionConfig>,
    private_field_to_disable_public_constructor: (),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActionConfig {
    kind: ActionKind,
}

#[derive(Debug, Clone, Deserialize)]
pub enum ActionKind {
    simple,
    download,
    extract_and_run,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ActionRoute {
    pub name: String,
    pub download: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_stream_count: 30,
            prioritize_live_data: false,
            enable_remote_shell: true,
            enable_script_runner: true,
            persistence_path: default_persistence_path(),
            download_path: default_download_path(),
            mqtt: MqttConfig {
                max_packet_size: 256000,
                max_inflight: 100,
                keep_alive: 30,
                network_timeout: 30,
            },
            streams: hashmap!(
                "action_status".to_owned() => StreamConfig {
                    topic: "/tenants/{tenant_id}/devices/{device_id}/action/status".to_owned(),
                    batch_size: 1,
                    flush_period_secs: 1,
                    compress_data: false,
                    persistence: Persistence {
                        max_file_size: 102400,
                        max_file_count: 1,
                    },
                    priority: 100,
                }
            ),
            stream_metrics: StreamMetricsConfig {
                enabled: false,
                interval_secs: 30,
            },
            serializer_metrics: SerializerMetricsConfig {
                enabled: false,
                interval_secs: 30,
            },
            mqtt_metrics: MqttMetricsConfig {
                enabled: false,
            },
            system_stats: SystemStats {
                enabled: true,
                process_names: vec![],
                update_period: 10,
            },
            device_shadow: DeviceShadowConfig {
                enabled: true,
                interval_seconds: 10,
            },
            #[cfg(target_os = "linux")]
            journalctl_args: None,
            #[cfg(target_os = "android")]
            logcat_args: None,
            actions: hashmap!(
                "update_firmware".to_owned() => ActionConfig {
                    kind: download
                }
            ),
            private_field_to_disable_public_constructor: (),
        }
    }
}

fn config_template_for_stream(name: &str, compress) -> String {
    format!("/tenants/{tenant_id}/devices/{device_id}/events/")
}

pub struct UplinkConfig {
    pub app_config: Config,
    pub credentials: DeviceConfig,
    pub actions_callback: Option<ActionCallback>,
}

impl UplinkConfig {
    pub fn topic_for_stream(&self, name: &str) -> String {
        format!("/tenants/{}/devices/{}/events/{name}/jsonarray", self.credentials.project_id, self.credentials.device_id)
    }
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