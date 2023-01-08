use config::{Environment, File, FileFormat};
use serde::Deserialize;
use structopt::StructOpt;

use std::{collections::HashMap, fs};

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
    /// Binary version
    #[structopt(skip = env ! ("VERGEN_BUILD_SEMVER"))]
    pub version: String,
    /// Build profile
    #[structopt(skip = env ! ("VERGEN_CARGO_PROFILE"))]
    pub profile: String,
    /// Commit SHA
    #[structopt(skip = env ! ("VERGEN_GIT_SHA"))]
    pub commit_sha: String,
    /// Commit SHA
    #[structopt(skip = env ! ("VERGEN_GIT_COMMIT_TIMESTAMP"))]
    pub commit_date: String,
    /// config file
    #[structopt(short = "c", help = "Config file")]
    pub config: Option<String>,
    /// config file
    #[structopt(short = "a", help = "Authentication file")]
    pub auth: String,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
    /// list of modules to log
    #[structopt(short = "m", long = "modules")]
    pub modules: Vec<String>,
}

const DEFAULT_CONFIG: &str = r#"
    bridge_port = 5555
    run_logcat = true
    max_packet_size = 102400
    max_inflight = 100
    keep_alive = 60

    # Whitelist of binaries which uplink can spawn as a process
    # This makes sure that user is protected against random actions
    # triggered from cloud.
    actions = ["tunshell"]

    # Create empty streams map
    [streams]

    [serializer_metrics]
    enabled = false

    [stream_metrics]
    enabled = false

    [action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    buf_size = 1

    [stats]
    enabled = false
    process_names = ["uplink"]
    update_period = 30
"#;

/// Reads config file to generate config struct and replaces places holders
/// like bike id and data version
pub fn initialize(auth_config: &str, uplink_config: &str) -> Result<Config, anyhow::Error> {
    let config = config::Config::builder()
        .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml))
        .add_source(File::from_str(uplink_config, FileFormat::Toml))
        .add_source(File::from_str(auth_config, FileFormat::Json))
        .add_source(Environment::default())
        .build()?;

    let mut config: Config = config.try_deserialize()?;

    if config.simulator.is_some() {
        config.device_id = "+".to_string();
    }

    if let Some(persistence) = &config.persistence {
        fs::create_dir_all(&persistence.path)?;
    }

    // replace placeholders with device/tenant ID
    let tenant_id = config.project_id.trim();
    let device_id = config.device_id.trim();
    for config in config.streams.values_mut() {
        replace_topic_placeholders(config, tenant_id, device_id);
    }

    replace_topic_placeholders(&mut config.action_status, tenant_id, device_id);

    for config in [&mut config.serializer_metrics, &mut config.stream_metrics] {
        if let Some(topic) = &config.topic {
            let topic = topic.replace("{tenant_id}", tenant_id);
            let topic = topic.replace("{device_id}", device_id);
            config.topic = Some(topic);
        }
    }

    for stat in [
        "disk_stats",
        "network_stats",
        "processor_stats",
        "process_stats",
        "component_stats",
        "system_stats",
    ] {
        config.bypass_streams.push("uplink_".to_string() + stat);
    }

    Ok(config)
}

// Replace placeholders in topic strings with configured values for tenant_id and device_id
fn replace_topic_placeholders(config: &mut StreamConfig, tenant_id: &str, device_id: &str) {
    if let Some(topic) = &config.topic {
        let topic = topic.replace("{tenant_id}", tenant_id);
        let topic = topic.replace("{device_id}", device_id);
        config.topic = Some(topic);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadFileError {
    #[error("Auth file not found at {0}")]
    Auth(String),
    #[error("Config file not found at {0}")]
    Config(String),
}

fn read_file_contents(path: &str) -> Option<String> {
    fs::read_to_string(path).ok()
}

pub fn get_configs(commandline: &CommandLine) -> Result<(String, Option<String>), ReadFileError> {
    let auth = read_file_contents(&commandline.auth)
        .ok_or_else(|| ReadFileError::Auth(commandline.auth.to_string()))?;
    let config = match &commandline.config {
        Some(path) => {
            Some(read_file_contents(path).ok_or_else(|| ReadFileError::Config(path.to_string()))?)
        }
        None => None,
    };

    Ok((auth, config))
}

pub const DEFAULT_TIMEOUT: u64 = 60;

#[inline]
fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StreamConfig {
    pub topic: Option<String>,
    pub buf_size: usize,
    #[serde(default = "default_timeout")]
    /// Duration(in seconds) that bridge collector waits from
    /// receiving first element, before the stream gets flushed.
    pub flush_period: u64,
}

fn default_file_size() -> usize {
    104857600 // 100MB
}

fn default_file_count() -> usize {
    3
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
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct JournalctlConfig {
    pub tags: Vec<String>,
    pub priority: u8,
    pub stream_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Downloader {
    pub actions: Vec<String>,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub topic: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<Authentication>,
    pub bridge_port: u16,
    pub max_packet_size: usize,
    pub max_inflight: u16,
    pub keep_alive: u64,
    pub actions: Vec<String>,
    pub persistence: Option<Persistence>,
    pub streams: HashMap<String, StreamConfig>,
    pub action_status: StreamConfig,
    pub serializer_metrics: MetricsConfig,
    pub stream_metrics: MetricsConfig,
    /// List of streams that are to be ignored by uplink's internal metrics collectors
    #[serde(default)]
    pub bypass_streams: Vec<String>,
    pub downloader: Option<Downloader>,
    pub stats: Stats,
    pub simulator: Option<SimulatorConfig>,
    #[serde(default)]
    pub ignore_actions_if_no_clients: bool,
    #[cfg(target_os = "linux")]
    pub journalctl: Option<JournalctlConfig>,
    #[cfg(target_os = "android")]
    pub run_logcat: bool,
}
