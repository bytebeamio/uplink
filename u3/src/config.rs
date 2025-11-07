use crate::utils::byte_offset_to_position;
use log::warn;
use reqwest::{Certificate, Identity};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// persistence_path is mandatory
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct UplinkConfig {
    #[serde(with = "crate::utils::path_parser")]
    pub persistence_path: Option<PathBuf>,
    pub enable_certificate_renewal: bool,
    pub enable_remote_shell: bool,
    pub max_dynamic_streams_count: usize,
    pub max_packet_size: usize,

    pub streams: HashMap<String, StreamConfig>,
    pub tcp_clients: HashMap<String, TcpClientConfig>,
    pub lib_actions: Vec<ActionConfig>,
    pub builtin_collectors: BuiltinCollectorsConfig,
}
impl Default for UplinkConfig {
    fn default() -> Self {
        Self {
            persistence_path: None,
            enable_certificate_renewal: true,
            enable_remote_shell: true,
            max_dynamic_streams_count: 5,
            max_packet_size: 1024 * 1024 * 3,
            streams: Default::default(),
            tcp_clients: Default::default(),
            lib_actions: vec![],
            builtin_collectors: Default::default(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamConfig {
    pub compress: bool,
    pub buffer_size: usize,
    pub flush_interval: u64,
    pub priority: i32,
    pub persistence: PersistenceConfig,
}
impl Default for StreamConfig {
    fn default() -> Self {
        StreamConfig {
            compress: false,
            buffer_size: 128,
            flush_interval: 10,
            priority: 0,
            persistence: PersistenceConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PersistenceConfig {
    pub max_file_size: usize,
    pub max_file_count: usize,
}
impl Default for PersistenceConfig {
    fn default() -> Self {
        Self { max_file_size: 100 * 1024, max_file_count: 0 }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TcpClientConfig {
    pub port: u16,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActionConfig {
    pub name: String,
}

#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct BuiltinCollectorsConfig {
    pub device_shadow: DeviceShadowConfig,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct DeviceShadowConfig {
    pub enable: bool,
    pub interval_seconds: u32,
}
impl Default for DeviceShadowConfig {
    fn default() -> Self {
        Self { enable: true, interval_seconds: 5 }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct HttpCreds {
    pub api_key: String,
    pub api_url: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub project_id: String,
    pub device_id: String,
    pub http_credentials: HttpCreds,
}

pub fn parse_config(config_path: &str) -> Result<UplinkConfig, String> {
    let config_str = match std::fs::read_to_string(config_path) {
        Ok(s) => s,
        Err(e) => {
            return Err(format!("couldn't read config file: {e:?}"));
        }
    };

    let mut cfg = match toml::from_str::<UplinkConfig>(&config_str) {
        Ok(r) => r,
        Err(e) => {
            let mut msg = "couldn't parse config file:\n".to_owned();
            if let Some(span) = e.span() {
                if let Ok((line, column)) = byte_offset_to_position(&config_str, span.start) {
                    msg.push_str(&format!("Error at: line {line}, column {column}\n"));
                }
            }
            msg.push_str(&format!("message: {}", e.message()));
            return Err(msg);
        }
    };

    for (stream_name, _) in cfg.streams.iter() {
        if stream_name == "action_status" {
            return Err("action_status is a special stream and cannot be configured".into());
        }
    }
    for metrics_stream in ["device_shadow", "uplink_serializer_metrics", "uplink_stream_metrics"] {
        cfg.streams.insert(
            metrics_stream.into(),
            StreamConfig {
                compress: false,
                buffer_size: 1,
                flush_interval: 5,
                priority: 0,
                persistence: PersistenceConfig { max_file_size: 102400, max_file_count: 10 },
            },
        );
    }
    // cfg.streams.insert(
    //     "device_shadow".into(),
    //     StreamConfig {
    //         compress: false,
    //         buffer_size: 1,
    //         flush_interval: 1,
    //         priority: 0,
    //         persistence: PersistenceConfig { max_file_size: 102400, max_file_count: 10 },
    //     },
    // );

    if !cfg.lib_actions.is_empty() {
        return Err("unsupported parameter 'lib_actions'".into());
    }
    cfg.lib_actions = vec![
        ActionConfig { name: "renew_cert".to_string() },
        ActionConfig { name: "update_uplink".to_string() },
    ];
    if let Some(p) = &cfg.persistence_path {
        if let Err(e) = validate_dir_permissions(p) {
            return Err(format!("encountered a problem with persistence_path({p:?}):\n{e}",));
        }
    } else {
        warn!("persistence_path not specified, persistence disabled!");
        for cfg in cfg.streams.values_mut() {
            cfg.persistence.max_file_count = 0;
        }
    }

    Ok(cfg)
}

pub fn parse_auth_file(auth_file_path: &str) -> Result<AuthConfig, String> {
    let auth_str = match std::fs::read_to_string(auth_file_path) {
        Ok(s) => s,
        Err(e) => {
            return Err(format!("couldn't read auth file: {e:?}"));
        }
    };

    let auth = match serde_json::from_str::<AuthConfig>(&auth_str) {
        Ok(r) => r,
        Err(e) => {
            return Err(format!(
                "couldn't parse auth file: error at line: {}, column: {}, message: {}",
                e.line(),
                e.column(),
                e
            ));
        }
    };

    Ok(auth)
}

fn validate_dir_permissions(path: &Path) -> Result<(), String> {
    if path.is_relative() {
        return Err("path has to be absolute".into());
    }
    std::fs::create_dir_all(&path).map_err(|e| format!("couldn't create directory: {e:?}"))?;
    let test_file = path.join(format!("fs_test_{}", rand::random::<u32>()));
    std::fs::write(&test_file, "test_file")
        .map_err(|e| format!("can't create files in this directory: {e:?}"))?;
    std::fs::remove_file(test_file)
        .map_err(|e| format!("couldn't remove file from directory: {e:?}"))?;
    Ok(())
}

#[test]
fn t1() {
    dbg!(std::fs::create_dir_all("/Users"));
}
