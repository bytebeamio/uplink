use std::collections::HashMap;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use crate::utils::byte_offset_to_position;
use std::str::FromStr;
use reqwest::{Certificate, Identity};
use bytes::BytesMut;

#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct UplinkConfig {
    pub download_path: String,
    pub persistence_path: String,
    pub prioritize_live_data: bool,
    pub enable_certificate_renewal: bool,
    pub enable_remote_shell: bool,

    pub streams: HashMap<String, StreamConfig>,
    pub socket_clients: HashMap<String, SocketClientConfig>,
    pub lib_actions: Option<Vec<ActionConfig>>,
    pub builtin_collectors: BuiltinCollectorsConfig,
    pub mqtt: MqttConfig,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamConfig {
    pub compress: bool,
    pub buffer_size: u64,
    pub flush_interval: u32,
    pub persistence: Option<PersistenceConfig>,
    pub http_delivery: bool,
}
impl Default for StreamConfig {
    fn default() -> Self {
        StreamConfig {
            compress: false,
            buffer_size: 64,
            flush_interval: 10,
            persistence: None,
            http_delivery: false,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    pub max_file_size: u64,
    pub max_file_count: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SocketClientConfig {
    pub socket_path: String,
    pub actions: Vec<ActionConfig>
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActionConfig {
    pub name: String,
    #[serde(default)]
    pub download_fw: bool,
}

#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BuiltinCollectorsConfig {
    pub device_shadow: DeviceShadowConfig,
    pub uplink_metrics: UplinkMetricsConfig,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct DeviceShadowConfig {
    pub enable: bool,
    pub interval_seconds: u32,
}
impl Default for DeviceShadowConfig {
    fn default() -> Self {
        Self {
            enable: true,
            interval_seconds: 20,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct UplinkMetricsConfig {
    pub enable: bool,
}
impl Default for UplinkMetricsConfig {
    fn default() -> Self {
        Self {
            enable: true,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MtlsCerts {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MqttConfig {
    pub max_packet_size: usize,
    pub keep_alive: u64,
    pub max_inflight: u16,
    pub network_timeout: u64
}
impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            max_packet_size: 1024000,
            max_inflight: 100,
            keep_alive: 30,
            network_timeout: 30,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct AuthConfig {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<MtlsCerts>,
}


pub fn parse_config(config_path: &str, auth_file_path: &str) -> Result<(UplinkConfig, AuthConfig), String> {
    let config_str = match std::fs::read_to_string(config_path) {
        Ok(s) => s,
        Err(e) => {
            return Err(format!("couldn't read config file: {e:?}"));
        }
    };

    let cfg = match toml::from_str::<UplinkConfig>(&config_str) {
        Ok(r) => r,
        Err(e) => {
            let mut msg = "Couldn't parse config file:\n".to_owned();
            if let Some(span) = e.span() {
                if let Ok((line, column )) = byte_offset_to_position(&config_str, span.start) {
                    msg.push_str(&format!("Error at: line {line}, column {column}\n"));
                }
            }
            msg.push_str(&format!("message: {}", e.message()));
            return Err(msg);
        }
    };

    for (stream_name, stream_cfg) in cfg.streams.iter() {
        if stream_name == "action_status" {
            return Err("action_status is a special stream and cannot be configured".into());
        }
        if stream_cfg.http_delivery && stream_cfg.persistence.is_some() {
            return Err(format!("[streams.{stream_name}]: persistence should not be specified if http_delivery is enabled"));
        }
    }

    if cfg.lib_actions.is_some() {
        return Err("unsupported parameter 'lib_actions'".into());
    }
    if let Err(e) = validate_dir_permissions(&cfg.download_path) {
        return Err(format!("encountered a problem with download_path({}):\n{e}", &cfg.download_path))
    }
    if let Err(e) = validate_dir_permissions(&cfg.persistence_path) {
        return Err(format!("encountered a problem with persistence_path({}):\n{e}", &cfg.persistence_path))
    }

    let auth_str = match std::fs::read_to_string(auth_file_path) {
        Ok(s) => s,
        Err(e) => {
            return Err(format!("couldn't read auth file: {e:?}"));
        }
    };

    let auth = match serde_json::from_str::<AuthConfig>(&auth_str) {
        Ok(r) => r,
        Err(e) => {
            return Err(format!("Couldn't parse auth file: error at line: {}, column: {}, message: {}", e.line(), e.column(), e));
        }
    };

    if let Some(auth) = &auth.authentication {
        Certificate::from_pem(auth.ca_certificate.as_bytes()).map_err(|_| "invalid ca certificate".to_owned())?;
        let mut buf = BytesMut::from(auth.device_private_key.as_bytes());
        buf.extend_from_slice(auth.device_certificate.as_bytes());
        Identity::from_pem(&buf).map_err(|_| "invalid device certificates".to_owned())?;
    }

    Ok((cfg, auth))
}

fn validate_dir_permissions(path: &str) -> Result<(), String> {
    let path = PathBuf::from_str(path).unwrap();
    std::fs::create_dir_all(&path)
        .map_err(|e| format!("couldn't create directory: {e:?}"))?;
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