use crate::utils::byte_offset_to_position;
use bytes::BytesMut;
use reqwest::{Certificate, Identity};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// persistence_path is mandatory
// download_path is mandatory if any downloads are enabled
// directory structures
// - <download directory>
//   - <file version>
//     - <file name>
//     - metadata.txt - download url, checksum, size, how much has been downloaded
// - persistence
//   - uplink_metadata
//     - active_actions.json - on shutdown, collectors can save active actions to this file, it will be reloaded on reboot
//     - uplink_config.txt - will contain hash of config file and uplink version, uplink will throw away the state on disk if it doesn't match
//   - inflight.bin - mqtt inflight messages
//   - backup@corrupted
//   - <stream name>
//     - backum@1
//     - backup@2
//     - backup@corrupted
//
#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct UplinkConfig {
    #[serde(with = "crate::utils::path_parser")]
    pub download_path: PathBuf,
    #[serde(with = "crate::utils::path_parser")]
    pub persistence_path: PathBuf,
    pub enable_certificate_renewal: bool,
    pub enable_remote_shell: bool,
    #[serde(default = "default_streams_count")]
    pub max_dynamic_streams_count: usize,

    pub streams: HashMap<String, StreamConfig>,
    pub tcp_clients: HashMap<String, TcpClientConfig>,
    pub lib_actions: Vec<ActionConfig>,
    pub builtin_collectors: BuiltinCollectorsConfig,
    pub mqtt: MqttConfig,
}
fn default_streams_count() -> usize {
    5
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamConfig {
    pub compress: bool,
    pub buffer_size: usize,
    pub flush_interval: u64,
    pub persistence: PersistenceConfig,
}
impl Default for StreamConfig {
    fn default() -> Self {
        StreamConfig {
            compress: false,
            buffer_size: 128,
            flush_interval: 10,
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
        Self { max_file_size: 1024 * 1024, max_file_count: 0 }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TcpClientConfig {
    pub port: u16,
    pub actions: Vec<ActionConfig>,
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
        Self { enable: true, interval_seconds: 20 }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MtlsCerts {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}
#[derive(Clone, Deserialize, Serialize)]
pub struct HttpCreds {
    pub api_key: String,
    pub api_url: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MqttConfig {
    pub max_packet_size: usize,
    pub keep_alive: u64,
    pub max_inflight: u16,
    pub network_timeout: u64,
}
impl Default for MqttConfig {
    fn default() -> Self {
        Self { max_packet_size: 1024000, max_inflight: 100, keep_alive: 30, network_timeout: 30 }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<MtlsCerts>,
    pub http_credentials: HttpCreds,
}

pub fn parse_config(
    config_path: &str,
) -> Result<UplinkConfig, String> {
    let config_str = match std::fs::read_to_string(config_path) {
        Ok(s) => s,
        Err(e) => {
            return Err(format!("couldn't read config file: {e:?}"));
        }
    };

    let mut cfg = match toml::from_str::<UplinkConfig>(&config_str) {
        Ok(r) => r,
        Err(e) => {
            let mut msg = "Couldn't parse config file:\n".to_owned();
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
    cfg.streams.insert("action_status".into(), StreamConfig {
        compress: false,
        buffer_size: 1,
        flush_interval: 5,
        persistence: PersistenceConfig {
            max_file_size: 102400,
            max_file_count: 10,
        },
    });
    for metrics_stream in ["uplink_mqtt_metrics", "uplink_serializer_metrics"] {
        cfg.streams.insert(metrics_stream.into(), StreamConfig {
            compress: false,
            buffer_size: 1,
            flush_interval: 10,
            persistence: PersistenceConfig {
                max_file_size: 102400,
                max_file_count: 10,
            },
        });
    }

    if !cfg.lib_actions.is_empty() {
        return Err("unsupported parameter 'lib_actions'".into());
    }
    cfg.lib_actions = vec![
        ActionConfig {
            name: "renew_cert".to_string(),
        },
        ActionConfig {
            name: "update_uplink".to_string(),
        }
    ];
    if let Err(e) = validate_dir_permissions(&cfg.download_path) {
        return Err(format!(
            "encountered a problem with download_path({:?}):\n{e}",
            &cfg.download_path
        ));
    }
    if let Err(e) = validate_dir_permissions(&cfg.persistence_path) {
        return Err(format!(
            "encountered a problem with persistence_path({:?}):\n{e}",
            &cfg.persistence_path
        ));
    }

    Ok(cfg)
}

pub fn parse_auth_file(
    auth_file_path: &str,
) -> Result<AuthConfig, String> {
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
                "Couldn't parse auth file: error at line: {}, column: {}, message: {}",
                e.line(),
                e.column(),
                e
            ));
        }
    };

    if let Some(auth) = &auth.authentication {
        Certificate::from_pem(auth.ca_certificate.as_bytes())
            .map_err(|_| "invalid ca certificate".to_owned())?;
        let mut buf = BytesMut::from(auth.device_private_key.as_bytes());
        buf.extend_from_slice(auth.device_certificate.as_bytes());
        Identity::from_pem(&buf).map_err(|_| "invalid device certificates".to_owned())?;
    }

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
