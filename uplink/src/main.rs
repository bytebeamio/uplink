//! uplink is a utility/library to interact with the Bytebeam platform. It's internal architecture is described in the diagram below.
//! We use [`rumqttc`], which implements the MQTT protocol, to communicate with the platform. Communication is handled separately as ingress and egress
//! by [`Mqtt`](uplink::Mqtt) and [`Serializer`](uplink::Serializer) respectively. [`Action`](uplink::Action)s are received and forwarded by Mqtt to the
//! [`Middleware`](uplink::Middleware) module, where it is handled depending on its type and purpose, forwarding it to either the [`Bridge`](uplink::Bridge),
//! `Process`, [`FileDownloader`](uplink::FileDownloader) or [`TunshellSession`](uplink::TunshellSession). Bridge forwards received Actions to the devices
//! connected to it through the `bridge_port` and collects response data from these devices, to forward to the platform.
//!
//! Response data can be of multiple types, of interest to us are [`ActionResponse`](uplink::ActionResponse)s, which are forwarded to Actions
//! and then to Serializer where depending on the network, it may be stored onto disk with [`Storage`](disk::Storage) to ensure packets aren't lost.
//!
//!```text
//!                                                                                  ┌────────────┐
//!                                                                                  │MQTT backend│
//!                                                                                  └─────┐▲─────┘
//!                                                                                        ││
//!                                                                                 Action ││ ActionResponse
//!                                                                                        ││ / Data
//!                                                                            Action    ┌─▼└─┐
//!                                                                        ┌─────────────┤Mqtt◄──────────────┐
//!                                                                        │             └────┘              │ ActionResponse
//!                                                                        │                                 │ / Data
//!                                                                        │                                 │
//!                                                                   ┌────▼─────┐   ActionResponse     ┌────┴─────┐
//!                                                  ┌────────────────►Middleware├──────────────────────►Serializer│
//!                                                  │                └┬──┬──┬──┬┘                      └────▲─────┘
//!                                                  │                 │  │  │  │                            │
//!                                                  │                 │  │  │  └────────────────────┐       │Data
//!                                                  │     Tunshell Key│  │  │ Action                │    ┌──┴───┐   Action       ┌───────────┐
//!                                                  │        ┌────────┘  │  └───────────┐           ├────►Bridge◄────────────────►Application│
//!                                                  │  ------│-----------│--------------│---------- │    └──┬───┘ ActionResponse │ / Device  │
//!                                                  │  '     │           │              │         ' │       │       / Data       └───────────┘
//!                                                  │  '┌────▼───┐   ┌───▼───┐   ┌──────▼───────┐ ' │       │
//!                                                  │  '│Tunshell│   │Process│   │FileDownloader├───┘       │
//!                                                  │  '└────┬───┘   └───┬───┘   └──────┬───────┘ '         │
//!                                                  │  '     │           │              │         '         │
//!                                                  │  ------│-----------│--------------│----------         │
//!                                                  │        │           │              │                   │
//!                                                  └────────┴───────────┴──────────────┴───────────────────┘
//!                                                                      ActionResponse
//!```

mod apis;

use std::sync::Arc;
use std::thread;

use anyhow::Error;
use structopt::StructOpt;
use tokio::task::JoinSet;

use tracing::error;
use tracing_subscriber::fmt::format::{Format, Pretty};
use tracing_subscriber::{fmt::Layer, layer::Layered, reload::Handle};
use tracing_subscriber::{EnvFilter, Registry};

pub type ReloadHandle =
    Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

use bridge::StreamConfig;
use protocol::DEFAULT_TIMEOUT;
use uplink::config::{AppConfig, Config};
use uplink::{simulator, TcpJson, Uplink};

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
    action_redirections = { "update_firmware" = "install_firmware" }
    
    [mqtt]
    max_packet_size = 256000
    max_inflight = 100
    keep_alive = 30

    # Create empty streams map
    [streams]

    # Downloader config
    [downloader]
    actions = [{ name = "update_firmware", timeout = 60 }, { name = "send_file", timeout = 60 }]
    path = "/var/tmp/ota-file"

    [stream_metrics]
    enabled = false
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_stream_metrics/jsonarray"
    blacklist = []
    timeout = 10

    [serializer_metrics]
    enabled = false
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_serializer_metrics/jsonarray"
    timeout = 10

    [mqtt_metrics]
    enabled = true
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_mqtt_metrics/jsonarray"

    [action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    buf_size = 1
    timeout = 10

    [streams.device_shadow]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/device_shadow/jsonarray"
    buf_size = 1

    [system_stats]
    enabled = true
    process_names = ["uplink"]
    update_period = 30

    [tcpapps.1]
    port = 5555

    [ota_installer]
    path = "/var/tmp/ota"
    actions = [{ name = "install_firmware", timeout = 60 }]
    uplink_port = 5555
"#;

/// Reads config file to generate config struct and replaces places holders
/// like bike id and data version
pub fn initialize(auth_config: &str, uplink_config: &str) -> Result<Config, anyhow::Error> {
    use ::config::{Environment, File, FileFormat};

    let config = ::config::Config::builder()
        .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml))
        .add_source(File::from_str(uplink_config, FileFormat::Toml))
        .add_source(File::from_str(auth_config, FileFormat::Json))
        .add_source(Environment::default())
        .build()?;

    let mut config: Config = config.try_deserialize()?;

    if let Some(persistence) = &config.serializer.persistence {
        std::fs::create_dir_all(&persistence.path)?;
    }

    config.serializer.max_packet_size = config.mqtt.max_packet_size;

    // replace placeholders with device/tenant ID
    let tenant_id = config.bridge.project_id.trim();
    let device_id = config.bridge.device_id.trim();
    for config in config.bridge.streams.values_mut() {
        replace_topic_placeholders(&mut config.topic, tenant_id, device_id);
    }

    replace_topic_placeholders(&mut config.bridge.action_status.topic, tenant_id, device_id);
    replace_topic_placeholders(&mut config.bridge.stream_metrics.topic, tenant_id, device_id);
    replace_topic_placeholders(&mut config.serializer_metrics.topic, tenant_id, device_id);
    replace_topic_placeholders(&mut config.mqtt_metrics.topic, tenant_id, device_id);

    // for config in [&mut config.serializer_metrics, &mut config.stream_metrics] {
    //     if let Some(topic) = &config.topic {
    //         let topic = topic.replace("{tenant_id}", tenant_id);
    //         let topic = topic.replace("{device_id}", device_id);
    //         config.topic = Some(topic);
    //     }
    // }

    if config.system_stats.enabled {
        for stream_name in [
            "uplink_disk_stats",
            "uplink_network_stats",
            "uplink_processor_stats",
            "uplink_process_stats",
            "uplink_component_stats",
            "uplink_system_stats",
        ] {
            config.bridge.stream_metrics.blacklist.push(stream_name.to_owned());
            let stream_config = StreamConfig {
                topic: format!(
                    "/tenants/{tenant_id}/devices/{device_id}/events/{stream_name}/jsonarray"
                ),
                buf_size: config.system_stats.stream_size.unwrap_or(100),
                flush_period: DEFAULT_TIMEOUT,
            };
            config.bridge.streams.insert(stream_name.to_owned(), stream_config);
        }
    }

    let action_topic_template = "/tenants/{tenant_id}/devices/{device_id}/actions";
    let mut device_action_topic = action_topic_template.to_string();
    replace_topic_placeholders(&mut device_action_topic, tenant_id, device_id);
    config.actions_subscription = device_action_topic;

    // Add topics to be subscribed to for simulation purposes, if in simulator mode
    if let Some(sim_cfg) = &mut config.simulator {
        for n in 1..=sim_cfg.num_devices {
            let mut topic = action_topic_template.to_string();
            replace_topic_placeholders(&mut topic, tenant_id, &n.to_string());
            sim_cfg.actions_subscriptions.push(topic);
        }
    }

    Ok(config)
}

// Replace placeholders in topic strings with configured values for tenant_id and device_id
fn replace_topic_placeholders(topic: &mut String, tenant_id: &str, device_id: &str) {
    *topic = topic.replace("{tenant_id}", tenant_id);
    *topic = topic.replace("{device_id}", device_id);
}

#[derive(Debug, thiserror::Error)]
pub enum ReadFileError {
    #[error("Auth file not found at {0}")]
    Auth(String),
    #[error("Config file not found at {0}")]
    Config(String),
}

fn read_file_contents(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
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

fn initialize_logging(commandline: &CommandLine) -> ReloadHandle {
    let level = match commandline.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    let levels =
        match commandline.modules.clone().into_iter().reduce(|acc, e| format!("{acc},{e}={level}"))
        {
            Some(f) => f,
            _ => format!("uplink={level},disk={level}"),
        };

    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(levels)
        .with_filter_reloading();

    let reload_handle = builder.reload_handle();

    builder.try_init().expect("initialized subscriber succesfully");

    reload_handle
}

fn banner(commandline: &CommandLine, config: &Arc<Config>) {
    const B: &str = r#"
    ░█░▒█░▄▀▀▄░█░░░▀░░█▀▀▄░█░▄
    ░█░▒█░█▄▄█░█░░░█▀░█░▒█░█▀▄
    ░░▀▀▀░█░░░░▀▀░▀▀▀░▀░░▀░▀░▀
    "#;

    println!("{B}");
    println!("    version: {}", commandline.version);
    println!("    profile: {}", commandline.profile);
    println!("    commit_sha: {}", commandline.commit_sha);
    println!("    commit_date: {}", commandline.commit_date);
    println!("    project_id: {}", config.bridge.project_id);
    println!("    device_id: {}", config.bridge.device_id);
    println!("    remote: {}:{}", config.broker, config.port);
    if !config.bridge.action_redirections.is_empty() {
        println!("    action redirections:");
        for (action, redirection) in config.bridge.action_redirections.iter() {
            println!("        {action} -> {redirection}");
        }
    }
    if !config.tcpapps.is_empty() {
        println!("    tcp applications:");
        for (app, AppConfig { port, actions }) in config.tcpapps.iter() {
            println!("        name: {app:?}");
            println!("        port: {port}");
            println!("        actions: {actions:?}");
            println!("        @");
        }
    }
    println!("    secure_transport: {}", config.authentication.is_some());
    println!("    max_packet_size: {}", config.mqtt.max_packet_size);
    println!("    max_inflight_messages: {}", config.mqtt.max_inflight);
    println!("    keep_alive_timeout: {}", config.mqtt.keep_alive);
    if let Some(persistence) = &config.serializer.persistence {
        println!("    persistence_dir: {}", persistence.path);
        println!("    persistence_max_segment_size: {}", persistence.max_file_size);
        println!("    persistence_max_segment_count: {}", persistence.max_file_count);
    }
    println!(
        "    downloader:\n\tpath: {}\n\tactions: {:?}",
        config.downloader.path, config.downloader.actions
    );
    println!(
        "    installer:\n\tpath: {}\n\tactions: {:?}",
        config.ota_installer.path, config.ota_installer.actions
    );
    if config.system_stats.enabled {
        println!("    processes: {:?}", config.system_stats.process_names);
    }
    if config.apis.enabled {
        println!("    tracing: http://localhost:{}", config.apis.port);
    }
    println!("\n");
}

fn main() -> Result<(), Error> {
    if std::env::args().any(|a| a == "--sha") {
        println!("{}", &env!("VERGEN_GIT_SHA")[0..8]);
        return Ok(());
    }

    let commandline: CommandLine = StructOpt::from_args();
    let reload_handle = initialize_logging(&commandline);

    let (auth, config) = get_configs(&commandline)?;
    let config = Arc::new(initialize(&auth, &config.unwrap_or_default())?);

    banner(&commandline, &config);

    let mut uplink = Uplink::new(config.clone())?;
    let bridge = uplink.spawn()?;

    if let Some(config) = config.simulator.clone() {
        let bridge = bridge.clone();
        thread::spawn(move || {
            simulator::start(bridge, &config).unwrap();
        });
    }

    if config.apis.enabled {
        let port = config.apis.port;
        thread::spawn(move || apis::start(port, reload_handle));
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .thread_name("tcpjson")
        .build()
        .unwrap();

    rt.block_on(async {
        let mut handles = JoinSet::new();
        for (app, cfg) in config.tcpapps.iter() {
            let tcpjson = TcpJson::new(app.to_owned(), cfg.clone(), bridge.clone()).await;
            handles.spawn(async move {
                if let Err(e) = tcpjson.start().await {
                    error!("App failed. Error = {:?}", e);
                }
            });
        }

        while let Some(Err(e)) = handles.join_next().await {
            error!("App failed. Error = {:?}", e);
        }
    });
    Ok(())
}
