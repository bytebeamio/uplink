mod console;

use std::fs::read_to_string;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Error;
use config::{Environment, File, FileFormat};
use log::info;
use structopt::StructOpt;
use tokio::time::sleep;
use tracing::error;
use tracing_subscriber::fmt::format::{Format, Pretty};
use tracing_subscriber::{fmt::Layer, layer::Layered, reload::Handle};
use tracing_subscriber::{EnvFilter, Registry};

pub type ReloadHandle =
    Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

use uplink::config::{AppConfig, Config, StreamConfig, MAX_BATCH_SIZE};
use uplink::{simulator, spawn_named_thread, TcpJson, Uplink};

const DEFAULT_CONFIG: &str = r#"    
    [mqtt]
    max_packet_size = 256000
    max_inflight = 100
    keep_alive = 30
    network_timeout = 30

    [stream_metrics]
    enabled = false
    bridge_topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_stream_metrics/jsonarray"
    serializer_topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_serializer_stream_metrics/jsonarray"
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
    batch_size = 1
    flush_period = 2
    persistence = { max_file_count = 1 } # Ensures action responses are not lost on restarts
    priority = 255 # highest priority for quick delivery of action status info to platform

    [streams.device_shadow]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/device_shadow/jsonarray"
    flush_period = 5

    [streams.logs]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/logs/jsonarray"
    batch_size = 32

    [system_stats]
    enabled = true
    process_names = ["uplink"]
    update_period = 30
"#;

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
    pub config: Option<PathBuf>,
    /// config file
    #[structopt(short = "a", help = "Authentication file")]
    pub auth: PathBuf,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
    /// list of modules to log
    #[structopt(short = "m", long = "modules")]
    pub modules: Vec<String>,
}

impl CommandLine {
    /// Reads config file to generate config struct and replaces places holders
    /// like bike id and data version
    fn get_configs(&self) -> Result<Config, anyhow::Error> {
        let mut config =
            config::Config::builder().add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml));

        if let Some(path) = &self.config {
            let read = read_to_string(path).map_err(|e| {
                Error::msg(format!(
                    "Config file couldn't be loaded from {:?}; error = {e}",
                    path.display()
                ))
            })?;
            config = config.add_source(File::from_str(&read, FileFormat::Toml));
        }

        let auth = read_to_string(&self.auth).map_err(|e| {
            Error::msg(format!(
                "Auth file couldn't be loaded from {:?}; error = {e}",
                self.auth.display()
            ))
        })?;
        config = config.add_source(File::from_str(&auth, FileFormat::Json));

        let mut config: Config =
            config.add_source(Environment::default()).build()?.try_deserialize()?;

        // Create directory at persistence_path if it doesn't already exist
        std::fs::create_dir_all(&config.persistence_path).map_err(|_| {
            Error::msg(format!(
                "Permission denied for creating persistence directory at {:?}",
                config.persistence_path.display()
            ))
        })?;

        // replace placeholders with device/tenant ID
        let tenant_id = config.project_id.trim();
        let device_id = config.device_id.trim();

        // Replace placeholders in topic strings with configured values for tenant_id and device_id
        // e.g. for tenant_id: "demo"; device_id: "123"
        // "/tenants/{tenant_id}/devices/{device_id}/events/stream/jsonarry" ~> "/tenants/demo/devices/123/events/stream/jsonarry"
        let replace_topic_placeholders = |topic: &mut String| {
            *topic = topic.replace("{tenant_id}", tenant_id).replace("{device_id}", device_id);
        };

        for config in config.streams.values_mut() {
            replace_topic_placeholders(&mut config.topic);
        }

        replace_topic_placeholders(&mut config.action_status.topic);
        replace_topic_placeholders(&mut config.stream_metrics.bridge_topic);
        replace_topic_placeholders(&mut config.stream_metrics.serializer_topic);
        replace_topic_placeholders(&mut config.serializer_metrics.topic);
        replace_topic_placeholders(&mut config.mqtt_metrics.topic);

        if config.system_stats.enabled {
            for stream_name in [
                "uplink_disk_stats",
                "uplink_network_stats",
                "uplink_processor_stats",
                "uplink_process_stats",
                "uplink_component_stats",
                "uplink_system_stats",
            ] {
                config.stream_metrics.blacklist.push(stream_name.to_owned());
                let stream_config = StreamConfig {
                    topic: format!(
                        "/tenants/{tenant_id}/devices/{device_id}/events/{stream_name}/jsonarray"
                    ),
                    batch_size: config.system_stats.stream_size.unwrap_or(MAX_BATCH_SIZE),
                    ..Default::default()
                };
                config.streams.insert(stream_name.to_owned(), stream_config);
            }
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(batch_size) = config.logging.as_ref().and_then(|c| c.stream_size) {
            let stream_config =
                config.streams.entry("logs".to_string()).or_insert_with(|| StreamConfig {
                    topic: format!(
                        "/tenants/{tenant_id}/devices/{device_id}/events/logs/jsonarray"
                    ),
                    batch_size: 32,
                    ..Default::default()
                });
            stream_config.batch_size = batch_size;
        }

        config.actions_subscription = format!("/tenants/{tenant_id}/devices/{device_id}/actions");

        // downloader actions are cancellable by default
        for route in config.downloader.actions.iter_mut() {
            route.cancellable = true;
        }

        // process actions are cancellable by default
        for route in config.processes.iter_mut() {
            route.cancellable = true;
        }

        // script runner actions are cancellable by default
        for route in config.script_runner.iter_mut() {
            route.cancellable = true;
        }

        Ok(config)
    }

    fn initialize_logging(&self) -> ReloadHandle {
        let level = match self.verbose {
            0 => "warn",
            1 => "info",
            2 => "debug",
            _ => "trace",
        };

        let levels =
            match self.modules.clone().into_iter().reduce(|e, acc| format!("{e}={level},{acc}")) {
                Some(f) => format!("{f}={level}"),
                _ => format!("uplink={level},storage={level}"),
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

    fn banner(&self, config: &Config) {
        const B: &str = r#"
        ░█░▒█░▄▀▀▄░█░░░▀░░█▀▀▄░█░▄
        ░█░▒█░█▄▄█░█░░░█▀░█░▒█░█▀▄
        ░░▀▀▀░█░░░░▀▀░▀▀▀░▀░░▀░▀░▀
        "#;

        println!("{B}");
        println!("    version: {}", self.version);
        println!("    profile: {}", self.profile);
        println!("    commit_sha: {}", self.commit_sha);
        println!("    commit_date: {}", self.commit_date);
        println!("    project_id: {}", config.project_id);
        println!("    device_id: {}", config.device_id);
        println!("    remote: {}:{}", config.broker, config.port);
        println!("    persistence_path: {}", config.persistence_path.display());
        if !config.action_redirections.is_empty() {
            println!("    action redirections:");
            for (action, redirection) in config.action_redirections.iter() {
                println!("\t{action} -> {redirection}");
            }
        }
        if !config.tcpapps.is_empty() {
            println!("    tcp applications:");
            for (app, AppConfig { port, actions }) in config.tcpapps.iter() {
                println!("\tname: {app:?}\n\tport: {port}\n\tactions: {actions:?}\n\t@");
            }
        }
        println!("    secure_transport: {}", config.authentication.is_some());
        println!("    max_packet_size: {}", config.mqtt.max_packet_size);
        println!("    max_inflight_messages: {}", config.mqtt.max_inflight);
        println!("    keep_alive_timeout: {}", config.mqtt.keep_alive);

        if !config.downloader.actions.is_empty() {
            println!(
                "    downloader:\n\tpath: {:?}\n\tactions: {:?}",
                config.downloader.path.display(),
                config.downloader.actions
            );
        }
        if !config.ota_installer.actions.is_empty() {
            println!(
                "    installer:\n\tpath: {}\n\tactions: {:?}",
                config.ota_installer.path, config.ota_installer.actions
            );
        }
        if config.system_stats.enabled {
            println!("    processes: {:?}", config.system_stats.process_names);
        }
        if config.console.enabled {
            println!("    console: http://localhost:{}", config.console.port);
        }
        println!("\n");
    }
}

fn main() -> Result<(), Error> {
    if std::env::args().any(|a| a == "--sha") {
        println!("{}", &env!("VERGEN_GIT_SHA")[0..8]);
        return Ok(());
    }

    let commandline: CommandLine = StructOpt::from_args();
    let reload_handle = commandline.initialize_logging();
    let config = commandline.get_configs()?;
    commandline.banner(&config);

    let config = Arc::new(config);
    let mut uplink = Uplink::new(config.clone())?;
    let mut bridge = uplink.configure_bridge();
    uplink.spawn_builtins(&mut bridge)?;

    let bridge_tx = bridge.bridge_tx();

    let mut tcpapps = vec![];
    for (app, cfg) in config.tcpapps.clone() {
        let route_rx = if !cfg.actions.is_empty() {
            let actions_rx = bridge.register_action_routes(&cfg.actions)?;
            Some(actions_rx)
        } else {
            None
        };
        tcpapps.push(TcpJson::new(app, cfg, route_rx, bridge.bridge_tx()));
    }

    let simulator_actions = match &config.simulator {
        Some(cfg) if !cfg.actions.is_empty() => {
            let actions_rx = bridge.register_action_routes(&cfg.actions)?;
            Some(actions_rx)
        }
        _ => None,
    };

    let downloader_disable = Arc::new(Mutex::new(false));
    let ctrl_tx = uplink.spawn(bridge, downloader_disable.clone())?;

    if let Some(config) = config.simulator.clone() {
        spawn_named_thread("Simulator", || {
            simulator::start(config, bridge_tx, simulator_actions).unwrap();
        });
    }

    if config.console.enabled {
        let port = config.console.port;
        let ctrl_tx = ctrl_tx.clone();
        spawn_named_thread("Uplink Console", move || {
            console::start(port, reload_handle, ctrl_tx, downloader_disable)
        });
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .thread_name("tcpjson")
        .build()
        .unwrap();

    rt.block_on(async {
        for app in tcpapps {
            tokio::task::spawn(async move {
                if let Err(e) = app.start().await {
                    error!("App failed. Error = {e}");
                }
            });
        }

        #[cfg(unix)]
        tokio::spawn(async move {
            use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
            use signal_hook_tokio::Signals;
            use tokio_stream::StreamExt;

            let mut signals = Signals::new([SIGTERM, SIGINT, SIGQUIT]).unwrap();
            // Handle a shutdown signal from POSIX
            while let Some(signal) = signals.next().await {
                match signal {
                    SIGTERM | SIGINT | SIGQUIT => ctrl_tx.trigger_shutdown().await,
                    s => error!("Couldn't handle signal: {s}"),
                }
            }
        });

        uplink.resolve_on_shutdown().await.unwrap();
        info!("Uplink shutting down...");
        // NOTE: wait 5s to allow serializer to write to network/disk
        sleep(Duration::from_secs(10)).await;
    });

    Ok(())
}
