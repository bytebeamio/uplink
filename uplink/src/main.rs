mod console;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use flume::bounded;
use log::info;
use structopt::StructOpt;
use tokio::time::sleep;
use tracing::error;
use tracing_subscriber::fmt::format::{Format, Pretty};
use tracing_subscriber::{fmt::Layer, layer::Layered, reload::Handle};
use tracing_subscriber::{EnvFilter, Registry};

pub type ReloadHandle =
    Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

use uplink::base::AppConfig;
use uplink::config::{get_configs, initialize, CommandLine};
use uplink::{simulator, spawn_named_thread, Config, TcpJson, Uplink};

fn initialize_logging(commandline: &CommandLine) -> ReloadHandle {
    let level = match commandline.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    let levels =
        match commandline.modules.clone().into_iter().reduce(|e, acc| format!("{e}={level},{acc}"))
        {
            Some(f) => format!("{f}={level}"),
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
    println!("    project_id: {}", config.project_id);
    println!("    device_id: {}", config.device_id);
    println!("    remote: {}:{}", config.broker, config.port);
    println!("    persistence_path: {}", config.persistence_path.display());
    if !config.action_redirections.is_empty() {
        println!("    action redirections:");
        for (action, redirection) in config.action_redirections.iter() {
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

    if !config.downloader.actions.is_empty() {
        println!(
            "    downloader:\n\tpath: \"{}\"\n\tactions: {:?}",
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
    let mut bridge = uplink.configure_bridge();
    uplink.spawn_builtins(&mut bridge)?;

    let bridge_tx = bridge.tx();

    let mut tcpapps = vec![];
    for (app, cfg) in config.tcpapps.clone() {
        let mut route_rx = None;
        if !cfg.actions.is_empty() {
            let (actions_tx, actions_rx) = bounded(1);
            bridge.register_action_routes(&cfg.actions, actions_tx)?;
            route_rx = Some(actions_rx)
        }
        tcpapps.push(TcpJson::new(app, cfg, route_rx, bridge.tx()));
    }

    let simulator_actions = config.simulator.as_ref().and_then(|cfg| {
        let mut route_rx = None;
        if !cfg.actions.is_empty() {
            let (actions_tx, actions_rx) = bounded(1);
            bridge.register_action_routes(&cfg.actions, actions_tx).unwrap();
            route_rx = Some(actions_rx)
        }

        route_rx
    });

    uplink.spawn(bridge)?;

    if let Some(config) = config.simulator.clone() {
        let bridge_tx = bridge_tx.clone();
        spawn_named_thread("Simulator", || {
            simulator::start(config, bridge_tx, simulator_actions).unwrap();
        });
    }

    if config.console.enabled {
        let port = config.console.port;
        let bridge_tx = bridge_tx.clone();
        spawn_named_thread("Uplink Console", move || {
            console::start(port, reload_handle, bridge_tx)
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
                    error!("App failed. Error = {:?}", e);
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
                    SIGTERM | SIGINT | SIGQUIT => bridge_tx.trigger_shutdown().await,
                    s => error!("Couldn't handle signal: {s}"),
                }
            }
        });

        uplink.resolve_on_shutdown().await.unwrap();
        info!("Uplink shutting down...");
        // NOTE: wait 5s to allow serializer to write to network/disk
        sleep(Duration::from_secs(5)).await;
    });

    Ok(())
}
