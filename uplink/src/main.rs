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

use std::sync::Arc;
use std::thread;

use anyhow::Error;

use structopt::StructOpt;
use tokio::task::JoinSet;

use uplink::base::AppConfig;
use uplink::config::{get_configs, initialize, CommandLine, ReloadHandle};
use uplink::{simulator, Config, TcpJson, Uplink};

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

    println!("{}", B);
    println!("    version: {}", commandline.version);
    println!("    profile: {}", commandline.profile);
    println!("    commit_sha: {}", commandline.commit_sha);
    println!("    commit_date: {}", commandline.commit_date);
    println!("    project_id: {}", config.project_id);
    println!("    device_id: {}", config.device_id);
    println!("    remote: {}:{}", config.broker, config.port);
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
    println!("    max_packet_size: {}", config.max_packet_size);
    println!("    max_inflight_messages: {}", config.max_inflight);
    println!("    keep_alive_timeout: {}", config.keep_alive);
    if let Some(persistence) = &config.persistence {
        println!("    persistence_dir: {}", persistence.path);
        println!("    persistence_max_segment_size: {}", persistence.max_file_size);
        println!("    persistence_max_segment_count: {}", persistence.max_file_count);
    }
    println!("    download_path: {}", config.downloader.path);
    if config.stats.enabled {
        println!("    processes: {:?}", config.stats.process_names);
    }
    if config.tracing.enabled {
        println!("    tracing: http://localhost:{}", config.tracing.port);
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

    let mut uplink = Uplink::new(config.clone(), reload_handle)?;
    let bridge = uplink.spawn()?;

    if let Some(config) = config.simulator.clone() {
        let bridge = bridge.clone();
        thread::spawn(move || {
            simulator::start(bridge, &config).unwrap();
        });
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
