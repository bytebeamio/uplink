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

use anyhow::Error;
use log::error;
use structopt::StructOpt;

use uplink::config::{get_configs, initialize, CommandLine};
use uplink::{simulator, Bridge, Config, Uplink};

fn initialize_logging(commandline: &CommandLine) {
    let level = match commandline.verbose {
        0 => "uplink=warn",
        1 => "uplink=info",
        2 => "uplink=debug",
        _ => "uplink=trace",
    };
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(level)
        .with_filter_reloading();

    let _reload_handle = builder.reload_handle();
    builder.try_init().expect("Couldn't successfully initialize tracing subscriber");
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
    println!("    bridge_port: {}", config.bridge_port);
    println!("    secure_transport: {}", config.authentication.is_some());
    println!("    max_packet_size: {}", config.max_packet_size);
    println!("    max_inflight_messages: {}", config.max_inflight);
    println!("    keep_alive_timeout: {}", config.keep_alive);
    if let Some(persistence) = &config.persistence {
        println!("    persistence_dir: {}", persistence.path);
        println!("    persistence_max_segment_size: {}", persistence.max_file_size);
        println!("    persistence_max_segment_count: {}", persistence.max_file_count);
    }
    if let Some(downloader_cfg) = &config.downloader {
        println!("    download_path: {}", downloader_cfg.path);
    }
    if config.stats.enabled {
        println!("    processes: {:?}", config.stats.process_names);
    }
    println!("\n");
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    if std::env::args().any(|a| a == "--sha") {
        println!("{}", &env!("VERGEN_GIT_SHA")[0..8]);
        return Ok(());
    }

    let commandline: CommandLine = StructOpt::from_args();

    initialize_logging(&commandline);
    let (auth, config) = get_configs(&commandline)?;
    let config = Arc::new(initialize(&auth, &config.unwrap_or_default())?);

    banner(&commandline, &config);

    let mut uplink = Uplink::new(config.clone())?;
    uplink.spawn()?;

    if let Some(simulator_config) = &config.simulator {
        if let Err(e) =
            simulator::start(uplink.bridge_data_tx(), uplink.bridge_action_rx(), simulator_config)
                .await
        {
            error!("Error while running simulator: {}", e)
        }
    } else if let Err(e) = Bridge::new(
        config,
        uplink.bridge_data_tx(),
        uplink.bridge_action_rx(),
        uplink.action_status(),
    )
    .start()
    .await
    {
        error!("Bridge stopped!! Error = {:?}", e);
    }

    Ok(())
}
