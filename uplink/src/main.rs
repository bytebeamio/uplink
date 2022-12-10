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

use std::fs;
use std::sync::Arc;

use anyhow::Error;
use log::error;
use simplelog::{ColorChoice, CombinedLogger, LevelFilter, LevelPadding, TermLogger, TerminalMode};
use structopt::StructOpt;

use uplink::config::{initialize, CommandLine};
use uplink::{simulator, Bridge, Config, Uplink};

fn initialize_logging(commandline: &CommandLine) {
    let level = match commandline.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    let mut config = simplelog::ConfigBuilder::new();
    config
        .set_location_level(LevelFilter::Off)
        .set_target_level(LevelFilter::Error)
        .set_thread_level(LevelFilter::Error)
        .set_level_padding(LevelPadding::Right);

    if commandline.modules.is_empty() {
        config.add_filter_allow_str("uplink").add_filter_allow_str("disk");
    } else {
        for module in commandline.modules.iter() {
            config.add_filter_allow(module.to_string());
        }
    }

    let loggers = TermLogger::new(level, config.build(), TerminalMode::Mixed, ColorChoice::Auto);
    CombinedLogger::init(vec![loggers]).unwrap();
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
    println!("    secure_transport: {}", config.authentication.is_some());
    println!("    max_packet_size: {}", config.max_packet_size);
    println!("    max_inflight_messages: {}", config.max_inflight);
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
    let commandline: CommandLine = StructOpt::from_args();

    initialize_logging(&commandline);
    let config = Arc::new(initialize(
        fs::read_to_string(&commandline.auth)?.as_str(),
        commandline
            .config
            .as_ref()
            .and_then(|path| fs::read_to_string(path).ok())
            .unwrap_or_default()
            .as_str(),
    )?);

    let _log_guards = config.log_dir.as_ref().map(|log_dir| {
        std::fs::create_dir_all(log_dir).unwrap();
        let out_path = format!("{}/{}", log_dir, "stdout.log");
        let err_path = format!("{}/{}", log_dir, "stderr.log");
        let _ = std::fs::remove_file(out_path.as_str());
        let _ = std::fs::remove_file(err_path.as_str());
        let _out_guard = stdio_override::StdoutOverride::override_file(out_path).unwrap();
        let _err_guard = stdio_override::StderrOverride::override_file(err_path).unwrap();
        (_out_guard, _err_guard)
    });
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
