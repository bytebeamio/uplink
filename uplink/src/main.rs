//! uplink is a utility/library to interact with the Bytebeam platform. It's internal architecture is described in the diagram below.
//! We use [`rumqttc`], which implements the MQTT protocol, to communicate with the platform. Communication is handled separately as ingress and egress
//! by [`Mqtt`] and [`Serializer`] respectively. [`Action`]s are received and forwarded by Mqtt to the [`Actions`] module, where it is handled depending
//! on it's type and purpose, forwarding it to either the [`Bridge`](collector::tcpjson::Bridge), [`Process`](base::actions::process::Process),
//! [`Controller`](base::actions::controller::Controller), [`OtaDownloader`](base::actions::ota::OtaDownloader) or [`TunshellSession`](base::actions::tunshell::TunshellSession).
//! Bridge forwards received Actions to devices connected to it through the `bridge_port` and collects response data from these devices, to forward to the platform.
//!
//! Response data can be of multiple types, of interest to us are [`ActionResponse`](base::actions::response::ActionResponse)s, which are forwarded to Actions
//! and then to Serializer where depending on the network, it may be stored onto disk with [`Storage`](disk::Storage) to ensure packets aren't lost.
//!
//!```text
//!                                                                                 ┌────────────┐
//!                                                                                 │MQTT backend│
//!                                                                                 └─────┐▲─────┘
//!                                                                                       ││
//!                                                                                Action ││ ActionResponse
//!                                                                                       ││ / Data
//!                                                                           Action    ┌─▼└─┐
//!                                                                       ┌─────────────┤Mqtt◄───────────┐
//!                                                                       │             └────┘           │ ActionResponse
//!                                                                       │                              │ / Data
//!                                                                       │                              │
//!                                                                   ┌───▼───┐   ActionResponse    ┌────┴─────┐
//!                                                  ┌────────────────►Actions├─────────────────────►Serializer│
//!                                                  │                └┬─┬─┬─┬┘                     └────▲─────┘
//!                                                  │                 │ │ │ │                           │
//!                                                  │                 │ │ │ └───────────────────┐       │Data
//!                                                  │     Tunshell Key│ │ │ Action              │    ┌──┴───┐   Action       ┌───────────┐
//!                                                  │        ┌────────┘ │ └───────────┐         ├────►Bridge◄────────────────►Application│
//!                                                  │  ------│----------│-------------│-------- │    └──┬───┘ ActionResponse │ / Device  │
//!                                                  │  '     │          │             │       ' │       │       / Data       └───────────┘
//!                                                  │  '┌────▼───┐  ┌───▼───┐  ┌──────▼──────┐' │       │
//!                                                  │  '│Tunshell│  │Process│  │OtaDownloader├──┘       │
//!                                                  │  '└────┬───┘  └───┬───┘  └──────┬──────┘'         │
//!                                                  │  '     │          │             │       '         │
//!                                                  │  ------│----------│-------------│--------         │
//!                                                  │        │          │             │                 │
//!                                                  └────────┴──────────┴─────────────┴─────────────────┘
//!                                                                      ActionResponse
//!```

use std::fs;
use std::sync::Arc;

use log::error;
use simplelog::{ColorChoice, CombinedLogger, LevelFilter, LevelPadding, TermLogger, TerminalMode};
use structopt::StructOpt;
use tokio::task;

use uplink::config::{initialize, CommandLine};
use uplink::{Bridge, Config, Simulator, Uplink};

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
    if config.ota.enabled {
        println!("    ota_path: {}", config.ota.path);
    }
    if config.stats.enabled {
        println!("    processes: {:?}", config.stats.process_names);
    }
    println!("\n");
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), anyhow::Error> {
    let commandline: CommandLine = StructOpt::from_args();
    let enable_simulator = commandline.simulator;

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

    banner(&commandline, &config);

    let mut uplink = Uplink::new(config.clone())?;
    uplink.spawn()?;

    if enable_simulator {
        let mut simulator = Simulator::new(config.clone(), uplink.bridge_data_tx());
        task::spawn(async move {
            simulator.start().await;
        });
    }

    let mut bridge = Bridge::new(
        config,
        uplink.bridge_data_tx(),
        uplink.bridge_action_rx(),
        uplink.action_status(),
    );
    if let Err(e) = bridge.start().await {
        error!("Bridge stopped!! Error = {:?}", e);
    }

    Ok(())
}
