use std::fs::read_to_string;
use std::path::PathBuf;
use anyhow::{Context, Error};
use structopt::StructOpt;
use uplink::*;

fn main() -> Result<(), Error> {
    if std::env::args().any(|a| a == "--sha") {
        println!("{}", &env!("VERGEN_GIT_SHA")[0..8]);
        return Ok(());
    }

    let commandline: CommandLine = StructOpt::from_args();
    let device_json = read_to_string(commandline.auth.as_path())
        .context("couldn't read auth json file")?;
    let config_toml = match commandline.config.as_ref() {
        None => { String::new() }
        Some(p) => {
            read_to_string(p)
                .context("couldn't read config toml file")?
        }
    };

    initialize_logging(commandline.verbose, commandline.modules.clone());
    let controller = entrypoint(device_json, config_toml, None as Option<ActionCallback>)?;
    if let Err(_) = controller.end_rx.recv() {
        log::error!("uplink stopped without sending to end_tx");
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
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

pub fn initialize_logging(log_level: u8, log_modules: Vec<String>) {
    let level = match log_level {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    let levels =
        match log_modules.into_iter().reduce(|e, acc| format!("{e}={level},{acc}")) {
            Some(f) => format!("{f}={level}"),
            _ => format!("{level}"),
        };

    let builder = tracing_subscriber::fmt()
        .pretty()
        .compact()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(levels)
        .with_filter_reloading();

    builder.try_init().expect("initialized subscriber succesfully");
}
