use std::fs::read_to_string;
use anyhow::Error;
use structopt::StructOpt;
use uplink::*;

fn main() -> Result<(), Error> {
    if std::env::args().any(|a| a == "--sha") {
        println!("{}", &env!("VERGEN_GIT_SHA")[0..8]);
        return Ok(());
    }

    let commandline: CommandLine = StructOpt::from_args();
    let device_json = read_to_string(commandline.auth.as_path())?;
    let config_toml = match commandline.config.as_ref() {
        None => { String::new() }
        Some(p) => {
            read_to_string(p)?
        }
    };

    let (_, end_rx) = entrypoint(device_json, config_toml, None as Option<Box<dyn Fn(Action) + Send + Sync>>, commandline.verbose, commandline.modules.clone(), true)?;
    if let Err(_) = end_rx.recv() {
        log::error!("uplink stopped without sending to end_tx");
    }

    Ok(())
}

