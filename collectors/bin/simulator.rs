use std::{path::PathBuf, time::Duration};

use collectors::{simulator, ActionsRx, DataTx};
use futures_util::StreamExt;
use log::LevelFilter;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelPadding, TermLogger, TerminalMode,
};
use structopt::StructOpt;
use tokio::{net::TcpStream, time::sleep};
use tokio_util::codec::{Framed, LinesCodec};

#[derive(StructOpt, Debug)]
#[structopt(name = "simulator", about = "simulates a demo device")]
pub struct CommandLine {
    /// uplink port
    #[structopt(short = "p", help = "uplink port")]
    pub port: u16,
    /// path of GPS coordinates
    #[structopt(short = "g", help = "gps path file directory")]
    pub paths: PathBuf,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
}

#[tokio::main]
async fn main() {
    let commandline = init();
    let addr = format!("localhost:{}", commandline.port);
    loop {
        let Ok(stream) = TcpStream::connect(&addr).await else {
            sleep(Duration::from_secs(1)).await;
            continue;
        };
        let (data_tx, actions_rx) = Framed::new(stream, LinesCodec::new()).split();
        let bridge_tx = DataTx { data_tx };
        let actions_rx = Some(ActionsRx { actions_rx });
        if let Ok(_) = simulator::start(commandline.paths.clone(), bridge_tx, actions_rx).await {
            break;
        }
    }
}

fn init() -> CommandLine {
    let commandline: CommandLine = StructOpt::from_args();
    let level = match commandline.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    let mut config = ConfigBuilder::new();
    config
        .set_location_level(LevelFilter::Off)
        .set_target_level(LevelFilter::Error)
        .set_thread_level(LevelFilter::Error)
        .set_level_padding(LevelPadding::Right);

    let loggers = TermLogger::new(level, config.build(), TerminalMode::Mixed, ColorChoice::Auto);
    CombinedLogger::init(vec![loggers]).unwrap();

    commandline
}
