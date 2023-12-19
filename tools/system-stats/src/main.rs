use log::LevelFilter;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelPadding, TermLogger, TerminalMode,
};
use structopt::StructOpt;
use system_stats::{Config, StatCollector};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[derive(StructOpt, Debug)]
#[structopt(name = "simulator", about = "simulates a demo device")]
pub struct CommandLine {
    /// uplink port
    #[structopt(short = "p", help = "uplink port")]
    pub port: u16,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
    /// name of processes to be monitored
    #[structopt(short = "P", help = "processes")]
    pub process_names: Vec<String>,
    /// time between updates
    #[structopt(short = "t", help = "update period", default_value = "60")]
    pub update_period: u64,
}

#[tokio::main]
async fn main() {
    let CommandLine { process_names, update_period, port, .. } = init();

    let addr = format!("localhost:{}", port);

    let stream = TcpStream::connect(addr).await.unwrap();
    let client = Framed::new(stream, LinesCodec::new());
    let config = Config { process_names, update_period };

    let collector = StatCollector::new(config, client);
    collector.start().await;
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
