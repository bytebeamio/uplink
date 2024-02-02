use std::path::PathBuf;

use base::{Action, ActionResponse, CollectorRx, CollectorTx, Payload};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use log::LevelFilter;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelPadding, TermLogger, TerminalMode,
};
use simulator::Error;
use structopt::StructOpt;
use tokio::net::TcpStream;
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

struct DataTx {
    data_tx: SplitSink<Framed<TcpStream, LinesCodec>, String>,
}

#[async_trait::async_trait]
impl CollectorTx for DataTx {
    async fn send_action_response(&mut self, response: ActionResponse) {
        self.data_tx.send(serde_json::to_string(&response).unwrap()).await.unwrap()
    }

    async fn send_payload(&mut self, payload: Payload) {
        self.data_tx.send(serde_json::to_string(&payload).unwrap()).await.unwrap()
    }

    fn send_payload_sync(&mut self, _: Payload) {
        unimplemented!();
    }
}

struct ActionsRx {
    actions_rx: SplitStream<Framed<TcpStream, LinesCodec>>,
}

#[async_trait::async_trait]
impl CollectorRx for ActionsRx {
    async fn recv_action(&mut self) -> Option<Action> {
        let line = self.actions_rx.next().await.unwrap().unwrap();
        serde_json::from_str(&line).unwrap()
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let commandline = init();

    let addr = format!("localhost:{}", commandline.port);
    let stream = TcpStream::connect(addr).await?;
    let (data_tx, actions_rx) = Framed::new(stream, LinesCodec::new()).split();
    let bridge_tx = DataTx { data_tx };
    let actions_rx = Some(ActionsRx { actions_rx });
    simulator::start(commandline.paths, bridge_tx, actions_rx).await
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
