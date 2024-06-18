use data::{Bms, DeviceData, DeviceShadow, Gps, Imu, Motor, PeripheralState};
use flume::{bounded, Sender};
use log::{error, info, LevelFilter};
use rand::Rng;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Publish};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelPadding, TermLogger, TerminalMode,
};
use structopt::StructOpt;
use thiserror::Error;
use tokio::spawn;
use tokio::time::{interval, sleep};
use uplink::Action;

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, io, sync::Arc};

mod data;

#[derive(StructOpt, Debug)]
#[structopt(name = "simulator", about = "simulates a demo device")]
pub struct CommandLine {
    /// uplink port
    #[structopt(short = "p", help = "uplink port")]
    pub port: u16,
    /// path of GPS coordinates
    #[structopt(short = "g", help = "gps path file directory")]
    pub paths: String,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Mqtt error {0}")]
    Mqtt(#[from] rumqttc::ClientError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

#[derive(Serialize)]
pub struct ActionResponse {
    action_id: String,
    state: String,
    progress: u8,
    errors: Vec<String>,
}

impl ActionResponse {
    pub async fn simulate(action: Action, tx: Sender<Payload>) {
        let action_id = action.action_id;
        info!("Generating action events for action: {action_id}");
        let mut sequence = 0;
        let mut interval = interval(Duration::from_secs(1));

        // Action response, 10% completion per second
        for i in 1..10 {
            let response = ActionResponse {
                action_id: action_id.clone(),
                progress: i * 10 + rand::thread_rng().gen_range(0..10),
                state: String::from("in_progress"),
                errors: vec![],
            };
            sequence += 1;
            if let Err(e) = tx
                .send_async(Payload::new("action_status".to_string(), sequence, json!(response)))
                .await
            {
                error!("{e}");
                break;
            }

            interval.tick().await;
        }

        let response = ActionResponse {
            action_id,
            progress: 100,
            state: String::from("Completed"),
            errors: vec![],
        };
        sequence += 1;
        if let Err(e) = tx
            .send_async(Payload::new("action_status".to_string(), sequence, json!(response)))
            .await
        {
            error!("{e}");
        }
        info!("Successfully sent all action responses");
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

impl Payload {
    fn new(stream: String, sequence: u32, payload: Value) -> Self {
        Self {
            stream,
            sequence,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            payload,
        }
    }
}

pub fn read_gps_path(paths_dir: &str) -> Arc<Vec<Gps>> {
    let i = rand::thread_rng().gen_range(0..10);
    let file_name: String = format!("{paths_dir}/path{i}.json");

    let contents = fs::read_to_string(file_name).expect("Oops, failed ot read path");

    let parsed: Vec<Gps> = serde_json::from_str(&contents).unwrap();

    Arc::new(parsed)
}

pub fn new_device_data(path: Arc<Vec<Gps>>) -> DeviceData {
    let mut rng = rand::thread_rng();

    let path_index = rng.gen_range(0..path.len()) as u32;

    DeviceData { path, path_offset: path_index }
}

pub fn spawn_data_simulators(device: DeviceData, tx: Sender<Payload>) {
    spawn(Gps::simulate(tx.clone(), device));
    spawn(Bms::simulate(tx.clone()));
    spawn(Imu::simulate(tx.clone()));
    spawn(Motor::simulate(tx.clone()));
    spawn(PeripheralState::simulate(tx.clone()));
    spawn(DeviceShadow::simulate(tx.clone()));
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let commandline = init();
    let path = read_gps_path(&commandline.paths);
    let device = new_device_data(path);

    let (tx, rx) = bounded(10);
    spawn_data_simulators(device, tx.clone());

    let options = MqttOptions::new("simulator", "127.0.0.1", commandline.port);
    let (client, mut eventloop) = AsyncClient::new(options, 1);

    spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(Publish { payload, .. }))) => {
                    let Ok(action) = serde_json::from_slice(&payload) else {
                        error!("Failed to serialize");
                        continue;
                    };
                    spawn(ActionResponse::simulate(action, tx.clone()));
                }
                Err(e) => {
                    error!("{e}");
                    sleep(Duration::from_secs(1)).await;
                }
                _ => {}
            }
        }
    });
    loop {
        let Ok(payload) = rx.recv_async().await else {
            error!("All generators have stopped!");
            return Ok(());
        };

        let topic = format!("streams/{}", payload.stream);
        let payload = serde_json::to_vec(&payload)?;
        client.publish(topic, rumqttc::QoS::AtLeastOnce, false, payload).await?;
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
