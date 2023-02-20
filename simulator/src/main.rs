use fake::{Dummy, Fake, Faker};
use futures_util::sink::SinkExt;
use futures_util::stream::SplitSink;
use futures_util::StreamExt;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, LevelPadding, TermLogger, TerminalMode,
};
use structopt::StructOpt;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::select;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use tokio_util::time::DelayQueue;
use uplink::Action;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, io, sync::Arc};

const RESET_LIMIT: u32 = 1500;

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
    #[error("Stream done")]
    StreamDone,
    #[error("Lines codec error {0}")]
    Codec(#[from] LinesCodecError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Location {
    latitude: f64,
    longitude: f64,
}

#[derive(Clone, PartialEq)]
pub struct DeviceData {
    path: Arc<Vec<Location>>,
    path_offset: u32,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DataEventType {
    GenerateGPS,
    GenerateIMU,
    GenerateVehicleData,
    GeneratePeripheralData,
    GenerateMotor,
    GenerateBMS,
}

impl DataEventType {
    fn duration(&self) -> Duration {
        match self {
            DataEventType::GenerateGPS => Duration::from_millis(1000),
            DataEventType::GenerateIMU => Duration::from_millis(100),
            DataEventType::GenerateVehicleData => Duration::from_millis(1000),
            DataEventType::GeneratePeripheralData => Duration::from_millis(1000),
            DataEventType::GenerateMotor => Duration::from_millis(250),
            DataEventType::GenerateBMS => Duration::from_millis(250),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct DataEvent {
    timestamp: Instant,
    event_type: DataEventType,
    device: DeviceData,
    sequence: u32,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ActionResponseEvent {
    timestamp: Instant,
    action_id: String,
    status: String,
    progress: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub stream: String,
    #[serde(skip)]
    pub device_id: Option<String>,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Clone, PartialEq)]
pub enum Event {
    DataEvent(DataEvent),
    ActionResponseEvent(ActionResponseEvent),
}

pub fn generate_gps_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let path_len = device.path.len() as u32;
    let path_index = ((device.path_offset + sequence) % path_len) as usize;
    let position = device.path.get(path_index).unwrap();

    Payload {
        timestamp,
        device_id: None,
        sequence,
        stream: "gps".to_string(),
        payload: json!(position),
    }
}

struct BoolString;

impl Dummy<BoolString> for String {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &BoolString, rng: &mut R) -> String {
        const NAMES: &[&str] = &["on", "off"];
        NAMES.choose(rng).unwrap().to_string()
    }
}

#[derive(Debug, Serialize, Dummy)]
struct Bms {
    #[dummy(faker = "250")]
    periodicity_ms: i32,
    #[dummy(faker = "40.0 .. 45.0")]
    mosfet_temperature: f64,
    #[dummy(faker = "35.0 .. 40.0")]
    ambient_temperature: f64,
    #[dummy(faker = "1")]
    mosfet_status: i32,
    #[dummy(faker = "16")]
    cell_voltage_count: i32,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_1: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_2: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_3: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_4: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_5: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_6: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_7: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_8: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_9: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_10: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_11: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_12: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_13: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_14: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_15: f64,
    #[dummy(faker = "3.0 .. 3.2")]
    cell_voltage_16: f64,
    #[dummy(faker = "8")]
    cell_thermistor_count: i32,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_1: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_2: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_3: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_4: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_5: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_6: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_7: f64,
    #[dummy(faker = "40.0 .. 43.0")]
    cell_temp_8: f64,
    #[dummy(faker = "1")]
    cell_balancing_status: i32,
    #[dummy(faker = "95.0 .. 96.0")]
    pack_voltage: f64,
    #[dummy(faker = "15.0 .. 20.0")]
    pack_current: f64,
    #[dummy(faker = "80.0 .. 90.0")]
    pack_soc: f64,
    #[dummy(faker = "9.5 .. 9.9")]
    pack_soh: f64,
    #[dummy(faker = "9.5 .. 9.9")]
    pack_sop: f64,
    #[dummy(faker = "100 .. 150")]
    pack_cycle_count: i64,
    #[dummy(faker = "2000 .. 3000")]
    pack_available_energy: i64,
    #[dummy(faker = "2000 .. 3000")]
    pack_consumed_energy: i64,
    #[dummy(faker = "0")]
    pack_fault: i32,
    #[dummy(faker = "1")]
    pack_status: i32,
}

pub fn generate_bms_data(sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload: Bms = Faker.fake();

    Payload {
        timestamp,
        device_id: None,
        sequence,
        stream: "bms".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize, Dummy)]
struct Imu {
    #[dummy(faker = "1.0 .. 2.8")]
    ax: f64,
    #[dummy(faker = "1.0 .. 2.8")]
    ay: f64,
    #[dummy(faker = "9.79 .. 9.82")]
    az: f64,
    #[dummy(faker = "0.8 .. 1.0")]
    pitch: f64,
    #[dummy(faker = "0.8 .. 1.0")]
    roll: f64,
    #[dummy(faker = "0.8 .. 1.0")]
    yaw: f64,
    #[dummy(faker = "-45.0 .. -15.0")]
    magx: f64,
    #[dummy(faker = "-45.0 .. -15.0")]
    magy: f64,
    #[dummy(faker = "-45.0 .. -15.0")]
    magz: f64,
}

pub fn generate_imu_data(sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload: Imu = Faker.fake();

    Payload {
        timestamp,
        device_id: None,
        sequence,
        stream: "imu".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize, Dummy)]
struct Motor {
    #[dummy(faker = "40.0 .. 45.0")]
    motor_temperature1: f64,
    #[dummy(faker = "40.0 .. 45.0")]
    motor_temperature2: f64,
    #[dummy(faker = "40.0 .. 45.0")]
    motor_temperature3: f64,
    #[dummy(faker = "95.0 .. 96.0")]
    motor_voltage: f64,
    #[dummy(faker = "20.0 .. 25.0")]
    motor_current: f64,
    #[dummy(faker = "1000 .. 9000")]
    motor_rpm: i64,
}

pub fn generate_motor_data(sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload: Motor = Faker.fake();


    Payload {
        timestamp,
        device_id: None,
        sequence,
        stream: "motor".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize, Dummy)]
struct PeripheralState {
    #[dummy(faker = "BoolString")]
    gps_status: String,
    #[dummy(faker = "BoolString")]
    gsm_status: String,
    #[dummy(faker = "BoolString")]
    imu_status: String,
    #[dummy(faker = "BoolString")]
    left_indicator: String,
    #[dummy(faker = "BoolString")]
    right_indicator: String,
    #[dummy(faker = "BoolString")]
    headlamp: String,
    #[dummy(faker = "BoolString")]
    horn: String,
    #[dummy(faker = "BoolString")]
    left_brake: String,
    #[dummy(faker = "BoolString")]
    right_brake: String,
}

pub fn generate_peripheral_state_data(sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload: PeripheralState = Faker.fake();

    Payload {
        timestamp,
        device_id: None,
        sequence,
        stream: "peripheral_state".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize, Dummy)]
struct DeviceShadow {
    #[dummy(faker = "BoolString")]
    mode: String,
    #[dummy(faker = "BoolString")]
    status: String,
    #[dummy(faker = "BoolString")]
    firmware_version: String,
    #[dummy(faker = "BoolString")]
    config_version: String,
    #[dummy(faker = "20000..30000")]
    distance_travelled: i64,
    #[dummy(faker = "50000..60000")]
    range: i64,
    #[serde(rename(serialize = "SOC"))]
    #[dummy(faker = "50.0..90.0")]
    soc: f64,
}

pub fn generate_device_shadow_data(sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload: DeviceShadow = Faker.fake();

    Payload {
        timestamp,
        device_id: None,
        sequence,
        stream: "device_shadow".to_string(),
        payload: json!(payload),
    }
}

pub fn read_gps_path(paths_dir: &str) -> Arc<Vec<Location>> {
    let i = rand::thread_rng().gen_range(0..10);
    let file_name = format!("{}/path{}.json", paths_dir, i);

    let contents = fs::read_to_string(file_name).expect("Oops, failed ot read path");

    let parsed: Vec<Location> = serde_json::from_str(&contents).unwrap();

    Arc::new(parsed)
}

pub fn new_device_data(path: Arc<Vec<Location>>) -> DeviceData {
    let mut rng = rand::thread_rng();

    let path_index = rng.gen_range(0..path.len()) as u32;

    DeviceData { path, path_offset: path_index }
}

pub fn generate_initial_events(events: &mut DelayQueue<Event>, now: Instant, device: &DeviceData) {
    let duration = DataEventType::GenerateGPS.duration();
    events.insert(
        Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateGPS,
            device: device.clone(),
            timestamp: now + duration,
            sequence: 1,
        }),
        duration,
    );

    let duration = DataEventType::GenerateVehicleData.duration();
    events.insert(
        Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateVehicleData,
            device: device.clone(),
            timestamp: now + duration,
            sequence: 1,
        }),
        duration,
    );

    let duration = DataEventType::GeneratePeripheralData.duration();
    events.insert(
        Event::DataEvent(DataEvent {
            event_type: DataEventType::GeneratePeripheralData,
            device: device.clone(),
            timestamp: now + duration,
            sequence: 1,
        }),
        duration,
    );

    let duration = DataEventType::GenerateMotor.duration();
    events.insert(
        Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateMotor,
            device: device.clone(),
            timestamp: now + duration,
            sequence: 1,
        }),
        duration,
    );

    let duration = DataEventType::GenerateBMS.duration();
    events.insert(
        Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateBMS,
            device: device.clone(),
            timestamp: now + duration,
            sequence: 1,
        }),
        duration,
    );

    let duration = DataEventType::GenerateIMU.duration();
    events.insert(
        Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateIMU,
            device: device.clone(),
            timestamp: now + duration,
            sequence: 1,
        }),
        duration,
    );
}

pub async fn process_data_event(
    event: &DataEvent,
    events: &mut DelayQueue<Event>,
    client: &mut SplitSink<Framed<TcpStream, LinesCodec>, String>,
) -> Result<(), Error> {
    let data = match event.event_type {
        DataEventType::GenerateGPS => generate_gps_data(&event.device, event.sequence),
        DataEventType::GenerateIMU => generate_imu_data(event.sequence),
        DataEventType::GenerateVehicleData => generate_device_shadow_data(event.sequence),
        DataEventType::GeneratePeripheralData => generate_peripheral_state_data(event.sequence),
        DataEventType::GenerateMotor => generate_motor_data(event.sequence),
        DataEventType::GenerateBMS => generate_bms_data(event.sequence),
    };

    let payload = serde_json::to_string(&data)?;
    client.send(payload).await?;

    let duration = event.event_type.duration();
    let sequence = if event.sequence >= RESET_LIMIT { 0 } else { event.sequence + 1 };

    events.insert(
        Event::DataEvent(DataEvent {
            sequence,
            timestamp: event.timestamp + duration,
            device: event.device.clone(),
            event_type: event.event_type,
        }),
        duration,
    );

    Ok(())
}

async fn process_action_response_event(
    event: &ActionResponseEvent,
    client: &mut SplitSink<Framed<TcpStream, LinesCodec>, String>,
) -> Result<(), Error> {
    //info!("Sending action response {:?} {} {} {}", event.action_id, event.progress, event.status);
    let payload = Payload {
        stream: "action_status".to_string(),
        device_id: None,
        sequence: 0,
        timestamp: SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
        payload: json!({
            "action_id": event.action_id,
            "state": event.status,
            "progress": event.progress,
            "errors": []
        }),
    };

    info!("Sending action response {} {} {}", event.action_id, event.progress, event.status);

    let payload = serde_json::to_string(&payload)?;
    client.send(payload).await?;
    info!("Successfully sent action response");

    Ok(())
}

pub async fn process_events(
    events: &mut DelayQueue<Event>,
    client: &mut SplitSink<Framed<TcpStream, LinesCodec>, String>,
) -> Result<(), Error> {
    if let Some(e) = events.next().await {
        match e.into_inner() {
            Event::DataEvent(event) => {
                process_data_event(&event, events, client).await?;
            }

            Event::ActionResponseEvent(event) => {
                process_action_response_event(&event, client).await?;
            }
        }
    }

    Ok(())
}

pub fn generate_action_events(action: Action, events: &mut DelayQueue<Event>) {
    let action_id = action.action_id;

    info!("Generating action events for action: {action_id}");
    let now = Instant::now() + Duration::from_millis(rand::thread_rng().gen_range(0..5000));

    // Action response, 10% completion per second
    for i in 1..10 {
        let duration = Duration::from_secs(i as u64);
        events.insert(
            Event::ActionResponseEvent(ActionResponseEvent {
                action_id: action_id.clone(),
                progress: i * 10,
                status: String::from("in_progress"),
                timestamp: now + duration,
            }),
            duration,
        );
    }

    let duration = Duration::from_secs(10);
    events.insert(
        Event::ActionResponseEvent(ActionResponseEvent {
            action_id: action_id.clone(),
            progress: 100,
            status: String::from("Completed"),
            timestamp: now + duration,
        }),
        duration,
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let commandline = init();

    let addr = format!("localhost:{}", commandline.port);
    let path = read_gps_path(&commandline.paths);
    let device = new_device_data(path);

    let stream = TcpStream::connect(addr).await?;
    let (mut data_tx, mut actions_rx) = Framed::new(stream, LinesCodec::new()).split();

    let mut events = DelayQueue::new();

    generate_initial_events(&mut events, Instant::now(), &device);

    loop {
        select! {
            line = actions_rx.next() => {
                let line = line.ok_or(Error::StreamDone)??;
                let action = serde_json::from_str(&line)?;
                generate_action_events(action, &mut events);
            }
            _ = process_events(&mut events, &mut data_tx) => {
            }
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
