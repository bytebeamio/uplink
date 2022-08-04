use flume::{Receiver, Sender};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::select;
use tokio_util::codec::LinesCodecError;

use std::collections::{BinaryHeap, HashMap};
use std::io;

use crate::base::{actions::Action, Buffer, Package, Stream};
use crate::Point;
use std::{
    cmp::Ordering,
    fs,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rand::Rng;

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
    #[error("flume error {0}")]
    Recv(#[from] flume::RecvError),
}

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is inturn a json
// TODO which cloud will doubel deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    #[serde(skip_serializing)]
    stream: String,
    timestamp: u64,
    sequence: u64,
    #[serde(flatten)]
    payload: Value,
}

impl Point for Payload {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }

    fn sequence(&self) -> u32 {
        self.sequence as u32
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Location {
    latitude: f64,
    longitude: f64,
}
#[derive(Clone)]
pub struct DeviceData {
    device_id: u32,
    path: Arc<Vec<Location>>,
    path_offset: u32,
    time_offset: Duration,
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

#[derive(Clone)]
pub struct DataEvent {
    timestamp: Instant,
    event_type: DataEventType,
    device: DeviceData,
    sequence: u64,
}

#[derive(Clone, PartialEq)]
pub struct ActionResponseEvent {
    timestamp: Instant,
    action_id: String,
    device_id: String,
    status: String,
    progress: u16,
}

#[derive(Clone)]
pub enum Event {
    DataEvent(DataEvent),
    ActionResponseEvent(ActionResponseEvent),
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Event::DataEvent(e1) => match other {
                Event::DataEvent(e2) => {
                    e1.timestamp == e2.timestamp
                        && e1.event_type == e2.event_type
                        && e1.device.device_id == e2.device.device_id
                }
                Event::ActionResponseEvent(_) => false,
            },
            Event::ActionResponseEvent(e1) => match other {
                Event::ActionResponseEvent(e2) => e1 == e2,
                Event::DataEvent(_) => false,
            },
        }
    }
}

impl Eq for Event {}

fn event_timestamp(event: &Event) -> Instant {
    match event {
        Event::DataEvent(e) => e.timestamp,
        Event::ActionResponseEvent(e) => e.timestamp,
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Event) -> Ordering {
        let t1 = event_timestamp(self);
        let t2 = event_timestamp(other);

        t1.cmp(&t2)
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Event) -> Option<Ordering> {
        Some(other.cmp(&self))
    }
}

pub struct Partitions {
    map: HashMap<String, Stream<Payload>>,
    tx: Sender<Box<dyn Package>>,
}

pub fn generate_gps_data(device: &DeviceData, sequence: u64) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    let path_len = device.path.len() as u32;
    let path_index = ((device.path_offset as u64 + sequence) % (path_len as u64)) as usize;
    let position = device.path.get(path_index).unwrap();

    payload.insert("latitude".to_owned(), json!(position.longitude));
    payload.insert("longitude".to_owned(), json!(position.latitude));

    return Payload {
        timestamp,
        sequence,
        stream: format!("/tenants/demo/devices/{}/events/gps/jsonarray", device.device_id),
        payload: Value::Object(payload),
    };
}

pub fn generate_float(start: f64, end: f64) -> Value {
    let mut rng = rand::thread_rng();

    json!(rng.gen_range(start..end))
}

pub fn generate_int(start: i32, end: i32) -> Value {
    json!(rand::thread_rng().gen_range(start..end) as i64)
}

pub fn generate_bool_string(p: f64) -> Value {
    if rand::thread_rng().gen_bool(p) {
        return Value::String("on".to_owned());
    } else {
        return Value::String("off".to_owned());
    }
}

pub fn generate_bms_data(device: &DeviceData, sequence: u64) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    payload.insert("periodicity_ms".to_owned(), json!(250));
    payload.insert("mosfet_temperature".to_owned(), generate_float(40f64, 45f64));
    payload.insert("ambient_temperature".to_owned(), generate_float(35f64, 40f64));
    payload.insert("mosfet_status".to_owned(), json!(1));
    payload.insert("cell_voltage_count".to_owned(), json!(16));
    payload.insert("cell_voltage_1".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_2".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_3".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_4".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_5".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_6".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_7".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_8".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_9".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_10".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_11".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_12".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_13".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_14".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_15".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_voltage_16".to_owned(), generate_float(3.0f64, 3.2f64));
    payload.insert("cell_thermistor_count".to_owned(), json!(8));
    payload.insert("cell_temp_1".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_2".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_3".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_4".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_5".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_6".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_7".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_temp_8".to_owned(), generate_float(40.0f64, 43.0f64));
    payload.insert("cell_balancing_status".to_owned(), json!(1));
    payload.insert("pack_voltage".to_owned(), generate_float(95f64, 96f64));
    payload.insert("pack_current".to_owned(), generate_float(15f64, 20f64));
    payload.insert("pack_soc".to_owned(), generate_float(80f64, 90f64));
    payload.insert("pack_soh".to_owned(), generate_float(9.5f64, 9.9f64));
    payload.insert("pack_sop".to_owned(), generate_float(9.5f64, 9.9f64));
    payload.insert("pack_cycle_count".to_owned(), generate_int(100, 150));
    payload.insert("pack_available_energy".to_owned(), generate_int(2000, 3000));
    payload.insert("pack_consumed_energy".to_owned(), generate_int(2000, 3000));
    payload.insert("pack_fault".to_owned(), json!(0));
    payload.insert("pack_status".to_owned(), json!(1));

    return Payload {
        timestamp,
        sequence,
        stream: format!("/tenants/demo/devices/{}/events/bms/jsonarray", device.device_id),
        payload: json!(payload),
    };
}

pub fn generate_imu_data(device: &DeviceData, sequence: u64) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    payload.insert("ax".to_owned(), generate_float(1f64, 2.8f64));
    payload.insert("ay".to_owned(), generate_float(1f64, 2.8f64));
    payload.insert("az".to_owned(), generate_float(9.79f64, 9.82f64));

    payload.insert("pitch".to_owned(), generate_float(0.8f64, 1f64));
    payload.insert("roll".to_owned(), generate_float(0.8f64, 1f64));
    payload.insert("yaw".to_owned(), generate_float(0.8f64, 1f64));

    payload.insert("magx".to_owned(), generate_float(-45f64, -15f64));
    payload.insert("magy".to_owned(), generate_float(-45f64, -15f64));
    payload.insert("magz".to_owned(), generate_float(-45f64, -15f64));

    return Payload {
        timestamp,
        sequence,
        stream: format!("/tenants/demo/devices/{}/events/imu/jsonarray", device.device_id),
        payload: json!(payload),
    };
}

pub fn generate_motor_data(device: &DeviceData, sequence: u64) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    payload.insert("motor_temperature1".to_owned(), generate_float(40f64, 45f64));
    payload.insert("motor_temperature2".to_owned(), generate_float(40f64, 45f64));
    payload.insert("motor_temperature3".to_owned(), generate_float(40f64, 45f64));

    payload.insert("motor_voltage".to_owned(), generate_float(95f64, 96f64));
    payload.insert("motor_current".to_owned(), generate_float(20f64, 25f64));
    payload.insert("motor_rpm".to_owned(), generate_int(1000, 9000));

    return Payload {
        timestamp,
        sequence,
        stream: format!("/tenants/demo/devices/{}/events/motor/jsonarray", device.device_id),
        payload: json!(payload),
    };
}

pub fn generate_peripheral_state_data(device: &DeviceData, sequence: u64) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    payload.insert("gps_status".to_owned(), generate_bool_string(0.99));
    payload.insert("gsm_status".to_owned(), generate_bool_string(0.99));
    payload.insert("imu_status".to_owned(), generate_bool_string(0.99));
    payload.insert("left_indicator".to_owned(), generate_bool_string(0.1));
    payload.insert("right_indicator".to_owned(), generate_bool_string(0.1));
    payload.insert("headlamp".to_owned(), generate_bool_string(0.9));
    payload.insert("horn".to_owned(), generate_bool_string(0.05));
    payload.insert("left_brake".to_owned(), generate_bool_string(0.1));
    payload.insert("right_brake".to_owned(), generate_bool_string(0.1));

    return Payload {
        timestamp,
        sequence,
        stream: format!(
            "/tenants/demo/devices/{}/events/peripheral_state/jsonarray",
            device.device_id
        ),
        payload: json!(payload),
    };
}

pub fn generate_device_shadow_data(device: &DeviceData, sequence: u64) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    payload.insert("mode".to_owned(), Value::String("economy".to_owned()));
    payload.insert("status".to_owned(), Value::String("Locked".to_owned()));
    payload.insert("firmware_version".to_owned(), Value::String("1.33-Aug-2020b1".to_owned()));
    payload.insert("config_version".to_owned(), Value::String("1.23".to_owned()));
    payload.insert("distance_travelled".to_owned(), generate_int(20000, 30000));
    payload.insert("range".to_owned(), generate_int(50000, 60000));
    payload.insert("SOC".to_owned(), generate_float(50f64, 90f64));

    return Payload {
        timestamp,
        sequence,
        stream: format!(
            "/tenants/demo/devices/{}/events/device_shadow/jsonarray",
            device.device_id
        ),
        payload: json!(payload),
    };
}

pub fn read_gps_paths(paths_dir: String) -> Vec<Arc<Vec<Location>>> {
    (0..10)
        .map(|i| {
            let file_name = format!("{}/path{}.json", paths_dir, i);

            let contents = fs::read_to_string(file_name).expect("Oops, failed ot read path");

            let parsed: Vec<Location> = serde_json::from_str(&contents).unwrap();

            Arc::new(parsed)
        })
        .collect::<Vec<_>>()
}

pub fn new_device_data(device_id: u32, paths: &Vec<Arc<Vec<Location>>>) -> DeviceData {
    let mut rng = rand::thread_rng();

    let n = rng.gen_range(0..10);
    let path = paths.get(n).unwrap().clone();
    let path_index = rng.gen_range(0..path.len()) as u32;

    DeviceData {
        device_id: device_id,
        path: path,
        path_offset: path_index,
        time_offset: Duration::from_millis(rng.gen_range(0..10000)),
    }
}

pub fn generate_initial_events(
    events: &mut BinaryHeap<Event>,
    timestamp: Instant,
    devices: &Vec<DeviceData>,
) {
    for device in devices.iter() {
        let timestamp = timestamp + device.time_offset;

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateGPS,
            device: device.clone(),
            timestamp: timestamp,
            sequence: 0,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateVehicleData,
            device: device.clone(),
            timestamp: timestamp,
            sequence: 0,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GeneratePeripheralData,
            device: device.clone(),
            timestamp: timestamp,
            sequence: 0,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateMotor,
            device: device.clone(),
            timestamp: timestamp,
            sequence: 0,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateBMS,
            device: device.clone(),
            timestamp: timestamp,
            sequence: 0,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateIMU,
            device: device.clone(),
            timestamp: timestamp,
            sequence: 0,
        }));
    }
}

pub fn create_streams(num_devices: u32) -> Vec<(String, usize)> {
    (1..(num_devices + 1))
        .map(|i| {
            vec![
                (format!("/tenants/demo/devices/{}/events/gps/jsonarray", i), 1 as usize),
                (
                    format!("/tenants/demo/devices/{}/events/peripheral_state/jsonarray", i),
                    1 as usize,
                ),
                (format!("/tenants/demo/devices/{}/events/device_shadow/jsonarray", i), 1 as usize),
                (format!("/tenants/demo/devices/{}/events/motor/jsonarray", i), 4 as usize),
                (format!("/tenants/demo/devices/{}/events/bms/jsonarray", i), 4 as usize),
                (format!("/tenants/demo/devices/{}/events/imu/jsonarray", i), 10 as usize),
                (format!("/tenants/demo/devices/{}/action/status", i), 1 as usize),
            ]
            .into_iter()
        })
        .flatten()
        .collect()
}

pub fn next_event_duration(event_type: DataEventType) -> Duration {
    match event_type {
        DataEventType::GenerateGPS => Duration::from_millis(1000),
        DataEventType::GenerateIMU => Duration::from_millis(100),
        DataEventType::GenerateVehicleData => Duration::from_millis(1000),
        DataEventType::GeneratePeripheralData => Duration::from_millis(1000),
        DataEventType::GenerateMotor => Duration::from_millis(250),
        DataEventType::GenerateBMS => Duration::from_millis(250),
    }
}

pub async fn process_data_event(
    event: &DataEvent,
    events: &mut BinaryHeap<Event>,
    partitions: &mut Partitions,
) {
    let data = match event.event_type {
        DataEventType::GenerateGPS => generate_gps_data(&event.device, event.sequence),
        DataEventType::GenerateIMU => generate_imu_data(&event.device, event.sequence),
        DataEventType::GenerateVehicleData => {
            generate_device_shadow_data(&event.device, event.sequence)
        }
        DataEventType::GeneratePeripheralData => {
            generate_peripheral_state_data(&event.device, event.sequence)
        }
        DataEventType::GenerateMotor => generate_motor_data(&event.device, event.sequence),
        DataEventType::GenerateBMS => generate_bms_data(&event.device, event.sequence),
    };

    if let Err(e) = partitions
        .map
        .entry(data.stream.clone())
        .or_insert(Stream::new(&data.stream, &data.stream, 10, partitions.tx.clone()))
        .fill(data)
        .await
    {
        error!("Failed to send data. Error = {:?}", e);
    }

    let duration = next_event_duration(event.event_type);

    events.push(Event::DataEvent(DataEvent {
        sequence: event.sequence + 1,
        timestamp: event.timestamp + duration,
        device: event.device.clone(),
        event_type: event.event_type,
    }));
}

async fn process_action_response_event(
    event: &ActionResponseEvent,
    data_tx: Sender<Box<dyn Package>>,
) {
    //info!("Sending action response {:?} {} {} {}", event.device_id, event.action_id, event.progress, event.status);

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut payload = serde_json::Map::new();

    payload.insert(String::from("errors"), Value::Array(vec![]));
    payload.insert(String::from("id"), Value::String(event.action_id.to_string()));
    payload.insert(String::from("progress"), json!(event.progress as i64));
    payload.insert(String::from("state"), Value::String(event.status.clone()));

    let data = Payload {
        timestamp,
        sequence: 0,
        stream: format!("/tenants/demo/devices/{}/action/status", event.device_id),
        payload: json!(payload),
    };

    let mut stream = Stream::new(&data.stream, &data.stream, 1, data_tx);

    info!(
        "Sending action response {:?} {} {} {}",
        event.device_id, event.action_id, event.progress, event.status
    );
    if let Err(e) = stream.fill(data).await {
        error!("Failed to send action result {:?}", e);
    }
    info!("Successfully sent action response");
}

pub async fn process_events(
    events: &mut BinaryHeap<Event>,
    partitions: &mut Partitions,
    data_tx: Sender<Box<dyn Package>>,
) {
    if let Some(e) = events.pop() {
        let current_time = Instant::now();
        let timestamp = event_timestamp(&e);

        if timestamp > current_time {
            let time_left = timestamp.duration_since(current_time);

            if time_left > Duration::from_millis(50) {
                tokio::time::sleep(time_left).await;
            }
        }

        match e {
            Event::DataEvent(event) => {
                process_data_event(&event, events, partitions).await;
            }

            Event::ActionResponseEvent(event) => {
                process_action_response_event(&event, data_tx).await;
            }
        }
    } else {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn generate_action_events(action: &Action, events: &mut BinaryHeap<Event>) {
    info!("Generating action events {}", action.action_id);
    let now = Instant::now() + Duration::from_millis(rand::thread_rng().gen_range(0..5000));

    for i in 1..100 {
        events.push(Event::ActionResponseEvent(ActionResponseEvent {
            action_id: action.action_id.clone(),
            device_id: "123".to_string(),
            progress: i,
            status: String::from("in_progress"),
            timestamp: now + Duration::from_secs(i as u64),
        }));
    }

    events.push(Event::ActionResponseEvent(ActionResponseEvent {
        action_id: action.action_id.clone(),
        device_id: "123".to_string(),
        progress: 100,
        status: String::from("Completed"),
        timestamp: now + Duration::from_secs(100),
    }));
}

pub async fn start(
    data_tx: Sender<Box<dyn Package>>,
    actions_rx: Receiver<Action>,
    num_devices: u32,
    gps_paths: String,
) -> Result<(), Error> {
    let paths = read_gps_paths(gps_paths);

    let devices = (1..(num_devices + 1)).map(|i| new_device_data(i, &paths)).collect::<Vec<_>>();

    let mut events = BinaryHeap::new();

    generate_initial_events(&mut events, Instant::now(), &devices);

    let mut partitions = Partitions { map: HashMap::new(), tx: data_tx.clone() };
    let mut time = Instant::now();
    let mut i = 0;

    loop {
        let current_time = Instant::now();

        if time.elapsed() > Duration::from_secs(1) {
            if let Some(event) = events.peek() {
                let timestamp = event_timestamp(event);

                if current_time > timestamp {
                    info!("Time delta {:?} {:?} {:?}", num_devices, current_time - timestamp, i);
                } else {
                    info!("Time delta {:?} -{:?} {:?}", num_devices, timestamp - current_time, i);
                }

                i += 1;
            }
            time = Instant::now();
        }

        select! {
            action = actions_rx.recv_async() => {
                let action = action?;
                generate_action_events(&action, &mut events);
            }

            _ = process_events(&mut events, &mut partitions, data_tx.clone()) => {
            }
        }
    }
}

impl Package for Buffer<Payload> {
    fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.buffer)
    }

    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}
