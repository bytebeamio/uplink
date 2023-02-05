use log::{error, info, trace};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::select;
use tokio_util::codec::LinesCodecError;

use std::collections::BinaryHeap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{cmp::Ordering, fs, io, sync::Arc};

use crate::base::bridge::BridgeTx;
use crate::base::SimulatorConfig;
use crate::{Action, ActionResponse, Payload};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Location {
    latitude: f64,
    longitude: f64,
}

#[derive(Clone)]
pub struct DeviceData {
    device_id: String,
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
    sequence: u32,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ActionResponseEvent {
    timestamp: Instant,
    action_id: String,
    device_id: String,
    status: String,
    progress: u8,
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
        Some(other.cmp(self))
    }
}

pub fn generate_gps_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let path_len = device.path.len() as u32;
    let path_index = ((device.path_offset + sequence) % path_len) as usize;
    let position = device.path.get(path_index).unwrap();

    Payload {
        timestamp,
        device_id: Some(device.device_id.to_owned()),
        sequence,
        stream: "gps".to_string(),
        payload: json!(position),
    }
}

pub fn generate_float(start: f64, end: f64) -> f64 {
    let mut rng = rand::thread_rng();

    rng.gen_range(start..end)
}

pub fn generate_int(start: i32, end: i32) -> i64 {
    rand::thread_rng().gen_range(start..end) as i64
}

pub fn generate_bool_string(p: f64) -> String {
    if rand::thread_rng().gen_bool(p) {
        "on".to_owned()
    } else {
        "off".to_owned()
    }
}

#[derive(Debug, Serialize)]
struct Bms {
    periodicity_ms: i32,
    mosfet_temperature: f64,
    ambient_temperature: f64,
    mosfet_status: i32,
    cell_voltage_count: i32,
    cell_voltage_1: f64,
    cell_voltage_2: f64,
    cell_voltage_3: f64,
    cell_voltage_4: f64,
    cell_voltage_5: f64,
    cell_voltage_6: f64,
    cell_voltage_7: f64,
    cell_voltage_8: f64,
    cell_voltage_9: f64,
    cell_voltage_10: f64,
    cell_voltage_11: f64,
    cell_voltage_12: f64,
    cell_voltage_13: f64,
    cell_voltage_14: f64,
    cell_voltage_15: f64,
    cell_voltage_16: f64,
    cell_thermistor_count: i32,
    cell_temp_1: f64,
    cell_temp_2: f64,
    cell_temp_3: f64,
    cell_temp_4: f64,
    cell_temp_5: f64,
    cell_temp_6: f64,
    cell_temp_7: f64,
    cell_temp_8: f64,
    cell_balancing_status: i32,
    pack_voltage: f64,
    pack_current: f64,
    pack_soc: f64,
    pack_soh: f64,
    pack_sop: f64,
    pack_cycle_count: i64,
    pack_available_energy: i64,
    pack_consumed_energy: i64,
    pack_fault: i32,
    pack_status: i32,
}

pub fn generate_bms_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload = Bms {
        periodicity_ms: 250,
        mosfet_temperature: generate_float(40f64, 45f64),
        ambient_temperature: generate_float(35f64, 40f64),
        mosfet_status: 1,
        cell_voltage_count: 16,
        cell_voltage_1: generate_float(3.0f64, 3.2f64),
        cell_voltage_2: generate_float(3.0f64, 3.2f64),
        cell_voltage_3: generate_float(3.0f64, 3.2f64),
        cell_voltage_4: generate_float(3.0f64, 3.2f64),
        cell_voltage_5: generate_float(3.0f64, 3.2f64),
        cell_voltage_6: generate_float(3.0f64, 3.2f64),
        cell_voltage_7: generate_float(3.0f64, 3.2f64),
        cell_voltage_8: generate_float(3.0f64, 3.2f64),
        cell_voltage_9: generate_float(3.0f64, 3.2f64),
        cell_voltage_10: generate_float(3.0f64, 3.2f64),
        cell_voltage_11: generate_float(3.0f64, 3.2f64),
        cell_voltage_12: generate_float(3.0f64, 3.2f64),
        cell_voltage_13: generate_float(3.0f64, 3.2f64),
        cell_voltage_14: generate_float(3.0f64, 3.2f64),
        cell_voltage_15: generate_float(3.0f64, 3.2f64),
        cell_voltage_16: generate_float(3.0f64, 3.2f64),
        cell_thermistor_count: 8,
        cell_temp_1: generate_float(40.0f64, 43.0f64),
        cell_temp_2: generate_float(40.0f64, 43.0f64),
        cell_temp_3: generate_float(40.0f64, 43.0f64),
        cell_temp_4: generate_float(40.0f64, 43.0f64),
        cell_temp_5: generate_float(40.0f64, 43.0f64),
        cell_temp_6: generate_float(40.0f64, 43.0f64),
        cell_temp_7: generate_float(40.0f64, 43.0f64),
        cell_temp_8: generate_float(40.0f64, 43.0f64),
        cell_balancing_status: 1,
        pack_voltage: generate_float(95f64, 96f64),
        pack_current: generate_float(15f64, 20f64),
        pack_soc: generate_float(80f64, 90f64),
        pack_soh: generate_float(9.5f64, 9.9f64),
        pack_sop: generate_float(9.5f64, 9.9f64),
        pack_cycle_count: generate_int(100, 150),
        pack_available_energy: generate_int(2000, 3000),
        pack_consumed_energy: generate_int(2000, 3000),
        pack_fault: 0,
        pack_status: 1,
    };

    Payload {
        timestamp,
        device_id: Some(device.device_id.to_owned()),
        sequence,
        stream: "bms".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize)]
struct Imu {
    ax: f64,
    ay: f64,
    az: f64,
    pitch: f64,
    roll: f64,
    yaw: f64,
    magx: f64,
    magy: f64,
    magz: f64,
}

pub fn generate_imu_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload = Imu {
        ax: generate_float(1f64, 2.8f64),
        ay: generate_float(1f64, 2.8f64),
        az: generate_float(9.79f64, 9.82f64),
        pitch: generate_float(0.8f64, 1f64),
        roll: generate_float(0.8f64, 1f64),
        yaw: generate_float(0.8f64, 1f64),
        magx: generate_float(-45f64, -15f64),
        magy: generate_float(-45f64, -15f64),
        magz: generate_float(-45f64, -15f64),
    };

    Payload {
        timestamp,
        device_id: Some(device.device_id.to_owned()),
        sequence,
        stream: "imu".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize)]
struct Motor {
    motor_temperature1: f64,
    motor_temperature2: f64,
    motor_temperature3: f64,
    motor_voltage: f64,
    motor_current: f64,
    motor_rpm: i64,
}

pub fn generate_motor_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload = Motor {
        motor_temperature1: generate_float(40f64, 45f64),
        motor_temperature2: generate_float(40f64, 45f64),
        motor_temperature3: generate_float(40f64, 45f64),

        motor_voltage: generate_float(95f64, 96f64),
        motor_current: generate_float(20f64, 25f64),
        motor_rpm: generate_int(1000, 9000),
    };

    Payload {
        timestamp,
        device_id: Some(device.device_id.to_owned()),
        sequence,
        stream: "motor".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize)]
struct PeripheralState {
    gps_status: String,
    gsm_status: String,
    imu_status: String,
    left_indicator: String,
    right_indicator: String,
    headlamp: String,
    horn: String,
    left_brake: String,
    right_brake: String,
}

pub fn generate_peripheral_state_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload = PeripheralState {
        gps_status: generate_bool_string(0.99),
        gsm_status: generate_bool_string(0.99),
        imu_status: generate_bool_string(0.99),
        left_indicator: generate_bool_string(0.1),
        right_indicator: generate_bool_string(0.1),
        headlamp: generate_bool_string(0.9),
        horn: generate_bool_string(0.05),
        left_brake: generate_bool_string(0.1),
        right_brake: generate_bool_string(0.1),
    };

    Payload {
        timestamp,
        device_id: Some(device.device_id.to_owned()),
        sequence,
        stream: "peripheral_state".to_string(),
        payload: json!(payload),
    }
}

#[derive(Debug, Serialize)]
struct DeviceShadow {
    mode: String,
    status: String,
    firmware_version: String,
    config_version: String,
    distance_travelled: i64,
    range: i64,
    #[serde(rename(serialize = "SOC"))]
    soc: f64,
}

pub fn generate_device_shadow_data(device: &DeviceData, sequence: u32) -> Payload {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let payload = DeviceShadow {
        mode: "economy".to_owned(),
        status: "Locked".to_owned(),
        firmware_version: "1.33-Aug-2020b1".to_owned(),
        config_version: "1.23".to_owned(),
        distance_travelled: generate_int(20000, 30000),
        range: generate_int(50000, 60000),
        soc: generate_float(50f64, 90f64),
    };

    Payload {
        timestamp,
        device_id: Some(device.device_id.to_owned()),
        sequence,
        stream: "device_shadow".to_string(),
        payload: json!(payload),
    }
}

pub fn read_gps_paths(paths_dir: &str) -> Vec<Arc<Vec<Location>>> {
    (0..10)
        .map(|i| {
            let file_name = format!("{}/path{}.json", paths_dir, i);

            let contents = fs::read_to_string(file_name).expect("Oops, failed ot read path");

            let parsed: Vec<Location> = serde_json::from_str(&contents).unwrap();

            Arc::new(parsed)
        })
        .collect::<Vec<_>>()
}

pub fn new_device_data(device_id: String, paths: &[Arc<Vec<Location>>]) -> DeviceData {
    let mut rng = rand::thread_rng();

    let n = rng.gen_range(0..10);
    let path = paths.get(n).unwrap().clone();
    let path_index = rng.gen_range(0..path.len()) as u32;

    DeviceData {
        device_id,
        path,
        path_offset: path_index,
        time_offset: Duration::from_millis(rng.gen_range(0..10000)),
    }
}

pub fn generate_initial_events(
    events: &mut BinaryHeap<Event>,
    timestamp: Instant,
    devices: &[DeviceData],
) {
    for device in devices.iter() {
        let timestamp = timestamp + device.time_offset;

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateGPS,
            device: device.clone(),
            timestamp,
            sequence: 1,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateVehicleData,
            device: device.clone(),
            timestamp,
            sequence: 1,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GeneratePeripheralData,
            device: device.clone(),
            timestamp,
            sequence: 1,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateMotor,
            device: device.clone(),
            timestamp,
            sequence: 1,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateBMS,
            device: device.clone(),
            timestamp,
            sequence: 1,
        }));

        events.push(Event::DataEvent(DataEvent {
            event_type: DataEventType::GenerateIMU,
            device: device.clone(),
            timestamp,
            sequence: 1,
        }));
    }
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
    bridge_tx: &BridgeTx,
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

    bridge_tx.send_payload(data).await;

    let duration = next_event_duration(event.event_type);

    events.push(Event::DataEvent(DataEvent {
        sequence: event.sequence + 1,
        timestamp: event.timestamp + duration,
        device: event.device.clone(),
        event_type: event.event_type,
    }));
}

async fn process_action_response_event(event: &ActionResponseEvent, bridge_tx: &BridgeTx) {
    //info!("Sending action response {:?} {} {} {}", event.device_id, event.action_id, event.progress, event.status);
    let mut response = ActionResponse::progress(&event.action_id, &event.status, event.progress);
    response.device_id = Some(event.device_id.to_owned());

    info!(
        "Sending action response {:?} {} {} {}",
        event.device_id, event.action_id, event.progress, event.status
    );

    bridge_tx.send_action_response(response).await;
    info!("Successfully sent action response");
}

pub async fn process_events(events: &mut BinaryHeap<Event>, bridge_tx: &BridgeTx) {
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
                process_data_event(&event, events, bridge_tx).await;
            }

            Event::ActionResponseEvent(event) => {
                process_action_response_event(&event, bridge_tx).await;
            }
        }
    } else {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn generate_action_events(action: Action, events: &mut BinaryHeap<Event>) {
    let action_id = action.action_id;
    let device_id = action.device_id.unwrap();

    info!("Generating action events for action: {action_id} on device: {device_id}");
    let now = Instant::now() + Duration::from_millis(rand::thread_rng().gen_range(0..5000));

    for i in 1..100 {
        events.push(Event::ActionResponseEvent(ActionResponseEvent {
            action_id: action_id.clone(),
            device_id: device_id.clone(),
            progress: i,
            status: String::from("in_progress"),
            timestamp: now + Duration::from_secs(i as u64),
        }));
    }

    events.push(Event::ActionResponseEvent(ActionResponseEvent {
        action_id: action_id.clone(),
        device_id: device_id.clone(),
        progress: 100,
        status: String::from("Completed"),
        timestamp: now + Duration::from_secs(100),
    }));
}

#[tokio::main(flavor = "current_thread")]
pub async fn start(bridge_tx: BridgeTx, simulator_config: &SimulatorConfig) -> Result<(), Error> {
    let paths = read_gps_paths(&simulator_config.gps_paths);
    let num_devices = simulator_config.num_devices;
    let actions_rx = bridge_tx.register_action_routes(&simulator_config.actions).await;

    let devices =
        (1..(num_devices + 1)).map(|i| new_device_data(i.to_string(), &paths)).collect::<Vec<_>>();

    let mut events = BinaryHeap::new();

    generate_initial_events(&mut events, Instant::now(), &devices);
    let mut time = Instant::now();
    let mut i = 0;

    loop {
        let current_time = Instant::now();
        if time.elapsed() > Duration::from_secs(1) {
            if let Some(event) = events.peek() {
                let timestamp = event_timestamp(event);

                if current_time > timestamp {
                    trace!("Time delta {:?} {:?} {:?}", num_devices, current_time - timestamp, i);
                } else {
                    trace!("Time delta {:?} -{:?} {:?}", num_devices, timestamp - current_time, i);
                }

                i += 1;
            }
            time = Instant::now();
        }

        select! {
            action = actions_rx.recv_async() => {
                let action = action?;
                generate_action_events(action, &mut events);
            }
            _ = process_events(&mut events, &bridge_tx) => {
            }
        }
    }
}
