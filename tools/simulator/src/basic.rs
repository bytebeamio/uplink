use fake::{Dummy, Fake, Faker};
use flume::Sender;
use log::{error, trace};
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::interval;

use std::sync::Arc;
use std::time::Duration;

use crate::Payload;

const RESET_LIMIT: u32 = 1500;

#[inline]
fn next_sequence(sequence: &mut u32) {
    if *sequence > RESET_LIMIT {
        *sequence = 1
    } else {
        *sequence += 1
    };
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DataType {
    Gps,
    Imu,
    DeviceShadow,
    PeripheralData,
    Motor,
    Bms,
}

impl DataType {
    fn duration(&self) -> Duration {
        match self {
            DataType::Gps => Duration::from_millis(1000),
            DataType::Imu => Duration::from_millis(100),
            DataType::DeviceShadow => Duration::from_millis(1000),
            DataType::PeripheralData => Duration::from_millis(1000),
            DataType::Motor => Duration::from_millis(250),
            DataType::Bms => Duration::from_millis(250),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct DeviceData {
    pub path: Arc<Vec<Gps>>,
    pub path_offset: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Gps {
    latitude: f64,
    longitude: f64,
}

impl Gps {
    pub async fn simulate(tx: Sender<Payload>, device: DeviceData) {
        let mut sequence = 0;
        let mut interval = interval(DataType::Gps.duration());
        let path_len = device.path.len() as u32;

        loop {
            interval.tick().await;
            next_sequence(&mut sequence);
            let path_index = ((device.path_offset + sequence) % path_len) as usize;
            let payload = device.path.get(path_index).unwrap();

            trace!("Data Event: {:?}", payload);

            if let Err(e) =
                tx.send_async(Payload::new("gps".to_string(), sequence, json!(payload))).await
            {
                error!("{e}");
                break;
            }
        }
    }
}

struct BoolString;

impl Dummy<BoolString> for String {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &BoolString, rng: &mut R) -> String {
        const NAMES: &[&str] = &["on", "off"];
        NAMES.choose(rng).unwrap().to_string()
    }
}

struct VerString;

impl Dummy<VerString> for String {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &VerString, rng: &mut R) -> String {
        const NAMES: &[&str] = &["v0.0.1", "v0.5.0", "v1.0.1"];
        NAMES.choose(rng).unwrap().to_string()
    }
}

#[derive(Debug, Serialize, Dummy)]
pub struct Bms {
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

impl Bms {
    pub async fn simulate(tx: Sender<Payload>) {
        let mut sequence = 0;
        let mut interval = interval(DataType::Bms.duration());
        loop {
            interval.tick().await;
            let payload: Bms = Faker.fake();
            next_sequence(&mut sequence);

            trace!("Data Event: {:?}", payload);

            if let Err(e) =
                tx.send_async(Payload::new("bms".to_string(), sequence, json!(payload))).await
            {
                error!("{e}");
                break;
            }
        }
    }
}

#[derive(Debug, Serialize, Dummy)]
pub struct Imu {
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

impl Imu {
    pub async fn simulate(tx: Sender<Payload>) {
        let mut sequence = 0;
        let mut interval = interval(DataType::Imu.duration());
        loop {
            interval.tick().await;
            let payload: Imu = Faker.fake();
            next_sequence(&mut sequence);

            trace!("Data Event: {:?}", payload);

            if let Err(e) =
                tx.send_async(Payload::new("imu".to_string(), sequence, json!(payload))).await
            {
                error!("{e}");
                break;
            }
        }
    }
}

#[derive(Debug, Serialize, Dummy)]
pub struct Motor {
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

impl Motor {
    pub async fn simulate(tx: Sender<Payload>) {
        let mut sequence = 0;
        let mut interval = interval(DataType::Motor.duration());
        loop {
            interval.tick().await;
            let payload: Motor = Faker.fake();
            next_sequence(&mut sequence);

            trace!("Data Event: {:?}", payload);

            if let Err(e) =
                tx.send_async(Payload::new("motor".to_string(), sequence, json!(payload))).await
            {
                error!("{e}");
                break;
            }
        }
    }
}

#[derive(Debug, Serialize, Dummy)]
pub struct PeripheralState {
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

impl PeripheralState {
    pub async fn simulate(tx: Sender<Payload>) {
        let mut sequence = 0;
        let mut interval = interval(DataType::PeripheralData.duration());
        loop {
            interval.tick().await;
            let payload: PeripheralState = Faker.fake();
            next_sequence(&mut sequence);

            trace!("Data Event: {:?}", payload);

            if let Err(e) = tx
                .send_async(Payload::new("peripheral_state".to_string(), sequence, json!(payload)))
                .await
            {
                error!("{e}");
                break;
            }
        }
    }
}

#[derive(Debug, Serialize, Dummy)]
pub struct DeviceShadow {
    #[dummy(faker = "BoolString")]
    mode: String,
    #[dummy(faker = "BoolString")]
    status: String,
    #[dummy(faker = "VerString")]
    firmware_version: String,
    #[dummy(faker = "VerString")]
    config_version: String,
    #[dummy(faker = "20000..30000")]
    distance_travelled: i64,
    #[dummy(faker = "50000..60000")]
    range: i64,
    #[serde(rename(serialize = "SOC"))]
    #[dummy(faker = "50.0..90.0")]
    soc: f64,
}

impl DeviceShadow {
    pub async fn simulate(tx: Sender<Payload>) {
        let mut sequence = 0;
        let mut interval = interval(DataType::DeviceShadow.duration());
        loop {
            interval.tick().await;
            let payload: DeviceShadow = Faker.fake();
            next_sequence(&mut sequence);

            trace!("Data Event: {:?}", payload);

            if let Err(e) = tx
                .send_async(Payload::new("device_shadow".to_string(), sequence, json!(payload)))
                .await
            {
                error!("{e}");

                break;
            }
        }
    }
}
