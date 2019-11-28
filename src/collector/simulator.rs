use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

use derive_more::From;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::vec::IntoIter;

use super::Reader;

#[derive(Debug, Serialize, Deserialize)]
pub struct Can {
    timestamp: u64,
    sequence: usize,
    can_id: String,
    data: String,
    dev: String,
}

impl Can {
    pub fn new(sequence: usize, timestamp: u64) -> Can {
        let can_ids = vec!["0x148", "0x149"];
        let mut rng = rand::thread_rng();
        let can_id = *can_ids.choose(&mut rng).unwrap();

        Can {
            timestamp,
            sequence,
            can_id: can_id.to_owned(),
            data: "123456789123456789".to_owned(),
            dev: "can0".to_owned(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Bms {
    timestamp: u64,
    sequence: usize,
    cell_count: u32,
    temperature: f32,
    current: f32,
    voltage: f32,
    faults: u64,
}

impl Bms {
    pub fn new(sequence: usize, timestamp: u64) -> Bms {
        let mut rng = rand::thread_rng();

        Bms {
            timestamp,
            sequence,
            cell_count: 100,
            temperature: rng.gen_range(50.0, 100.0),
            current: rng.gen_range(10.0, 30.0),
            voltage: rng.gen_range(70.0, 100.0),
            faults: rng.gen_range(0, 1000000),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Motor {
    timestamp: u64,
    sequence: usize,
    rpm: u32,
    current: f32,
    voltage: f32,
    temperature: f32,
    faults: u64,
}

impl Motor {
    pub fn new(sequence: usize, timestamp: u64) -> Motor {
        let mut rng = rand::thread_rng();
        Motor {
            timestamp,
            sequence,
            rpm: rng.gen_range(0, 1000000),
            current: rng.gen_range(10.0, 30.0),
            voltage: rng.gen_range(10.0, 30.0),
            temperature: rng.gen_range(10.0, 30.0),
            faults: rng.gen_range(0, 1000000),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, )]
pub struct Gps {
    timestamp: u64,
    sequence: usize,
    latitude: f32,
    longitude: f32,
    speed: f32,
    time: f32,
    climb: f32,
}

impl Gps {
    pub fn new(sequence: usize, timestamp: u64, latitude: f32, longitude: f32) -> Gps {
        let mut rng = rand::thread_rng();

        Gps {
            timestamp,
            sequence,
            latitude,
            longitude,
            speed: rng.gen_range(10.0, 30.0),
            time: rng.gen_range(10.0, 30.0),
            climb: rng.gen_range(10.0, 30.0),
        }
    }
}

pub struct Route {
    route: Vec<String>,
    iter: IntoIter<String>
}

impl Route {
    pub fn new() -> Route {
        let route = File::open("config/track_points.csv").unwrap();
        let route: Vec<String> = BufReader::new(route).lines().map(|e| e.unwrap()).collect();
        let iter = route.clone().into_iter();

        Route {
           route,
            iter
        }
    }

    pub fn next(&mut self) -> (f32, f32) {
        if let Some(line) = self.iter.next() {
            let data: Vec<&str> = line.split(",").collect();

            let lon = data[0].parse().unwrap();
            let lat = data[1].parse().unwrap();

            (lat, lon)
        } else {
            self.iter = self.route.clone().into_iter();
            (0.0, 0.0)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Data {
    Can(Can),
    Gps(Gps),
    Bms(Bms),
    Motor(Motor),
}

pub struct Simulator {
    count: usize,
    channel_count: usize,
    can_seq: usize,
    motor_seq: usize,
    bms_seq: usize,
    gps_seq: usize
}

#[derive(Debug, From)]
pub enum Error {
    Dummy,
}

impl Simulator {
    pub fn new() -> Result<Simulator, Error> {
        let simutlator = Simulator {
            count: 0,
            channel_count: 4,
            can_seq: 0,
            motor_seq: 0,
            bms_seq: 0,
            gps_seq: 0
        };

        Ok(simutlator)
    }
}

impl Reader for Simulator {
    type Item = Data;
    type Error = Error;

    fn next(&mut self) -> Result<Option<(String, Self::Item)>, Self::Error> {         
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = timestamp.as_secs();
        let mut route = Route::new();

        thread::sleep(Duration::from_millis(100));

        let data = match self.count % self.channel_count {
            0 => {
                self.can_seq += 1;
                let o = Data::Can(Can::new(self.can_seq, timestamp));
                Some(("can".to_owned(), o))
            }
            1 => {
                self.motor_seq += 1;
                let o = Data::Motor(Motor::new(self.motor_seq, timestamp));
                Some(("motor".to_owned(), o))
            }
            2 => {
                self.bms_seq += 1;
                let o = Data::Bms(Bms::new(self.bms_seq, timestamp));
                Some(("bms".to_owned(), o))
            }
            3 => {
                let (lat, lon) = route.next();
                self.gps_seq += 1;
                let o = Data::Gps(Gps::new(self.gps_seq, timestamp, lat, lon));
                Some(("gps".to_owned(), o))
            }
            _ => None
        };

        self.count += 1;
        Ok(data)
    }

    fn channels(&self) -> Vec<String> { 
        vec![
            "can".to_owned(),
            "motor".to_owned(),
            "bms".to_owned(),
            "gps".to_owned(),
        ]
    }
}
