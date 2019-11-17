use std::collections::HashMap;
use std::mem;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::Sender;
use derive_more::From;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::PathBuf;
use std::io::{BufReader, BufRead, Lines};
use std::vec::IntoIter;

type Channel = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Can {
    timestamp: u64,
    sequence: u32,
    can_id: String,
    data: String,
    dev: String,
}

impl Can {
    pub fn new(sequence: u32, timestamp: u64) -> Can {
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
    sequence: u32,
    cell_count: u32,
    temperature: f32,
    current: f32,
    voltage: f32,
    faults: u64,
}

impl Bms {
    pub fn new(sequence: u32, timestamp: u64) -> Bms {
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
    sequence: u32,
    rpm: u32,
    current: f32,
    voltage: f32,
    temperature: f32,
    faults: u64,
}

impl Motor {
    pub fn new(sequence: u32, timestamp: u64) -> Motor {
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
    sequence: u32,
    latitude: f32,
    longitude: f32,
    speed: f32,
    time: f32,
    climb: f32,
}

impl Gps {
    pub fn new(sequence: u32, timestamp: u64, latitude: f32, longitude: f32) -> Gps {
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

// TODO: Embed batching directly into Data trait make sure that
// TODO: it's serializable
// TODO: trait Data: Serialize + DeSerialize

#[derive(Debug)]
pub struct Buffer {
    pub channel: String,
    pub buffer: Vec<Data>,
}

impl Buffer {
    pub fn new(channel: &str) -> Buffer {
        Buffer {
            channel: channel.to_owned(),
            buffer: Vec::new(),
        }
    }

    pub fn fill(&mut self, data: Data) -> Option<Buffer> {
        self.buffer.push(data);

        if self.buffer.len() > 10 {
            let buffer = mem::replace(&mut self.buffer, Vec::new());
            let channel = self.channel.clone();
            let buffer = Buffer { channel, buffer };
            return Some(buffer);
        }

        None
    }
}

pub struct Simulator {
    buffers: HashMap<Channel, Buffer>,
    tx: Sender<Buffer>,
}

#[derive(Debug, From)]
pub enum Error {
    Dummy,
}

impl Simulator {
    pub fn new(tx: Sender<Buffer>) -> Result<Simulator, Error> {
        let mut buffers = HashMap::new();
        buffers.insert("can".to_owned(), Buffer::new("can"));
        buffers.insert("bms".to_owned(), Buffer::new("bms"));
        buffers.insert("motor".to_owned(), Buffer::new("motor"));
        buffers.insert("gps".to_owned(), Buffer::new("gps"));

        let s: Simulator = Simulator { buffers, tx };

        Ok(s)
    }

    fn fill_buffer(&mut self, channel: &str, data: Data) -> Option<Buffer> {
        if let Some(buffer) = self.buffers.get_mut(channel) {
            buffer.fill(data)
        } else {
            error!("Invalid channel = {}", channel);
            None
        }
    }

    pub fn start(&mut self) {
        let mut count = 0;
        let mut route = Route::new();

        loop {
            thread::sleep(Duration::from_millis(100));
            count += 1;

            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let timestamp = timestamp.as_secs();

            let data = Data::Can(Can::new(count, timestamp));
            if let Some(buffer) = self.fill_buffer("can", data) {
                self.tx.send(buffer).unwrap();
            }

            let data = Data::Motor(Motor::new(count, timestamp));
            if let Some(buffer) = self.fill_buffer("motor", data) {
                self.tx.send(buffer).unwrap();
            }

            let data = Data::Bms(Bms::new(count, timestamp));
            if let Some(buffer) = self.fill_buffer("bms", data) {
                self.tx.send(buffer).unwrap();
            }

            let (lat, lon) = route.next();
            let data = Data::Gps(Gps::new(count, timestamp, lat, lon));
            if let Some(buffer) = self.fill_buffer("gps", data) {
                self.tx.send(buffer).unwrap();
            }
        }
    }
}
