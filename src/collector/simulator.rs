use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use std::mem;

use derive_more::From;
use crossbeam_channel::Sender;
use serde::{Serialize, Deserialize};
use rand::Rng;

type Channel = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Bms {
    cell_count: u32,
    average_pack_temperature: f32,
    current: f32,
    voltage: f32,
    faults: u64,
}

impl Bms {
    pub fn new() -> Bms {
        let mut rng = rand::thread_rng();

        Bms {
            cell_count: 100,
            average_pack_temperature: rng.gen_range(50.0, 100.0),
            current: rng.gen_range(10.0, 30.0),
            voltage: rng.gen_range(70.0, 100.0),
            faults: rng.gen_range(0, 1000000),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Motor {
    rpm: u32,
    current: f32,
    voltage: f32,
    temperature: f32,
    faults: u64
}

impl Motor {
    pub fn new() -> Motor {
        let mut rng = rand::thread_rng();
        Motor {
            rpm: rng.gen_range(0, 1000000),
            current: rng.gen_range(10.0, 30.0),
            voltage: rng.gen_range(10.0, 30.0),
            temperature: rng.gen_range(10.0, 30.0),
            faults: rng.gen_range(0, 1000000),
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Gps {
    latitude: f32,
    longitude: f32,
    speed: f32,
    time: f32,
    climb: f32,
    count: usize
}

impl Gps {
    pub fn new() -> Gps {
        let mut rng = rand::thread_rng();
        Gps {
            latitude: rng.gen_range(10.0, 30.0),
            longitude: rng.gen_range(10.0, 30.0),
            speed: rng.gen_range(10.0, 30.0),
            time: rng.gen_range(10.0, 30.0),
            climb: rng.gen_range(10.0, 30.0),
            count: 0
        }
    }

    pub fn route(&mut self, latitude: f32, longitude: f32) {
        self.latitude = latitude;
        self.longitude = longitude;
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Data {
    Gps(Gps),
    Bms(Bms),
    Motor(Motor)
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
            buffer: Vec::new()
        }
    }

    pub fn fill(&mut self, data: Data) -> Option<Buffer> {
        self.buffer.push(data);

        if self.buffer.len() > 10 {
            let buffer = mem::replace(&mut self.buffer, Vec::new());
            let channel = self.channel.clone();
            let buffer = Buffer {channel, buffer};
            return Some(buffer)
        }

        None
    }
}

pub struct Simulator {
    buffers: HashMap<Channel, Buffer>,
    tx: Sender<Buffer>
}

#[derive(Debug, From)]
pub enum Error {
    Dummy
}

impl Simulator {
    pub fn new(tx: Sender<Buffer>) -> Result<Simulator, Error> {
        let mut buffers = HashMap::new();
        buffers.insert("bms".to_owned(), Buffer::new("bms"));
        buffers.insert("motor".to_owned(), Buffer::new("motor"));
        buffers.insert("gps".to_owned(), Buffer::new("gps"));

        let s: Simulator = Simulator {
            buffers,
            tx
        };

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
        for i in 0..10000000 {
            println!("count = {}", i);
            thread::sleep(Duration::from_millis(100));

            let (channel, data) = match i {
                i if i % 2 == 0 => ("motor", Data::Motor(Motor::new())),
                i if i % 3 == 0 => ("bms", Data::Bms(Bms::new())),
                i if i % 4 == 0 => ("gps", Data::Gps(Gps::new())),
                _ => continue
            };

            if let Some(buffer) = self.fill_buffer(channel, data) {
                self.tx.send(buffer).unwrap();
            }
        }
    }
}