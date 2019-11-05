use std::collections::HashMap;
use super::Data;
use derive_more::From;

use std::thread;
use std::time::Duration;
use std::mem;

type Channel = String;

pub struct Simulator {
    buffer: HashMap<Channel, Vec<Data>>
}

#[derive(Debug, From)]
pub enum Error {
    Dummy
}

impl Simulator {
    pub fn new() -> Result<Simulator, Error> {
        let s: Simulator = Simulator {
            buffer: HashMap::new()
        };

        Ok(s)
    }

    fn fill_buffer(&mut self, channel: &str, data: Data) -> Option<HashMap<Channel, Vec<Data>>> {
        if let Some(buffer) = self.buffer.get_mut(channel) {
            buffer.push(data);
            if buffer.len() > 10 {
                let buffer = mem::replace(&mut self.buffer, HashMap::new());
                Some(buffer)
            } else {
                None
            }
        } else {
            self.buffer.insert(channel.to_owned(), vec![data]);
            None
        }
    }

    pub fn start(&mut self) {
        for i in 0..1000 {
            println!("count = {}", i);
            thread::sleep(Duration::from_secs(1));

            let mut data = HashMap::new();
            data.insert("voltage".to_owned(), "100".to_owned());
            data.insert("current".to_owned(), "1000".to_owned());
            data.insert("temperature".to_owned(), "10.0".to_owned());

            let data = Data {
                data
            };

            if let Some(buffer) = self.fill_buffer("bms", data) {
                println!("{:?}", buffer);
            }
        }
    }
}