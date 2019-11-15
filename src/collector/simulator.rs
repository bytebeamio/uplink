use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use std::mem;

use derive_more::From;
use crossbeam_channel::Sender;
use serde::{Serialize, Deserialize};

type Channel = String;

// TODO: Embed batching directly into Data trait make sure that
// TODO: it's serializable
// TODO: trait Data: Serialize + DeSerialize

#[derive(Debug, From, Serialize, Deserialize)]
pub struct Data {
    data: HashMap<String, String>,
}

pub struct Simulator {
    buffer: HashMap<Channel, Vec<Data>>,
    tx: Sender<Vec<Data>>
}

#[derive(Debug, From)]
pub enum Error {
    Dummy
}

impl Simulator {
    pub fn new(tx: Sender<Vec<Data>>) -> Result<Simulator, Error> {
        let s: Simulator = Simulator {
            buffer: HashMap::new(),
            tx
        };

        Ok(s)
    }

    fn fill_buffer(&mut self, channel: &str, data: Data) -> Option<HashMap<Channel, Vec<Data>>> {
        if let Some(buffer) = self.buffer.get_mut(channel) {
            buffer.push(data);
            
            // return the filled buffer of a given channel
            if buffer.len() > 10 {
                let buffer = mem::replace(&mut self.buffer, HashMap::new());
                return Some(buffer)
            }
        } else {
            self.buffer.insert(channel.to_owned(), vec![data]);
        }

        None
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

            if let Some(mut buffer) = self.fill_buffer("bms", data) {
                //TODO: Remove hardcode
                let data = buffer.remove("bms").unwrap();
                self.tx.send(data);
            }
        }
    }
}