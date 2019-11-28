use derive_more::From;
use serde::Serialize;

use std::time::{SystemTime, UNIX_EPOCH};
use crate::collector::{Reader, Batch, Buffer};

pub struct Can {
    bus: socketcan::CANSocket
}

#[derive(From, Debug)]
pub enum Error {
    Can(socketcan::CANSocketOpenError)
}

#[derive(Debug, Serialize)]
pub struct CanFrame {
    timestamp: u64,
    sequence: usize,
    can_id: String,
    data: String,
    dev: String,
}

impl Can {
    pub fn new(ifname: &str) -> Result<Can, Error> {
        let bus = socketcan::CANSocket::open(ifname)?;

        let can = Can {
            bus
        };

        can.bus.read_frame();
        Ok(can)
    }
}

impl Reader for Can {
    type Item = CanFrame;
    type Error = ();
    
    fn channels(&self) -> Vec<String> {
        vec!["can".to_owned()]
    }
    
    fn next(&mut self) -> Result<Option<(String, Self::Item)>, Self::Error> {
        let frame = self.bus.read_frame().unwrap();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let timestamp = timestamp.as_secs();
        
        let frame = CanFrame {
            timestamp,
            sequence: 0,
            can_id: frame.id().to_string(),
            data: hex::encode(frame.data()),
            dev: "can0".to_owned()
        };

        let channel = "can".to_owned();

        let out = Some((channel, frame));
        Ok(out)
    }
}

impl Batch for Buffer<CanFrame> {
    fn channel(&self) -> String {
        "can".to_owned()
    }
    
    fn serialize(&self) -> Vec<u8> {
        let out = serde_json::to_vec(&self.buffer).unwrap();
        out
    }
}