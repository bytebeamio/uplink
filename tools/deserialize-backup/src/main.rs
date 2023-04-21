use std::{collections::HashMap, string::FromUtf8Error};

use rumqttc::{read, Packet};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "simulator", about = "simulates a demo device")]
pub struct CommandLine {
    /// max file size
    #[structopt(short = "s", help = "max file size")]
    pub max_file_size: usize,
    /// max file count
    #[structopt(short = "c", help = "max file count")]
    pub max_file_count: usize,
    /// max packet size
    #[structopt(short = "p", help = "max packet size")]
    pub max_packet_size: usize,
    /// backup directory
    #[structopt(short = "d", help = "backup directory")]
    pub directory: String,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Disk error {0}")]
    Disk(#[from] disk::Error),
    #[error("From UTF8 error {0}")]
    FromUtf8(#[from] FromUtf8Error),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    timestamp: u64,
}

pub struct Stream {
    count: usize,
    size: usize,
    start: u64,
    end: u64,
}

impl Default for Stream {
    fn default() -> Self {
        Self { count: 0, size: 0, start: u64::MAX, end: 0 }
    }
}

fn main() -> Result<(), Error> {
    let commandline: CommandLine = StructOpt::from_args();
    let mut storage = disk::Storage::new(
        commandline.directory,
        commandline.max_file_size,
        commandline.max_file_count,
    )?;
    storage.non_destructive_read = true;

    let mut streams: HashMap<String, Stream> = HashMap::new();

    'outer: loop {
        loop {
            match storage.reload_on_eof() {
                // Done reading all the pending files
                Ok(true) => break 'outer,
                Ok(false) => break,
                // Reload again on encountering a corrupted file
                Err(e) => {
                    eprintln!("Failed to reload from storage. Error = {e}");
                    continue;
                }
            }
        }

        let publish = match read(storage.reader(), commandline.max_packet_size) {
            Ok(Packet::Publish(publish)) => publish,
            Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
            Err(e) => {
                eprintln!("Failed to read from storage. Error = {:?}", e);
                break;
            }
        };
        let stream = streams.entry(publish.topic.to_string()).or_insert_with(|| Stream::default());
        stream.size += publish.payload.len();

        let payloads: Vec<Payload> = serde_json::from_slice(&publish.payload)?;
        for payload in payloads {
            let timestamp = payload.timestamp;
            stream.count += 1;
            if stream.start > timestamp {
                stream.start = timestamp
            }

            if stream.end < timestamp {
                stream.end = timestamp
            }
        }
    }

    println!("topic: count, size(bytes), rate(/second) [start, end]");
    for (stream, Stream { count, size, start, end }) in streams {
        let rate = (count * 1000) as f32 / (end - start) as f32;
        println!("{}: {}, {}, {} [{}, {}]", stream, count, size, rate, start, end);
    }

    Ok(())
}
