use std::{collections::HashMap, string::FromUtf8Error};

use rumqttc::{read, Packet};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tabled::{Table, Tabled};

#[derive(StructOpt, Debug)]
#[structopt(name = "simulator", about = "simulates a demo device")]
pub struct CommandLine {
    /// max packet size
    #[structopt(short = "s", help = "max packet size")]
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

struct Stream {
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

#[derive(Tabled)]
struct Entry {
    stream_name: String,
    serialization_format: String,
    count: usize,
    data_size: usize,
    data_rate: f32,
    start_timestamp: u64,
    end_timestamp: u64,
}

impl Entry {
    fn new(topic: &str, stream: &Stream) -> Self {
        let tokens: Vec<&str> = topic.split("/").collect();
        let stream_name = tokens[6].to_string();
        let serialization_format = tokens[7].to_string();
        let data_rate = (stream.count * 1000) as f32 / (stream.end - stream.start) as f32;

        Self {
            stream_name,
            serialization_format,
            count: stream.count,
            data_size: stream.size,
            data_rate,
            start_timestamp: stream.start,
            end_timestamp: stream.end,
        }
    }
}

fn main() -> Result<(), Error> {
    let commandline: CommandLine = StructOpt::from_args();
    // NOTE: max_file_size and max_file_count should not matter when reading non-destructively
    let mut storage = disk::Storage::new(commandline.directory, 1048576, 3)?;
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

    let mut entries = vec![];
    for (topic, stream) in streams.iter() {
        let entry = Entry::new(topic, stream);
        entries.push(entry);
    }

    let table = Table::new(entries);
    println!("{}", table);

    Ok(())
}
