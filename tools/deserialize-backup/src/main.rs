use std::{collections::HashMap, string::FromUtf8Error};

use human_bytes::human_bytes;
use rumqttc::{read, Packet};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tabled::{settings::Style, Table, Tabled};

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
    message_rate: String,
    data_size: String,
    data_rate: String,
    start_timestamp: u64,
    end_timestamp: u64,
    milliseconds: u64,
}

impl Entry {
    fn new(topic: &str, stream: &Stream) -> Self {
        let tokens: Vec<&str> = topic.split("/").collect();
        let stream_name = tokens[6].to_string();
        let serialization_format = tokens[7].to_string();
        let milliseconds = stream.end - stream.start;
        let message_rate = format!("{} /s", (stream.count * 1000) as f32 / milliseconds as f32);
        let data_size = human_bytes(stream.size as f64);
        let data_rate = human_bytes((stream.size * 1000) as f32 / milliseconds as f32) + "/s";

        Self {
            stream_name,
            serialization_format,
            count: stream.count,
            message_rate,
            data_size,
            data_rate,
            start_timestamp: stream.start,
            end_timestamp: stream.end,
            milliseconds,
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

    let mut total = Stream::default();
    let mut entries: Vec<Entry> = vec![];
    for (topic, stream) in streams.iter() {
        let entry = Entry::new(topic, stream);
        total.count += stream.count;
        total.size += stream.size;

        if total.start > stream.start {
            total.start = stream.start;
        }

        if total.end < stream.end {
            total.end = stream.end;
        }
        entries.push(entry);
    }

    let mut table = Table::new(entries);
    table.with(Style::rounded());
    println!("{}", table);
    println!("NOTE: timestamps are relative to UNIX epoch and in milliseconds and message_rate is in units of points/second");

    println!("\nAggregated values");
    let mut table = Table::new(vec![Entry::new("//////total/jsonarray", &total)]);
    table.with(Style::rounded());
    println!("{}", table);

    Ok(())
}
