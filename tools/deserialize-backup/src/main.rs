use std::{collections::HashMap, io::Read, string::FromUtf8Error};

use bytes::Bytes;
use human_bytes::human_bytes;
use lz4_flex::frame::FrameDecoder;
use rumqttc::{read, Packet};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tabled::{
    settings::{locator::ByColumnName, Disable, Style},
    Table, Tabled,
};

#[derive(StructOpt, Debug)]
#[structopt(name = "simulator", about = "simulates a demo device")]
pub struct CommandLine {
    /// max packet size, defaults to 256MB
    #[structopt(short = "s", default_value = "268435456", help = "max packet size")]
    pub max_packet_size: usize,
    /// backup directory
    #[structopt(short = "d", help = "backup directory")]
    pub directory: String,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Disk error {0}")]
    Disk(#[from] storage::Error),
    #[error("From UTF8 error {0}")]
    FromUtf8(#[from] FromUtf8Error),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    timestamp: u64,
}

#[derive(Tabled)]
struct Stream {
    count: usize,
    size: usize,
    uncompressed_size: usize,
    start: u64,
    end: u64,
    compression_algo: String,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            count: 0,
            size: 0,
            uncompressed_size: 0,
            start: u64::MAX,
            end: 0,
            compression_algo: "".to_string(),
        }
    }
}

#[derive(Tabled)]
struct Entry {
    stream_name: String,
    serialization_format: String,
    compression_algo: String,
    count: usize,
    message_rate: String,
    data_size: String,
    uncompressed_size: String,
    data_rate: String,
    uncompressed_data_rate: String,
    start_timestamp: u64,
    end_timestamp: u64,
    milliseconds: i64,
}

impl Entry {
    fn new(stream_name: String, stream: Stream) -> Self {
        let milliseconds = stream.end as i64 - stream.start as i64;
        let message_rate = format!("{} /s", (stream.count * 1000) as f32 / milliseconds as f32);
        let data_size = human_bytes(stream.size as f64);
        let uncompressed_size = human_bytes(stream.uncompressed_size as f64);
        let data_rate = human_bytes((stream.size * 1000) as f32 / milliseconds as f32) + "/s";
        let uncompressed_data_rate =
            human_bytes((stream.uncompressed_size * 1000) as f32 / milliseconds as f32) + "/s";

        Self {
            stream_name,
            serialization_format: "jsonarray".to_string(),
            count: stream.count,
            compression_algo: stream.compression_algo,
            message_rate,
            data_size,
            uncompressed_size,
            uncompressed_data_rate,
            data_rate,
            start_timestamp: stream.start,
            end_timestamp: stream.end,
            milliseconds,
        }
    }
}

fn main() -> Result<(), Error> {
    let commandline: CommandLine = StructOpt::from_args();

    let mut streams: HashMap<String, Stream> = HashMap::new();
    let mut total = Stream::default();
    let mut entries: Vec<Entry> = vec![];

    let dirs = std::fs::read_dir(commandline.directory)?;
    for dir in dirs {
        let dir = dir?;
        if dir.metadata()?.is_file() {
            continue;
        }

        let path = dir.path();
        let stream_name = dir.path().into_iter().last().unwrap().to_string_lossy().to_string();
        // NOTE: max_file_size and max_file_count should not matter when reading non-destructively
        let mut storage = storage::Storage::new(&stream_name, 1048576);
        storage.set_persistence(path, 3)?;
        storage.set_non_destructive_read(true);

        let stream = streams.entry(stream_name).or_default();
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

            let mut publish = match read(storage.reader(), commandline.max_packet_size) {
                Ok(Packet::Publish(publish)) => publish,
                Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
                Err(e) => {
                    eprintln!("Failed to read from storage. Error = {:?}", e);
                    break;
                }
            };
            stream.size += publish.payload.len();

            if publish.topic.ends_with("lz4") {
                stream.compression_algo = "lz4".to_owned();
                let mut decompressor = FrameDecoder::new(&*publish.payload);
                let mut bytes = vec![];
                decompressor.read_to_end(&mut bytes)?;
                publish.payload = Bytes::from(bytes);
            }

            stream.uncompressed_size += publish.payload.len();

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
    }

    if streams.is_empty() {
        eprintln!("Error: No data in persistence");

        return Ok(());
    }

    for (topic, stream) in streams {
        total.count += stream.count;
        total.size += stream.size;

        if total.start > stream.start {
            total.start = stream.start;
        }

        if total.end < stream.end {
            total.end = stream.end;
        }
        let entry = Entry::new(topic, stream);
        entries.push(entry);
    }

    let mut table = Table::new(entries);
    table.with(Style::rounded());
    println!("{}", table);
    println!("NOTE: timestamps are relative to UNIX epoch and in milliseconds and message_rate is in units of points/second");

    println!("\nAggregated values (Network Impact)");
    let mut table = Table::new(vec![Entry::new("".to_string(), total)]);
    table
        .with(Style::rounded())
        .with(Disable::column(ByColumnName::new("stream_name")))
        .with(Disable::column(ByColumnName::new("serialization_format")))
        .with(Disable::column(ByColumnName::new("compression_algo")))
        .with(Disable::column(ByColumnName::new("uncompressed_size")))
        .with(Disable::column(ByColumnName::new("uncompressed_data_rate")));
    println!("{}", table);

    Ok(())
}
