use bytes::{Buf, BytesMut};
use rumqttc::Packet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn main() {
    let args: CliArgs = structopt::StructOpt::from_args();
    let input_md = fs::metadata(args.input.as_path());

    match input_md {
        Ok(md) => {
            let mut result = Vec::<OutputPayload>::new();
            if md.is_file() {
                result.extend(extract_messages(args.input.as_path()).into_iter());
            } else if md.is_dir() {
                for child in fs::read_dir(args.input.as_path()).unwrap() {
                    match child {
                        Ok(child) => {
                            if child.file_name().to_str().unwrap().starts_with("backup@") {
                                result.extend(extract_messages(child.path().as_path()).into_iter());
                            }
                        }
                        Err(e) => {
                            println!("error: {e:?}");
                        }
                    }
                }
            } else {
                println!("error: symlinks aren't supported");
                exit(1);
            }
            fs::write(args.output_file.as_path(), serde_json::to_string_pretty(&result).unwrap()).unwrap()
        }
        Err(e) => {
            println!("error: can't access input directory: {e}");
            exit(1);
        }
    }
}

fn extract_messages(path: &Path) -> Vec<OutputPayload> {
    println!("processing file: {path:?}");
    let file_content = fs::read(path).unwrap();
    let mut buf = BytesMut::from(file_content.as_slice());
    let mut result = vec![];
    if buf.len() < 8 {
        println!("== error: this file is too small, ignoring");
    } else {
        let expected_hash = buf.get_u64();
        let actual_hash = seahash::hash(&buf[..]);
        if actual_hash != expected_hash {
            println!("== error: checksum doesn't match, ignoring");
        } else {
            match Packet::read(&mut buf, 1_000_000) {
                Ok(Packet::Publish(packet)) => {
                    let stream = if packet.topic.ends_with("/action/status") {
                        "action_status".to_owned()
                    } else {
                        packet.topic.split("/").nth(6).unwrap().to_owned()
                    };
                    unsafe {
                        match serde_json::from_str::<Vec<StoredPayload>>(std::str::from_utf8_unchecked(
                            packet.payload.iter().as_slice(),
                        )) {
                            Ok(messages) => {
                                for message in messages {
                                    result.push(OutputPayload {
                                        stream: stream.to_owned(),
                                        sequence: message.sequence,
                                        timestamp: message.timestamp,
                                        payload: message.payload,
                                    });
                                }
                            }
                            Err(e) => {
                                println!("== error: couldn't read a packet from this file: {e:?}");
                            }
                        }
                    }
                }
                Ok(_p) => {
                    println!("== error: found unsupported packet type, ignoring");
                }
                Err(e) => {
                    println!("== error: couldn't read packet from file: {e:?}");
                }
            }
        }
    }
    result
}

#[derive(Deserialize)]
struct StoredPayload {
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Serialize)]
struct OutputPayload {
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(structopt::StructOpt)]
struct CliArgs {
    input: PathBuf,
    output_file: PathBuf,
}
