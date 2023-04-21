use std::string::FromUtf8Error;

use rumqttc::{read, Packet};
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
}

fn main() -> Result<(), Error> {
    let commandline: CommandLine = StructOpt::from_args();
    let mut storage = disk::Storage::new(
        commandline.directory,
        commandline.max_file_size,
        commandline.max_file_count,
    )?;

    loop {
        loop {
            match storage.reload_on_eof() {
                // Done reading all the pending files
                Ok(true) => return Ok(()),
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
                eprintln!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                continue;
            }
        };

        let payload = String::from_utf8(publish.payload.to_vec())?;
        println!("{}", payload)
    }
}
