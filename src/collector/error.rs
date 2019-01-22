use std::net::AddrParseError;
use std::io;

#[derive(From)]
pub enum CollectorError {
    Parse(AddrParseError),
    Io(io::Error),
    Channel
}
