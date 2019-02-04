use std::net::AddrParseError;
use std::io;
use rumqtt;

#[derive(Debug, From)]
pub enum CollectorError {
    Parse(AddrParseError),
    Io(io::Error),
    Channel
}

#[derive(Debug, From)]
pub enum PublisherError {
    Io(io::Error),
    RumqttConnect(rumqtt::ConnectError),
    RumqttClinet(rumqtt::ClientError)
}
