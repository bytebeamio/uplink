use std::net::SocketAddr;

use rumqttd::{
    local::{LinkRx, LinkTx},
    protocol::Publish,
    Broker, Config, ConnectionSettings, Forward, Notification, RouterConfig, ServerSettings,
};

use crate::{
    base::{bridge::Payload, ServiceBusRx, ServiceBusTx},
    Action,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Link error: {0}")]
    Link(#[from] rumqttd::local::LinkError),
    #[error("Parse error: {0}")]
    Parse(#[from] std::net::AddrParseError),
    #[error("Rumqttd error: {0}")]
    Rumqttd(#[from] rumqttd::Error),
}

pub struct BusRx {
    rx: LinkRx,
}

impl ServiceBusRx<Publish> for BusRx {
    fn recv(&mut self) -> Option<Publish> {
        loop {
            return match self.rx.recv() {
                Ok(Some(Notification::Forward(Forward { publish, .. }))) => Some(publish),
                Err(_) => None,
                _ => continue,
            };
        }
    }
}

pub struct BusTx {
    tx: LinkTx,
}

impl ServiceBusTx for BusTx {
    type Error = Error;

    fn publish_data(&mut self, data: Payload) -> Result<(), Self::Error> {
        let topic = format!("streams/{}", data.stream);
        let payload = serde_json::to_vec(&data)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }

    fn update_action_status(&mut self, status: crate::ActionResponse) -> Result<(), Self::Error> {
        let topic = "streams/action_status".to_owned();
        let payload = serde_json::to_vec(&status)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }

    fn subscribe_to_streams(
        &mut self,
        streams: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), Self::Error> {
        for stream in streams {
            let filter = format!("streams/{}", stream.into());
            self.tx.subscribe(filter)?;
        }

        Ok(())
    }

    fn register_action(&mut self, name: impl Into<String>) -> Result<(), Self::Error> {
        let filter = format!("actions/{}", name.into());
        self.tx.subscribe(filter)?;

        Ok(())
    }

    fn deregister_action(&mut self, name: impl Into<String>) -> Result<(), Self::Error> {
        let filter = format!("actions/{}", name.into());
        self.tx.unsubscribe(filter)?;

        Ok(())
    }

    fn push_action(&mut self, action: Action) -> Result<(), Self::Error> {
        let topic = format!("streams/{}", action.name);
        let payload = serde_json::to_vec(&action)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }
}

pub fn new() -> Result<(BusTx, BusRx), Error> {
    let router = RouterConfig {
        max_segment_size: 1024,
        max_connections: 10,
        max_segment_count: 10,
        max_outgoing_packet_count: 1024,
        ..Default::default()
    };
    let connections = ConnectionSettings {
        connection_timeout_ms: 10000,
        max_payload_size: 1073741824,
        max_inflight_count: 10,
        auth: None,
        external_auth: None,
        dynamic_filters: false,
    };
    let server = ServerSettings {
        name: "service_bus".to_owned(),
        listen: "localhost:1883".parse::<SocketAddr>()?,
        tls: None,
        next_connection_delay_ms: 0,
        connections,
    };
    let servers = [("service_bus".to_owned(), server)].into_iter().collect();
    let config = Config { id: 0, router, v4: Some(servers), ..Default::default() };
    let mut broker = Broker::new(config);
    let (tx, rx) = broker.link("uplink")?;
    broker.start()?;

    Ok((BusTx { tx }, BusRx { rx }))
}
