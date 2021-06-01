use async_channel::Receiver;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

pub struct Relay {
    host: String,
    tls_port: u16,
    ws_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    encryption: String
}

pub struct TunshellSession {
    relay: Relay,
    echo_stdout: bool,
    keys_rx: Receiver<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Tunshell error {0}")]
    Tunshell(#[from] anyhow::Error),
}

impl TunshellSession {
    pub fn new(relay: Relay, echo_stdout: bool, tunshell_keys_rx: Receiver<String>) -> Self {
        Self { relay, echo_stdout, keys_rx: tunshell_keys_rx }
    }

    fn config(&self, keys: Keys) -> Config {
        Config::new(
            ClientMode::Target,
            &keys.session,
            &self.relay.host,
            self.relay.tls_port,
            self.relay.ws_port,
            &keys.encryption,
            true,
            self.echo_stdout,
        )
    }

    pub async fn start(self) -> Result<(), Error> {
        while let Ok(keys) = self.keys_rx.recv().await {
            let keys = serde_json::from_str(&keys)?;
            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());
            client.start_session().compat().await?;
        }
        Ok(())
    }
}

impl Default for Relay {
    fn default() -> Self {
        Relay { host: "eu.relay.tunshell.com".to_string(), tls_port: 5000, ws_port: 443 }
    }
}
