use std::time::Duration;

use config::{Environment, File, FileFormat};
use log::{error, info};
use rumqttc::{AsyncClient, Event, Incoming, Key, MqttOptions, QoS, TlsConfiguration, Transport};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::task;
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};
use uplink::{Action, ActionResponse};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to deserialize keys. Error = {0}")]
    Serde(#[from] serde_json::Error),
    #[error("TunshellClient client Error = {0}")]
    TunshellClient(#[from] anyhow::Error),
    #[error("TunshellClient exited with unexpected status: {0}")]
    UnexpectedStatus(u8),
    #[error("Couldn't read auth file: {0}")]
    ReadAuth(String),
}

#[derive(Debug, Deserialize)]
pub struct Auth {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<Authentication>,
}

#[derive(Debug, Deserialize)]
pub struct Authentication {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}

impl Keys {
    fn config(&self) -> Config {
        Config::new(
            ClientMode::Target,
            &self.session,
            &self.relay,
            5000,
            443,
            &self.encryption,
            true,
            false,
        )
    }
}

/// Mqtt Client handle
#[derive(Debug, Clone)]
pub struct MqttClient {
    response_topic: String,
    client: AsyncClient,
}

impl MqttClient {
    async fn send_action_response(&self, status: ActionResponse) {
        let payload = match serde_json::to_vec(&vec![status]) {
            Ok(p) => p,
            Err(e) => {
                error!("Error serializing response: {e}");
                return;
            }
        };
        if let Err(e) =
            self.client.publish(&self.response_topic, QoS::AtLeastOnce, false, payload).await
        {
            error!("Error writing to network: {e}")
        }
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let cmd: CommandLine = StructOpt::from_args();
    let auth_config =
        std::fs::read_to_string(&cmd.auth).map_err(|_| Error::ReadAuth(cmd.auth.to_string()))?;
    let auth: Auth = config::Config::builder()
        .add_source(File::from_str(&auth_config, FileFormat::Json))
        .add_source(Environment::default())
        .build()?
        .try_deserialize()?;

    cmd.banner(&auth);
    cmd.initialize_logging();

    let subscription = format!("/tenants/{}/devices/{}/actions", auth.project_id, auth.device_id);
    let response_topic =
        format!("/tenants/{}/devices/{}/action/status", auth.project_id, auth.device_id);

    let mut options = MqttOptions::new(&auth.device_id, &auth.broker, auth.port);
    if let Some(auth) = auth.authentication {
        let ca = auth.ca_certificate.into_bytes();
        let device_certificate = auth.device_certificate.into_bytes();
        let device_private_key = auth.device_private_key.into_bytes();
        let transport = Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: Some((device_certificate, Key::RSA(device_private_key))),
        });

        options.set_transport(transport);
    }

    let (client, mut eventloop) = AsyncClient::new(options, 10);
    let mqtt_client = MqttClient { response_topic, client };

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                info!("Connected to broker. Session present = {}", connack.session_present);
                let client = mqtt_client.client.clone();
                let subscription = subscription.clone();
                task::spawn(async move {
                    match client.subscribe(&subscription, QoS::AtLeastOnce).await {
                        Ok(..) => info!("Subscribe -> {:?}", subscription),
                        Err(e) => error!("Failed to send subscription. Error = {:?}", e),
                    }
                });
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                if subscription != p.topic {
                    error!("Unsolicited publish on {}", p.topic);
                    return Ok(());
                }

                let action: Action = serde_json::from_slice(&p.payload)?;
                // Ignore non-tunshell actions
                if action.name != "launch_shell" {
                    continue;
                }

                info!("Action = {:?}", action);
                let mqtt_client = mqtt_client.clone();
                tokio::spawn(async move {
                    if let Err(e) = session(&mqtt_client, &action).await {
                        error!("{}", e.to_string());
                        let status = ActionResponse::failure(&action.action_id, e.to_string());
                        mqtt_client.send_action_response(status).await;
                    }
                });
            }
            Ok(_) => {}
            Err(e) => {
                error!("{e}");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        }
    }
}

async fn session(mqtt_client: &MqttClient, action: &Action) -> Result<(), Error> {
    let action_id = action.action_id.clone();

    // println!("{:?}", keys);
    let keys: Keys = serde_json::from_str(&action.payload)?;
    let mut client = Client::new(keys.config(), HostShell::new().unwrap());

    let response = ActionResponse::progress(&action_id, "ShellSpawned", 90);
    mqtt_client.send_action_response(response).await;

    let status = client.start_session().compat().await?;
    if status != 0 {
        Err(Error::UnexpectedStatus(status))
    } else {
        log::info!("Tunshell session ended successfully");
        Ok(())
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink-tunshell", about = "remotely shell into your edge devices")]
pub struct CommandLine {
    /// Binary version
    #[structopt(skip = env!("VERGEN_BUILD_SEMVER"))]
    pub version: String,
    /// Build profile
    #[structopt(skip = env!("VERGEN_CARGO_PROFILE"))]
    pub profile: String,
    /// Commit SHA
    #[structopt(skip = env!("VERGEN_GIT_SHA"))]
    pub commit_sha: String,
    /// Commit SHA
    #[structopt(skip = env!("VERGEN_GIT_COMMIT_TIMESTAMP"))]
    pub commit_date: String,
    #[structopt(short = "a", help = "Authentication file")]
    pub auth: String,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
    /// list of modules to log
    #[structopt(short = "m", long = "modules")]
    pub modules: Vec<String>,
}

impl CommandLine {
    fn initialize_logging(&self) {
        let level = match self.verbose {
            0 => "warn",
            1 => "info",
            2 => "debug",
            _ => "trace",
        };

        let levels =
            match self.modules.clone().into_iter().reduce(|e, acc| format!("{e}={level},{acc}")) {
                Some(f) => format!("{f}={level}"),
                _ => format!("tunshell={level}"),
            };
        tracing_subscriber::fmt()
            .pretty()
            .with_line_number(false)
            .with_file(false)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_env_filter(levels)
            .with_filter_reloading()
            .try_init()
            .expect("initialized subscriber succesfully");
    }

    fn banner(&self, auth: &Auth) {
        println!("    version: {}", self.version);
        println!("    profile: {}", self.profile);
        println!("    commit_sha: {}", self.commit_sha);
        println!("    commit_date: {}", self.commit_date);
        println!("    project_id: {}", auth.project_id);
        println!("    device_id: {}", auth.device_id);
        println!("    remote: {}:{}", auth.broker, auth.port);
        println!("\n");
    }
}
