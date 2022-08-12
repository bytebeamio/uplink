use std::process::exit;

use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rumqttc::{Client, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use tunshell_client::{Client as TunshellClient, ClientMode, Config as TunshellConfig, HostShell};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(version = "0.1.0")]
struct Config {
    #[clap(short, long, default_value = "eu.relay.tunshell.com")]
    relay: String,
    #[clap(short, long, default_value = "0.0.0.0")]
    host: String,
    #[clap(long, default_value = "1883")]
    port: u16,
    #[clap(short, long)]
    device_id: String,
    #[clap(short, long)]
    project_id: String,
}

#[derive(Serialize)]
struct Action {
    id: String,
    kind: String,
    name: String,
    payload: String,
}

#[derive(Serialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}

#[derive(Deserialize)]
struct SessionKeys {
    peer1_key: String,
    peer2_key: String,
}

fn main() {
    env_logger::init();

    // parsing arguments
    let config: Config = Config::parse();
    let relay = "https://".to_string() +  &config.relay + "/api/sessions";
    let publish_topic: String = "/tenants/".to_string() + &config.project_id + "/devices/" + &config.device_id + "/actions";
    let subscribe_topic: String = publish_topic.clone() + "/status";

    // get session keys from relay server
    let SessionKeys {
        peer1_key: self_key,
        peer2_key: target_key,
    } = ureq::post(&relay)
        .call()
        .expect("unable to connect to relay server")
        .into_json()
        .expect("server returned invalid JSON");
    let encrytion_key = generate_encryption_key();

    // open mqtt client
    let mut mqttoptions = MqttOptions::new("tunshell-client", &config.host, config.port);
    mqttoptions.set_keep_alive(60);
    let (mut client, mut eventloop) = Client::new(mqttoptions, 3);
    let action = Action {
        id: "tunshell".to_string(),
        name: "launch_shell".to_string(),
        kind: "launch_shell".to_string(),
        payload: serde_json::to_string(&Keys {
            session: target_key,
            relay: "eu.relay.tunshell.com".to_string(),
            encryption: encrytion_key.clone(),
        })
        .unwrap(),
    };

    std::thread::spawn(move || {
        for (_, notification) in eventloop.iter().enumerate() {
            println!("Notification = {:?}", notification);
        }
    });

    // publish request to  open tunshell
    if let Err(e) = client.publish(
        publish_topic,
        QoS::AtLeastOnce,
        true,
        serde_json::to_string(&action).unwrap(),
    ) {
        println!("{:?}", e);
        exit(1);
    }

    if let Err(e) = client.subscribe(subscribe_topic, QoS::AtLeastOnce) {
        println!("{:?}", e);
        exit(1);
    }

    // initialize tunshell locally
    let config = TunshellConfig::new(
        ClientMode::Local,
        &self_key,
        &config.relay,
        5000,
        443,
        &encrytion_key,
        true,
        false,
    );
    let mut client = TunshellClient::new(config, HostShell::new().unwrap());
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    // start tunshell
    match rt.block_on(client.start_session()) {
        Ok(code) => exit(code as i32),
        Err(e) => {
            println!("Session error = {:?}", e);
            exit(1)
        }
    }
}

fn generate_encryption_key() -> String {
    std::str::from_utf8(
        &thread_rng()
            .sample_iter(&Alphanumeric)
            .take(22)
            .collect::<Vec<u8>>(),
    )
    .unwrap()
    .to_owned()
}
