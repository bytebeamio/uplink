use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use base::{Action, ActionResponse, CollectorRx, CollectorTx, Payload};
use flume::{bounded, Sender};
use log::{error, info};
use rand::Rng;
use tokio::{select, spawn, time::interval};

use self::data::{Bms, DeviceData, DeviceShadow, Gps, Imu, Motor, PeripheralState};

mod data;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Recv error {0}")]
    Recv(#[from] flume::RecvError),
    #[error("Disconnected")]
    Disconnected,
}

pub enum Event {
    Data(Payload),
    ActionResponse(ActionResponse),
}

pub async fn simulate_action(action: Action, tx: Sender<Event>) {
    let action_id = action.action_id;
    info!("Generating action events for action: {action_id}");
    let mut sequence = 0;
    let mut interval = interval(Duration::from_secs(1));

    // Action response, 10% completion per second
    for i in 1..10 {
        let progress = i * 10 + rand::thread_rng().gen_range(0..10);
        sequence += 1;
        let response =
            ActionResponse::progress(&action_id, "in_progress", progress).set_sequence(sequence);
        if let Err(e) = tx.send_async(Event::ActionResponse(response)).await {
            error!("{e}");
            break;
        }

        interval.tick().await;
    }

    sequence += 1;
    let response = ActionResponse::progress(&action_id, "Completed", 100).set_sequence(sequence);
    if let Err(e) = tx.send_async(Event::ActionResponse(response)).await {
        error!("{e}");
    }
    info!("Successfully sent all action responses");
}

pub fn read_gps_path(paths_dir: PathBuf) -> Arc<Vec<Gps>> {
    let i = rand::thread_rng().gen_range(0..10);
    let mut file = paths_dir.clone();
    let file_name = format!("path{}.json", i);
    file.push(file_name);

    let contents = fs::read_to_string(file).expect("Oops, failed ot read path");

    let parsed: Vec<Gps> = serde_json::from_str(&contents).unwrap();

    Arc::new(parsed)
}

pub fn new_device_data(path: Arc<Vec<Gps>>) -> DeviceData {
    let mut rng = rand::thread_rng();

    let path_index = rng.gen_range(0..path.len()) as u32;

    DeviceData { path, path_offset: path_index }
}

pub fn spawn_data_simulators(device: DeviceData, tx: Sender<Event>) {
    spawn(Gps::simulate(tx.clone(), device));
    spawn(Bms::simulate(tx.clone()));
    spawn(Imu::simulate(tx.clone()));
    spawn(Motor::simulate(tx.clone()));
    spawn(PeripheralState::simulate(tx.clone()));
    spawn(DeviceShadow::simulate(tx));
}

pub async fn start(
    gps_paths: PathBuf,
    mut bridge_tx: impl CollectorTx,
    mut actions_rx: Option<impl CollectorRx>,
) -> Result<(), Error> {
    let path = read_gps_path(gps_paths);
    let device = new_device_data(path);

    let (tx, rx) = bounded(10);
    spawn_data_simulators(device, tx.clone());

    loop {
        if let Some(actions_rx) = actions_rx.as_mut() {
            select! {
                action = actions_rx.recv_action() => {
                    let action = action.map_err(|_| Error::Disconnected)?;
                    spawn(simulate_action(action,  tx.clone()));
                }
                event = rx.recv_async() => {
                    match event {
                        Ok(Event::ActionResponse(status)) => bridge_tx.send_action_response(status).await.map_err(|_| Error::Disconnected)?,
                        Ok(Event::Data(payload)) => bridge_tx.send_payload(payload).await.map_err(|_| Error::Disconnected)?,
                        Err(_) => {
                            error!("All generators have stopped!");
                            return Ok(())
                        }
                    };
                }
            }
        } else {
            match rx.recv_async().await {
                Ok(Event::ActionResponse(status)) => {
                    bridge_tx.send_action_response(status).await.map_err(|_| Error::Disconnected)?
                }
                Ok(Event::Data(payload)) => {
                    bridge_tx.send_payload(payload).await.map_err(|_| Error::Disconnected)?
                }
                Err(_) => {
                    error!("All generators have stopped!");
                    return Ok(());
                }
            }
        }
    }
}
