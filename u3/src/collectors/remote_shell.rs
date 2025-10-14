use crate::DataRow;
use flume::{Receiver, Sender};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt};
use serde::Deserialize;
use std::pin::Pin;
use tokio::select;
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};
use crate::core::actions::{send_action_response, Action};

pub async fn remote_shell_task(data_tx: Sender<DataRow>, action_rx: Receiver<Action>) {
    let mut shells = FuturesUnordered::<Pin<Box<dyn Future<Output = ()> + Send>>>::new();
    loop {
        select! {
            Ok(action) = action_rx.recv_async() => {
                shells.push(Box::pin(run_remote_shell(action, data_tx.clone())));
            }
            _ = shells.next(), if !shells.is_empty() => {}
            else => break
        }
    }
}

async fn run_remote_shell(action: Action, data_tx: Sender<DataRow>) {
    send_action_response(&data_tx, &action.id, "ShellSpawned", 90, &[]).await;
    match run_remote_shell_impl(&action).await {
        Ok(_) => {
            send_action_response(&data_tx, &action.id, "Completed", 100, &[]).await;
        }
        Err(e) => {
            send_action_response(&data_tx, &action.id, "Failed", 100, &[e]).await;
        }
    }
}

async fn run_remote_shell_impl(action: &Action) -> Result<(), String> {
    let keys =
        serde_json::from_str::<Keys>(&action.payload).map_err(|_| "invalid action payload!")?;
    let config = Config::new(
        ClientMode::Target,
        &keys.session,
        &keys.relay,
        5000,
        443,
        &keys.encryption,
        true,
        false,
    );
    let mut client = Client::new(config, HostShell::new().unwrap());
    // TODO: update tunshell to use tokio 1.0 and remove printlns in tunshell_client
    let status =
        client.start_session().compat().await.map_err(|e| format!("tunshell-client: {e:?}"))?;
    if status == 0 {
        Ok(())
    } else {
        Err(format!("tunshell-client returned error code({status})!"))
    }
}

#[derive(Debug, Deserialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}
