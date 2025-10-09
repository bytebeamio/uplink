use std::time::Duration;
use flume::{Receiver, Sender};
use futures::SinkExt;
use log::{debug, error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use crate::core::mqtt::Action;
use crate::DataRow;

pub async fn tcp_client_task(port: u16, data_tx: Sender<DataRow>, actions_rx: Receiver<Action>) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = loop {
        match TcpListener::bind(&addr).await {
            Ok(s) => break s,
            Err(e) => {
                error!(
                    "couldn't bind to port: {}; Error = {e}; retrying in 5s",
                    port
                );
                sleep(Duration::from_secs(5)).await;
            }
        }
    };
    info!("listening on port {port}");
    let mut existing_connection: Option<Framed<TcpStream, LinesCodec>> = None;
    loop {
        select! {
            new_connection = listener.accept() => {
                match new_connection {
                    Ok((stream, _)) => {
                        info!("accepted new connection");
                        if let Some(conn) = existing_connection.take() {
                            warn!("a client was already connected to this port, only one client per port is allowed. closing old connection");
                        }
                        let stream = Framed::new(stream, LinesCodec::new());
                        existing_connection = Some(stream);
                    }
                    Err(e) => {
                        error!("error when awaiting connection: {e:?}");
                        sleep(Duration::from_secs(3)).await;
                    }
                }
            }
            Ok(action) = actions_rx.recv_async(), if !actions_rx.is_disconnected() => {
                let mut error = false;
                if let Some(conn) = existing_connection.as_mut() {
                    if let Err(e) = conn.send(serde_json::to_string(&action).unwrap()).await {
                        error!("couldn't send action to client: {e:?}");
                        error!("closing connection");
                        error = true;
                    }
                }
                existing_connection = None;
            }
            line = async { existing_connection.as_mut().unwrap().next().await }, if existing_connection.is_some() => {
                let ok = match line {
                    Some(Ok(line)) => {
                        debug!("received line = {line:?}");
                        match serde_json::from_str::<DataRow>(&line) {
                            Ok(row) => {
                                let _ = data_tx.send_async(row).await;
                            }
                            Err(e) => {
                                error!("received invalid data from client. line = {line:?}, error = {e:?}");
                            }
                        }
                        true
                    }
                    Some(Err(e)) => {
                        error!("error: {e:?}");
                        false
                    }
                    None => {
                        info!("client closed connection");
                        false
                    }
                };
                if !ok {
                    existing_connection = None;
                }
            }
        }
    }
}
