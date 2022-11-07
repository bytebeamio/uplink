use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use flume::{Receiver, Sender, TrySendError};
use log::{debug, error};
use tokio::select;
use tokio::time::{sleep, Sleep};

mod actions;
pub mod logcat;
pub mod ota;
mod process;
pub mod tunshell;
mod util;

use crate::base::{Stream, StreamStatus};
use crate::{Config, Package, Payload, Point};
pub use actions::{Action, ActionResponse};
use logcat::{LogLevel, LogcatConfig, LogcatInstance};
use process::Process;
use util::DelayMap;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Process error {0}")]
    Process(#[from] process::Error),
    #[error("Error sending keys to tunshell thread {0}")]
    TunshellSend(#[from] flume::SendError<Action>),
    #[error("Error forwarding Action {0}")]
    TrySend(#[from] flume::TrySendError<Action>),
    #[error("Invalid action")]
    InvalidActionKind(String),
    #[error("Another Action is being processed by collector")]
    CollectorOccupied,
}

pub struct Middleware {
    config: Arc<Config>,
    action_status_rx: Receiver<ActionResponse>,
    action_status_tx: Sender<ActionResponse>,
    process: Process,
    actions_rx: Receiver<Action>,
    action_fwd: HashMap<String, Sender<Action>>,
    collector_data_tx: Sender<Box<dyn Package>>,
    bridge_data_rx: Receiver<Payload>,
    logcat: Option<LogcatInstance>,
}

impl Middleware {
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        action_status_rx: Receiver<ActionResponse>,
        action_status_tx: Sender<ActionResponse>,
        action_fwd: HashMap<String, Sender<Action>>,
        collector_data_tx: Sender<Box<dyn Package>>,
        bridge_data_rx: Receiver<Payload>,
    ) -> Self {
        let process = Process::new(action_status_tx.clone());
        Self {
            config,
            action_status_rx,
            action_status_tx,
            process,
            actions_rx,
            action_fwd,
            collector_data_tx,
            bridge_data_rx,
            logcat: None,
        }
    }

    fn create_log_stream(&self) -> Stream<Payload> {
        Stream::dynamic_with_size(
            "logs",
            &self.config.project_id,
            &self.config.device_id,
            32,
            self.collector_data_tx.clone(),
        )
    }

    /// Start receiving and processing [Action]s
    pub async fn start(mut self) {
        let mut end = Box::pin(sleep(Duration::from_secs(u64::MAX)));

        struct CurrentAction {
            id: String,
            timeout: Pin<Box<Sleep>>,
        }
        // - set to None when
        // -- timeout ends
        // -- A response with status "Completed" is received
        // - set to a value when
        // -- it is currently None and a new action is received
        // - timeout is updated
        // -- when a non "Completed" action is received
        let mut current_action_: Option<CurrentAction> = None;

        let mut flush_handler = DelayMap::new();
        let action_status_topic = self
            .config
            .action_status
            .topic
            .as_ref()
            .expect("Action status topic missing from config");
        let mut action_status =
            Stream::new("action_status", action_status_topic, 1, self.collector_data_tx.clone());

        let mut bridge_partitions = HashMap::new();
        for (name, config) in &self.config.streams {
            let stream = Stream::with_config(
                name,
                &self.config.project_id,
                &self.config.device_id,
                config,
                self.collector_data_tx.clone(),
            );
            bridge_partitions.insert(name.to_owned(), stream);
        }

        if self.config.run_logcat {
            debug!("starting logcat");
            self.logcat = Some(LogcatInstance::new(
                self.create_log_stream(),
                &LogcatConfig { tags: vec!["*".to_string()], min_level: LogLevel::Debug },
            ));
        }

        loop {
            select! {
                action = self.actions_rx.recv_async() => {
                    let action = match action {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Action stream receiver error = {:?}", e);
                            break;
                        }
                    };

                    debug!("Action = {:?}", action);

                    let action_id = action.action_id.clone();
                    let action_name = action.name.clone();
                    let error = match self.handle(action).await {
                        Ok(_) => continue,
                        Err(e) => e,
                    };

                    self.forward_action_error(&action_id, &action_name, error).await;
                }

                status = self.action_status_rx.recv_async() => {
                    let status = status.expect("Action status channel closed");

                    match &current_action_ {
                        Some(action) => {
                            if action.id == status.id {
                                if status.state == "Completed"{
                                    current_action_ = None;
                                } else {
                                    current_action_.as_mut().unwrap().timeout = Box::pin(sleep(Duration::from_secs(10)));
                                }
                            } else {
                                error!("action_id in action_status({}) does not match that of active action ({})", status.id, action.id);
                                continue;
                            }
                    }
                     None => {
                        error!("Action timed out already, ignoring response: {:?}", status);
                        continue;
                    }
                }

                    if let Err(e) = action_status.fill(status).await {
                        error!("Failed to forward action status. Error = {:?}", e);
                    }
                }

                data = self.bridge_data_rx.recv_async() => {
                    let data = data.expect("data channel closed");

                    let stream = match bridge_partitions.get_mut(data.stream()) {
                        Some(partition) => partition,
                        None => {
                            if bridge_partitions.keys().len() > 20 {
                                error!("Failed to create {:?} stream. More than max 20 streams", data.stream());
                                continue
                            }

                            let stream = Stream::dynamic(data.stream(), &self.config.project_id, &self.config.device_id, self.collector_data_tx.clone());
                            bridge_partitions.entry(data.stream().to_owned()).or_insert(stream)
                        }
                    };

                    let max_stream_size = stream.max_buffer_size;
                    let state = match stream.fill(data).await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to send data. Error = {:?}", e.to_string());
                            continue
                        }
                    };

                    // Remove timeout from flush_handler for selected stream if stream state is flushed,
                    // do nothing if stream state is partial. Insert a new timeout if initial fill.
                    // Warn in case stream flushed stream was not in the queue.
                    if max_stream_size > 1 {
                        match state {
                            StreamStatus::Flushed(name) => flush_handler.remove(name),
                            StreamStatus::Init(name, flush_period) => flush_handler.insert(name, flush_period),
                            StreamStatus::Partial(l) => {
                                debug!("Stream contains {} elements", l);
                            }
                        }
                    }
                }

                _ = &mut current_action_.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                    let action = current_action_.take().unwrap();
                    error!("Timeout waiting for action response. Action ID = {}", action.id);

                    // Send failure response to cloud
                    let status = ActionResponse::failure(&action.id, "Action timed out");
                    if let Err(e) = self.action_status_tx.send_async(status).await {
                        error!("Failed to fill. Error = {:?}", e);
                    }
                }

                // Flush stream/partitions that timeout
                Some(stream) = flush_handler.next(), if !flush_handler.is_empty() => {
                    let stream = bridge_partitions.get_mut(&stream).unwrap();
                    if let Err(e) = stream.flush().await{
                        error!("Failed to send data. Error = {:?}", e.to_string());
                        continue
                    }
                }
            }
        }
    }

    /// Handle received actions
    async fn handle(&mut self, action: Action) -> Result<(), Error> {
        match action.name.as_ref() {
            "launch_shell" => {
                self.action_fwd.get("launch_shell").unwrap().send_async(action).await?;
                return Ok(());
            }
            "configure_logcat" => {
                match serde_json::from_str::<LogcatConfig>(action.payload.as_str()) {
                    Ok(mut logcat_config) => {
                        logcat_config.tags =
                            logcat_config.tags.into_iter().filter(|tag| !tag.is_empty()).collect();
                        log::info!("restarting logcat with following config: {:?}", logcat_config);
                        self.logcat =
                            Some(LogcatInstance::new(self.create_log_stream(), &logcat_config))
                    }
                    Err(e) => {
                        error!("couldn't parse logcat config payload:\n{}\n{}", action.payload, e)
                    }
                }
            }
            "update_firmware" if self.config.ota.enabled => {
                // if action can't be sent, Error out and notify cloud
                self.action_fwd.get("update_firmware").unwrap().try_send(action).map_err(|e| {
                    match e {
                        TrySendError::Full(_) => Error::CollectorOccupied,
                        e => Error::TrySend(e),
                    }
                })?;
                return Ok(());
            }

            // Bridge actions are forwarded
            name => {
                if let Some(tx) = self.action_fwd.get(name) {
                    tx.try_send(action)?;
                    return Ok(());
                }
            }
        }

        // Regular actions are executed natively
        match action.kind.as_ref() {
            "process" => {
                let command = action.name.clone();
                let payload = action.payload.clone();
                let id = action.action_id;

                self.process.execute(id.clone(), command.clone(), payload).await?;
            }
            v => return Err(Error::InvalidActionKind(v.to_owned())),
        }

        Ok(())
    }

    async fn forward_action_error(&mut self, id: &str, action: &str, error: Error) {
        error!("Failed to execute. Command = {:?}, Error = {:?}", action, error);
        let status = ActionResponse::failure(id, error.to_string());

        if let Err(e) = self.action_status_tx.send_async(status).await {
            error!("Failed to send status. Error = {:?}", e);
        }
    }
}
