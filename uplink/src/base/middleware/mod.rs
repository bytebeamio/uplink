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
use crate::{Config, Package, Payload};
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

struct Streams {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    map: HashMap<String, Stream<Payload>>,
    flush_handler: DelayMap<String>,
}

impl Streams {
    fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>) -> Self {
        let mut map = HashMap::new();
        for (name, stream) in &config.streams {
            let stream = Stream::with_config(
                name,
                &config.project_id,
                &config.device_id,
                stream,
                data_tx.clone(),
            );
            map.insert(name.to_owned(), stream);
        }

        let flush_handler = DelayMap::new();

        Self { config, data_tx, map, flush_handler }
    }

    async fn forward(&mut self, data: Payload) {
        let stream = match self.map.get_mut(&data.stream) {
            Some(partition) => partition,
            None => {
                if self.map.keys().len() > 20 {
                    error!("Failed to create {:?} stream. More than max 20 streams", data.stream);
                    return;
                }

                let stream = Stream::dynamic(
                    &data.stream,
                    &self.config.project_id,
                    &self.config.device_id,
                    self.data_tx.clone(),
                );
                self.map.entry(data.stream.to_owned()).or_insert(stream)
            }
        };

        let max_stream_size = stream.max_buffer_size;
        let state = match stream.fill(data).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to send data. Error = {:?}", e.to_string());
                return;
            }
        };

        // Remove timeout from flush_handler for selected stream if stream state is flushed,
        // do nothing if stream state is partial. Insert a new timeout if initial fill.
        // Warn in case stream flushed stream was not in the queue.
        if max_stream_size > 1 {
            match state {
                StreamStatus::Flushed(name) => self.flush_handler.remove(name),
                StreamStatus::Init(name, flush_period) => {
                    self.flush_handler.insert(name, flush_period)
                }
                StreamStatus::Partial(l) => {
                    debug!("Stream contains {} elements", l);
                }
            }
        }
    }

    fn is_flushable(&self) -> bool {
        !self.flush_handler.is_empty()
    }

    // Flush stream/partitions that timeout
    async fn flush(&mut self) {
        let stream = self.flush_handler.next().await.unwrap();
        let stream = self.map.get_mut(&stream).unwrap();
        if let Err(e) = stream.flush().await {
            error!("Failed to send data. Error = {:?}", e.to_string());
            return;
        }
    }
}

struct CurrentAction {
    id: String,
    timeout: Pin<Box<Sleep>>,
}

impl CurrentAction {
    fn start(id: String) -> Option<Self> {
        Some(Self { id, timeout: Box::pin(sleep(Duration::from_secs(u64::MAX))) })
    }

    // When a non "Completed" status for current action is received, the timeout is updated,
    // returns false only if status is not for current action
    fn update(&mut self, status: &ActionResponse) -> bool {
        if self.id != status.id {
            return false;
        }

        if status.state != "Completed" {
            self.timeout = Box::pin(sleep(Duration::from_secs(10)));
        }

        true
    }
}

pub struct Middleware {
    config: Arc<Config>,
    process: Process,
    actions_rx: Receiver<Action>,
    action_fwd: HashMap<String, Sender<Action>>,
    collector_data_tx: Sender<Box<dyn Package>>,
    bridge_data_rx: Receiver<Payload>,
    logcat: Option<LogcatInstance>,
    // - set to None when
    // -- timeout ends
    // -- A response with status "Completed" is received
    // - set to a Some value when
    // -- it is currently None and a new action is received
    current_action: Option<CurrentAction>,
}

impl Middleware {
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        action_fwd: HashMap<String, Sender<Action>>,
        collector_data_tx: Sender<Box<dyn Package>>,
        bridge_data_tx: Sender<Payload>,
        bridge_data_rx: Receiver<Payload>,
    ) -> Self {
        let process = Process::new(bridge_data_tx);
        Self {
            config,
            process,
            actions_rx,
            action_fwd,
            collector_data_tx,
            bridge_data_rx,
            logcat: None,
            current_action: None,
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

        let action_status_topic = self
            .config
            .action_status
            .topic
            .as_ref()
            .expect("Action status topic missing from config");
        let mut action_status =
            Stream::new("action_status", action_status_topic, 1, self.collector_data_tx.clone());

        let mut streams = Streams::new(self.config.clone(), self.collector_data_tx.clone());

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

                    if let Err(error) = self.handle(action).await {
                        error!("Failed to execute. Command = {:?}, Error = {:?}", action_name, error);
                        let status = ActionResponse::failure(&action_id, error.to_string());

                        if let Err(e) = action_status.fill(status).await {
                            error!("Failed to send status. Error = {:?}", e);
                        }
                    }
                }

                data = self.bridge_data_rx.recv_async() => {
                    let data = data.expect("data channel closed");

                    // If incoming data is not a response for an action forward to appropriate stream,
                    // if it is, drop it if the timeout is already sent to cloud, else update timeout
                    if data.stream != "action_status" {
                        streams.forward(data).await;
                        continue;
                    }

                    let status = ActionResponse::from(data);
                    if self.current_action.is_none() {
                        error!("Action timed out already, ignoring response: {:?}", status);
                        continue;
                    }

                    let action = self.current_action.as_mut().unwrap();
                    if !action.update(&status) {
                        error!(
                            "action_id in action_status({}) does not match that of active action ({})",
                            status.id, action.id
                        );
                        continue;
                    }

                    if let Err(e) = action_status.fill(status).await {
                        error!("Failed to forward action status. Error = {:?}", e);
                    }
                }

                _ = &mut self.current_action.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                    let action = self.current_action.take().unwrap();
                    error!("Timeout waiting for action response. Action ID = {}", action.id);

                    // Send failure response to cloud
                    let status = ActionResponse::failure(&action.id, "Action timed out");
                    if let Err(e) = action_status.fill(status).await {
                        error!("Failed to fill. Error = {:?}", e);
                    }
                }

                _ = streams.flush(), if streams.is_flushable() => {}
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

            // Bridge actions are forwarded to bridge if it isn't currently occupied
            name => {
                if self.current_action.is_some() {
                    error!(
                        "Action: {} still in execution",
                        self.current_action.as_ref().unwrap().id
                    );
                    return Ok(());
                }

                if let Some(tx) = self.action_fwd.get(name) {
                    self.current_action = CurrentAction::start(action.action_id.clone());
                    tx.try_send(action)?;
                    return Ok(());
                }

                error!("Action: {} handler not found", name)
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
}
