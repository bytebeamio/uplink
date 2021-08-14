use std::collections::HashMap;
use std::io;
use std::time::SystemTimeError;

use super::{ActionResponse, Control, Package};
use crate::base::{self, Stream};
use async_channel::{SendError, Sender, TrySendError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Base error {0}")]
    Base(#[from] base::Error),
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Json error {0}")]
    Json(#[from] serde_json::Error),
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
    #[error("Try send error {0}")]
    TrySend(#[from] TrySendError<Control>),
    #[error("Time error {0}")]
    Time(#[from] SystemTimeError),
}

/// Actions should be able to do the following
/// 1. Send device state to cloud. This can be part of device state. Device state informs state of
///    actions
///
/// 2. Receive predefined commands like reboot, deactivate a channel or all channels (hence the
///    collector thread. Activate a channel or all channels (restart the collector thread)
///
/// 3. Get configuration from the cloud and act on it. Enabling and disabling channels can also be
///    part of this
///
/// 4. Ability to get a predefined list of files from cloud. Is this http? Can we do this via mqtt
///    itself?
///
/// 5. Receive OTA update file and perform OTA
///
/// Device State
/// {
///     config_version: "v1.0"
///     errors: "101, 102, 103",
///     last_action: "ota"
///     action_start_time: "12345.123"
///     action_state: "in_progress"
/// }
pub struct Controller {
    // Storage to send action status to serializer. This is also cloned to spawn
    // a new collector
    action_status: Stream<ActionResponse>,
    // controller_tx per collector
    collector_controllers: HashMap<String, Sender<Control>>,
    // collector running status. Used to spawn a new collector thread based on current
    // run status
    collector_run_status: HashMap<String, bool>,
}

impl Controller {
    pub fn new(
        controllers: HashMap<String, Sender<Control>>,
        action_status: Stream<ActionResponse>,
    ) -> Self {
        let controller = Controller {
            collector_controllers: controllers,
            collector_run_status: HashMap::new(),
            action_status,
        };
        controller
    }

    pub async fn execute(&mut self, id: &str, command: String) -> Result<(), Error> {
        // TODO remove all try sends
        let mut args = vec!["simulator".to_owned(), "can".to_owned()];
        match command.as_ref() {
            "stop_collector_channel" => {
                let collector_name = args.remove(0);
                let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
                for channel in args.into_iter() {
                    controller_tx.try_send(Control::StopStream(channel)).unwrap();
                }

                let status = ActionResponse::new(id);
                self.action_status.fill(status).await?;
            }
            "start_collector_channel" => {
                let collector_name = args.remove(0);
                let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
                for channel in args.into_iter() {
                    controller_tx.try_send(Control::StartStream(channel)).unwrap();
                }

                let status = ActionResponse::new(id);
                self.action_status.fill(status).await?;
            }
            "stop_collector" => {
                let collector_name = args.remove(0);
                if let Some(running) = self.collector_run_status.get_mut(&collector_name) {
                    if *running {
                        let controller_tx =
                            self.collector_controllers.get_mut(&collector_name).unwrap();
                        controller_tx.try_send(Control::Shutdown).unwrap();
                        // there is no way of knowing if collector thread is actually shutdown. so
                        // tihs flag is an optimistic assignment. But UI should only enable next
                        // control action based on action status from the controller
                        *running = false;
                        let status = ActionResponse::new(id);
                        self.action_status.fill(status).await?;
                    }
                }
            }
            "start_collector" => {
                let collector_name = args.remove(0);
                if let Some(running) = self.collector_run_status.get_mut(&collector_name) {
                    if !*running {
                        let status = ActionResponse::success(id);
                        self.action_status.fill(status).await?;
                    }
                }

                if let Some(status) = self.collector_run_status.get_mut(&collector_name) {
                    *status = true;
                }
            }
            _ => unimplemented!(),
        }

        Ok(())
    }
}
