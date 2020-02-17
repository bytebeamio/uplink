use std::collections::HashMap;
use std::io;
use std::time::SystemTimeError;

use super::{ActionStatus, Config, Packable};
use derive_more::From;

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    Json(serde_json::Error),
    Send(crossbeam_channel::SendError<Box<dyn Packable + Send>>),
    Time(SystemTimeError),
    InvalidChannel(String),
    Busy,
}

/// Signal to modify the behaviour of collector
pub enum Control {
    Shutdown,
    StopChannel(String),
    StartChannel(String),
}

/// Actions should be able to following struff
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
    config: Config,
    // collector tx to send action status to serializer. This is also cloned to spawn
    // a new collector
    collector_tx: crossbeam_channel::Sender<Box<dyn Packable + Send>>,
    // controller_tx per collector
    collector_controllers: HashMap<String, crossbeam_channel::Sender<Control>>,
    // collector running status. Used to spawn a new collector thread based on current
    // run status
    collector_run_status: HashMap<String, bool>,
}

impl Controller {
    pub fn new(
        config: Config,
        controllers: HashMap<String, crossbeam_channel::Sender<Control>>,
        collector_tx: crossbeam_channel::Sender<Box<dyn Packable + Send>>,
    ) -> Self {
        let controller =
            Controller { config, collector_tx, collector_controllers: controllers, collector_run_status: HashMap::new() };

        controller
    }

    pub fn execute(&mut self, id: &str, command: String, _payload: String) -> Result<(), Error> {
        let mut args = vec!["simulator".to_owned(), "can".to_owned()];
        match command.as_ref() {
            "stop_collector_channel" => {
                let collector_name = args.remove(0);
                let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
                for channel in args.into_iter() {
                    controller_tx.send(Control::StopChannel(channel)).unwrap();
                }
                let action_status = ActionStatus::new(id, "running")?;
                self.collector_tx.send(Box::new(action_status)).unwrap();
            }
            "start_collector_channel" => {
                let collector_name = args.remove(0);
                let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
                for channel in args.into_iter() {
                    controller_tx.send(Control::StartChannel(channel)).unwrap();
                }
                let action_status = ActionStatus::new(id, "running")?;
                self.collector_tx.send(Box::new(action_status)).unwrap();
            }
            "stop_collector" => {
                let collector_name = args.remove(0);
                if let Some(running) = self.collector_run_status.get_mut(&collector_name) {
                    if *running {
                        let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
                        controller_tx.send(Control::Shutdown).unwrap();
                        // there is no way of knowing if collector thread is actually shutdown. so
                        // tihs flag is an optimistic assignment. But UI should only enable next
                        // control action based on action status from the controller
                        *running = false;
                        let action_status = ActionStatus::new(id, "running")?;
                        self.collector_tx.send(Box::new(action_status)).unwrap();
                    }
                }
            }
            "start_collector" => {
                let collector_name = args.remove(0);
                if let Some(running) = self.collector_run_status.get_mut(&collector_name) {
                    if !*running {
                        let action_status = ActionStatus::new(id, "done")?;
                        self.collector_tx.send(Box::new(action_status)).unwrap();
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
