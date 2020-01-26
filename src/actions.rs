use rumq_client::{eventloop, MqttOptions, QoS, Request, Notification};
use tokio::process::Command;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::channel;
use tokio::io::{BufReader, AsyncBufReadExt};
use tokio::time;
use serde::{Serialize, Deserialize};
use derive_more::From;
use super::Config;
use super::collector::{self, Control, Packable};

use std::sync::{Arc, Mutex}; 
use std::thread;
use std::process::Stdio;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, From)]
pub enum Error {
    Serde(serde_json::Error),
    Stream(rumq_client::EventLoopError)
}


#[derive(Debug, Serialize, Deserialize)]
struct Action {
    // action id
    id: String,
    // control or process
    kind: String,
    // action name
    name: String,
    // action payload. json. can be args/payload. depends on the invoked command
    payload: String 
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionStatus {
    id: String,
    // running, failed
    state: String,
    // progress percentage for processes
    progress: String,
    // list of error
    errors: Vec<String>
}


impl ActionStatus {
    pub fn new(id: &str, state: &str) -> Self {
        ActionStatus {
            id: id.to_owned(),
            state: state.to_owned(),
            progress: "0".to_owned(),
            errors: Vec::new()
        }
    }
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
    collector_run_status: HashMap<String, bool>
}

impl Controller {
    pub fn new(config: Config, collector_tx: crossbeam_channel::Sender<Box<dyn Packable + Send>>) -> Self {
        let collectors = vec!["simulator"];

        let mut controller = Controller { 
            config, 
            collector_tx,
            collector_controllers: HashMap::new(),
            collector_run_status: HashMap::new()
        };

        for collector in collectors.iter() {
            controller.spawn_collector(collector);
        }

        controller 
    }

    fn spawn_collector(&mut self, name: &str) {
        let (controller_tx, controller_rx) = crossbeam_channel::bounded(10);
        match name {
            "simulator" => {
                self.collector_controllers.insert(name.to_owned(), controller_tx);
                let collector_tx = self.collector_tx.clone();

                thread::spawn(move || {
                    collector::simulator::start(collector_tx, controller_rx);
                });

                self.collector_run_status.insert(name.to_owned(), true);
            }
            _ => ()
        } 
    }

    /*
       fn execute(&mut self, id: &str, mut command: String, mut args: Vec<String>) {
       match command.as_ref() {
       "stop_collector_channel" => {
       let collector_name = command.remove(0);
       let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
       for channel in args.into_iter() {
       controller_tx.send(Control::StopChannel(channel)).unwrap();
       }
       let action_status = ActionStatus::new(id, "running");
       self.collector_tx.send(Box::new(action_status)).unwrap();
       }
       "start_collector_channel" => {
       let collector_name = command.remove(0);
       let controller_tx = self.collector_controllers.get_mut(&collector_name).unwrap();
       for channel in args.into_iter() {
       controller_tx.send(Control::StartChannel(channel)).unwrap();
       }
       let action_status = ActionStatus::new(id, "running");
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
     let action_status = ActionStatus::new(id, "running");
     self.collector_tx.send(Box::new(action_status)).unwrap();
     }
     }
     }
     "start_collector" => {
     let collector_name = args.remove(0);
     if let Some(running) = self.collector_run_status.get_mut(&collector_name) {
     if !*running { 
     self.spawn_collector(&collector_name);
     let action_status = ActionStatus::new(id, "done");
     self.collector_tx.send(Box::new(action_status)).unwrap();
     }
     }

     if let Some(status) = self.collector_run_status.get_mut(&collector_name) {
     *status = true;
     }
     }
     _ => unimplemented!()
     }
     }
     */
}

#[tokio::main(basic_scheduler)]
pub async fn start(config: Config, collector_tx: crossbeam_channel::Sender<Box<dyn Packable + Send>>) {
    let connection_id = format!("actions-{}", config.device_id);
    let actions_subscription = format!("/topics/devices/{}/actions", config.device_id);
    let mut mqttoptions = MqttOptions::new(connection_id, &config.broker, config.port);
    mqttoptions.set_keep_alive(30);

    let (mut requests_tx, requests_rx) = channel(10);
    let mut eventloop = eventloop(mqttoptions, requests_rx);
    let mut controller = Controller::new(config, collector_tx.clone());
    let mut process = Process::new(collector_tx);

    // start the eventloop
    loop {
        let subscribe = rumq_client::subscribe(actions_subscription.clone(), QoS::AtLeastOnce);
        let subscribe = Request::Subscribe(subscribe);
        let _ = requests_tx.send(subscribe).await;
        let mut stream = eventloop.stream();

        while let Some(notification) = stream.next().await {
            let action = match action(notification) {
                Ok(action) => action,
                Err(e) => {
                    error!("Unable to create action. Error = {:?}", e);
                    continue
                }
            };

            debug!("Action = {:?}", action);

            match action.kind.as_ref() {
                "control" => {
                    // let command = action.name.clone();
                    // let payload = action.payload.clone();
                    // let id = action.id;
                    // controller.execute(&id, command, payload);
                }
                "process" => {
                    let command = action.name.clone();
                    let payload = action.payload.clone();
                    let id = action.id;
                    process.execute(id, command, payload).await;
                }
                _ => unimplemented!()
            }
        }
    }
}


fn action(notification: Notification) -> Result<Action, Error> {
    let action = match notification {
        Notification::Publish(publish) => {
            serde_json::from_slice(publish.payload())?
        }
        Notification::StreamEnd(err) => {
            error!("Stream closed!! Error = {:?}", err);
            return Err(Error::Stream(err))
        }
        _ => {
            error!("Unsupported notification = {:?}", notification);
            unimplemented!()
        }
    };

    Ok(action)
}

/// Process abstracts functions to spawn process and handle their output
/// It makes sure that a new process isn't executed when the previous process
/// is in progress.
/// It sends result and errors to the broker over collector_tx
struct Process {
    // we use this flag to ignore new process spawn while previous process is in progress 
    last_process_done: Arc<Mutex<bool>>,
    // used to send errors and process status to cloud
    collector_tx: crossbeam_channel::Sender<Box<dyn Packable + Send>>,
}

impl Process {
    fn new(collector_tx: crossbeam_channel::Sender<Box<dyn Packable + Send>>) -> Process {
        Process {
            last_process_done: Arc::new(Mutex::new(true)),
            collector_tx
        }
    }

    async fn execute<S: Into<String>>(&mut self, id: S, command: S, payload: S) {
        // check if last process is in progress
        if *self.last_process_done.lock().unwrap() == false {
            error!("last command not done!! returning");
            return
        } else {
            *self.last_process_done.lock().unwrap() = false;
        }

        let mut cmd = Command::new(command.into());

        cmd.arg(id.into());
        cmd.arg(payload.into());
        cmd.kill_on_drop(true);
        cmd.stdout(Stdio::piped());

        let collector_tx = self.collector_tx.clone();
        let last_process_done = self.last_process_done.clone();

        tokio::spawn(async move {
            // spawn the process
            let mut child = cmd.spawn().expect("failed to spawn command");
            let stdout = child.stdout().take().expect("child did not have a handle to stdout");

            // wait for spawned process result without blocking
            tokio::spawn(async {
                let child = time::timeout(Duration::from_secs(120), async {
                    child.await.expect("child process encountered an error");
                });

                let status = child.await;
                println!("child status was: {:?}", status);
            });

            // stream the stdout of spawned process to capture its progress
            let mut stdout = BufReader::new(stdout).lines();
            while let Some(line) = stdout.next_line().await.unwrap() {
                dbg!(&line);
                let status: ActionStatus = match serde_json::from_str(&line) {
                    Ok(status) => status,
                    Err(e) => {
                        error!("Deserialization error = {:?}", e);
                        break;
                    }
                };

                println!("Acion status: {:?}", status);
                collector_tx.send(Box::new(status)).unwrap();
            }

            *last_process_done.lock().unwrap() = true;
        });
    }
}

impl Packable for ActionStatus {
    fn channel(&self) -> String {
        return "action_status".to_owned() 
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}
