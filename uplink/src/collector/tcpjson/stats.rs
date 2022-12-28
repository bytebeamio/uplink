use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use flume::Sender;
use log::error;
use serde::Serialize;

use crate::{Action, ActionResponse, Config, Package, Point, Stream};

#[derive(Serialize, Clone, Debug, Default)]
pub struct BridgeStats {
    app_count: usize,
    total_connections: usize,
    sequence: u32,
    timestamp: u64,
    actions_received: u32,
    current_action: Option<String>,
    error: Option<String>,
    connected: bool,
}

impl Point for BridgeStats {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

pub struct BridgeStatsHandler {
    stats: BridgeStats,
    stat_stream: Option<Stream<BridgeStats>>,
}

impl BridgeStatsHandler {
    pub fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>) -> Self {
        let stat_stream = config.bridge_stats.as_ref().map(|stat_config| {
            Stream::with_config(
                &"uplink_bridge_stats".to_string(),
                &config.project_id,
                &config.device_id,
                stat_config,
                data_tx,
            )
        });
        let stats = BridgeStats { connected: false, ..Default::default() };

        Self { stat_stream, stats }
    }

    pub fn capture_action(&mut self, action_id: String) {
        self.stats.current_action = Some(action_id)
    }

    pub fn capture_error(&mut self, error: String) {
        self.stats.error = Some(error);
    }

    pub async fn update(&mut self, app_count: usize) {
        self.stats.sequence += 1;
        self.stats.timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        self.stats.app_count = app_count;

        let stat_stream = match &mut self.stat_stream {
            Some(s) => s,
            _ => return,
        };

        if let Err(e) = stat_stream.fill(self.stats.clone()).await {
            error!("Failed to send data. Error = {:?}", e);
        }
    }

    pub fn accept(&mut self) -> usize {
        self.stats.total_connections += 1;
        self.stats.total_connections
    }

    pub fn reset_action(&mut self) {
        self.stats.current_action = None;
    }

    pub fn reset_error(&mut self) {
        self.stats.error = None;
    }
}

#[derive(Serialize, Clone, Default, Debug)]
struct AppStats {
    app_id: usize,
    sequence: u32,
    timestamp: u64,
    connection_timestamp: u64,
    actions_received: u32,
    last_action: Option<String>,
    actions_responded: u32,
    last_response: Option<ActionResponse>,
    error: Option<String>,
    connected: bool,
}

impl Point for AppStats {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

pub struct AppStatsHandler {
    stats: AppStats,
    stat_stream: Option<Stream<AppStats>>,
}

impl AppStatsHandler {
    pub fn new(app_id: usize, config: Arc<Config>, data_tx: Sender<Box<dyn Package>>) -> Self {
        let stats = AppStats {
            connection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
                as u64,
            connected: true,
            app_id,
            ..Default::default()
        };
        let stat_stream = config.app_stats.as_ref().map(|stat_config| {
            Stream::with_config(
                &"uplink_app_stats".to_string(),
                &config.project_id,
                &config.device_id,
                stat_config,
                data_tx,
            )
        });

        Self { stats, stat_stream }
    }

    pub fn capture_response(&mut self, response: &ActionResponse) {
        self.stats.last_response = Some(response.clone());
        self.stats.actions_responded += 1;
    }

    pub fn capture_action(&mut self, action: &Action) {
        self.stats.last_action = Some(action.action_id.clone());
        self.stats.actions_received += 1;
    }

    pub fn capture_error(&mut self, error: String) {
        self.stats.error = Some(error);
    }

    pub async fn update(&mut self) {
        self.stats.sequence += 1;
        self.stats.timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let stat_stream = match &mut self.stat_stream {
            Some(s) => s,
            _ => return,
        };

        if let Err(e) = stat_stream.fill(self.stats.clone()).await {
            error!("Failed to send data. Error = {:?}", e);
        }
    }

    pub fn disconnect(&mut self, error: String) {
        self.stats.connected = false;
        self.capture_error(error);
    }

    pub fn reset(&mut self) {
        self.stats.error = None;
        self.stats.last_action = None;
        self.stats.last_response = None;
    }
}
