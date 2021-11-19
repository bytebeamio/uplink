use log::error;
use serde::Serialize;
use sysinfo::{NetworkExt, System, SystemExt};

use std::{sync::Arc, time::Duration};

use crate::base::Config;

pub async fn telemetry_collector(cfg: Arc<Config>) {
    let update_duration = Duration::from_millis(10);
    let mut telemetry = Telemetrics::default();
    loop {
        telemetry.refresh_network(&update_duration);
        telemetry.refresh_disk(&cfg.persistence.path);

        tokio::time::sleep(update_duration).await;
    }
}

#[derive(Debug, Default, Serialize)]
struct Telemetrics {
    #[serde(skip_serializing)]
    sys: System,
    proc: Vec<Proc>,
    net: Net,
    disk: Disk,
}

impl Telemetrics {
    /// Update metrics values for network usage over time
    pub fn refresh_network(&mut self, update_duration: &Duration) {
        // Accumulate byte count from all network interfaces
        let (mut incoming_bytes, mut outgoing_bytes) = (0.0, 0.0);
        for (_, data) in self.sys.networks() {
            incoming_bytes += data.received() as f64;
            outgoing_bytes += data.transmitted() as f64;
        }

        // Measure time from last update
        let elapsed_secs = update_duration.as_secs_f64();

        // Calculate and update network byte rate
        self.net.incoming_data_rate = incoming_bytes / elapsed_secs;
        self.net.outgoing_data_rate = outgoing_bytes / elapsed_secs;

        // Refresh network byte-rate counter and time handle
        self.sys.refresh_networks();
    }

    /// Extract file count from persistence directory
    fn refresh_disk(&mut self, persistence_path: &String) {
        self.disk.file_count = match std::fs::read_dir(persistence_path) {
            Ok(d) => d.count(),
            Err(e) => {
                error!("Couldn't find file count: {}", e);
                return;
            }
        };
    }
}

#[derive(Debug, Default, Serialize)]
struct SystemInfo {
    kernel_version: String,
    uptime: u64,
    no_processes: u64,
}

#[derive(Debug, Default, Serialize)]
struct Net {
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
    errors: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
struct Disk {
    total: u64,
    available: u64,
    file_count: usize,
}

#[derive(Debug, Default, Serialize)]
struct Proc {
    frequency: f64,
    usage: f64,
    load_avg: f64,
}

#[derive(Debug, Default, Serialize)]
struct Process {
    cpu_usage: f64,
    mem_usage: f64,
    disk_usage: f64,
    start_time: u64,
}

#[derive(Debug, Default, Serialize)]
struct Mem {
    total: u64,
    available: u64,
}
