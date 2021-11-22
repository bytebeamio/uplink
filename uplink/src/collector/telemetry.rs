use async_channel::Sender;
use log::error;
use serde::Serialize;
use sysinfo::{DiskExt, NetworkData, NetworkExt, System, SystemExt};

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::base::{Buffer, Config, Package, Point, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
}

pub async fn spawn_telemetry_collector(config: Arc<Config>, tx: Sender<Box<dyn Package>>) {
    let update_duration = Duration::from_millis(10);
    let stream_name = "telemetry";

    let telemetry_cfg = match config.streams.get(stream_name) {
        Some(c) => c,
        None => {
            error!("Couldn't find config for telemetry stream");
            return;
        }
    };
    let mut telemetry_stream =
        Stream::new(stream_name, &telemetry_cfg.topic, telemetry_cfg.buf_size, tx);

    let mut telemetry = TelemetryHandler::init(&update_duration, config);

    loop {
        tokio::time::sleep(update_duration).await;

        let data = match telemetry.refresh() {
            Ok(d) => d,
            Err(e) => {
                error!("Faced error while refreshing telemetrics: {}", e);
                return;
            }
        };

        if let Err(e) = telemetry_stream.fill(data).await {
            error!("Couldn't send telemetry: {}", e);
        }
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct SysInfo {
    kernel_version: String,
    uptime: u64,
    no_processes: usize,
}

impl SysInfo {
    fn init(sys: &System) -> Self {
        SysInfo {
            kernel_version: match sys.kernel_version() {
                Some(kv) => kv,
                None => String::default(),
            },
            ..Default::default()
        }
    }

    fn refresh(&mut self, sys: &System) {
        self.uptime = sys.uptime();
        self.no_processes = sys.processes().len();
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Net {
    #[serde(skip_serializing)]
    elapsed_secs: f64,
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
}

impl Net {
    fn init(update_duration: &Duration) -> Self {
        // Time between update
        let elapsed_secs = update_duration.as_secs_f64();

        Net { elapsed_secs, ..Default::default() }
    }

    /// Update metrics values for network usage over time
    fn refresh(&mut self, data: &NetworkData) {
        self.incoming_data_rate = data.received() as f64 / self.elapsed_secs;
        self.outgoing_data_rate = data.transmitted() as f64 / self.elapsed_secs;
    }
}

#[derive(Debug, Serialize, Clone)]
struct Disk {
    total: u64,
    available: u64,
}

impl Disk {
    fn init(disk: &sysinfo::Disk) -> Self {
        Disk { total: disk.total_space(), available: disk.available_space() }
    }

    fn refresh(&mut self, disk: &sysinfo::Disk) {
        self.available = disk.available_space();
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Proc {
    frequency: f64,
    usage: f64,
    load_avg: f64,
}

impl Proc {}

#[derive(Debug, Default, Serialize, Clone)]
struct Process {
    cpu_usage: f64,
    mem_usage: f64,
    disk_usage: f64,
    start_time: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
struct Mem {
    total: u64,
    available: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
struct Telemetrics {
    sysinfo: SysInfo,
    processes: HashMap<i32, Process>,
    procs: HashMap<i32, Proc>,
    nets: HashMap<String, Net>,
    disks: HashMap<String, Disk>,
    file_count: usize,
}

#[derive(Debug)]
struct TelemetryHandler {
    sys: System,
    data: Telemetrics,
    config: Arc<Config>,
    update_duration: Duration,
}

impl TelemetryHandler {
    fn init(update_duration: &Duration, config: Arc<Config>) -> Self {
        let sys = System::default();

        let sysinfo = SysInfo::init(&sys);

        let mut disks = HashMap::new();
        for disk_data in sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            disks.insert(disk_name, Disk::init(disk_data));
        }

        let mut nets = HashMap::new();
        for (name, _) in sys.networks() {
            nets.insert(name.clone(), Net::init(update_duration));
        }

        let data = Telemetrics { sysinfo, disks, nets, ..Default::default() };

        TelemetryHandler { sys, data, config, update_duration: update_duration.clone() }
    }

    fn refresh(&mut self) -> Result<Telemetrics, Error> {
        self.data.sysinfo.refresh(&self.sys);

        for disk_data in self.sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            self.data.disks.entry(disk_name).or_insert(Disk::init(disk_data)).refresh(disk_data);
        }

        // Refresh network byte rate info
        for (interface, net_data) in self.sys.networks() {
            let net =
                self.data.nets.entry(interface.clone()).or_insert(Net::init(&self.update_duration));
            net.refresh(net_data)
        }

        // Extract file count from persistence directory
        self.data.file_count = match std::fs::read_dir(&self.config.persistence.path) {
            Ok(d) => d.count(),
            Err(e) => {
                error!("Couldn't find file count: {}", e);
                return Err(Error::Io(e));
            }
        };

        // Refresh sysinfo counters
        self.sys.refresh_all();

        Ok(self.data.clone())
    }
}

// TODO: Make changes to ensure this works properly
impl Point for Telemetrics {
    fn sequence(&self) -> u32 {
        0
    }

    fn timestamp(&self) -> u64 {
        0
    }
}

// TODO: Make changes to ensure this works properly
impl Package for Buffer<Telemetrics> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        vec![]
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        Some((self.anomalies.clone(), self.anomaly_count))
    }
}
