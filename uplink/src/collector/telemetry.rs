use log::error;
use serde::Serialize;
use sysinfo::{DiskExt, NetworkData, NetworkExt, ProcessExt, ProcessorExt, System, SystemExt};

use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use crate::base::{Buffer, Config, Package, Point, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
}

const TIME_PERIOD_SECS: f64 = 10.0;

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
struct Network {
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
}

impl Network {
    /// Update metrics values for network usage over time
    fn refresh(&mut self, data: &NetworkData) {
        self.incoming_data_rate = data.received() as f64 / TIME_PERIOD_SECS;
        self.outgoing_data_rate = data.transmitted() as f64 / TIME_PERIOD_SECS;
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
struct Processor {
    frequency: u64,
    usage: f32,
    load_avg: f32,
}

impl Processor {
    fn refresh(&mut self, proc: &sysinfo::Processor) {
        self.frequency = proc.frequency();
        self.usage = proc.cpu_usage();
        self.load_avg += self.usage;
        self.load_avg /= 2.0 * TIME_PERIOD_SECS as f32;
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct DiskUsage {
    total_written_bytes: u64,
    written_bytes: u64,
    total_read_bytes: u64,
    read_bytes: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
struct Process {
    cpu_usage: f32,
    mem_usage: u64,
    disk_usage: DiskUsage,
    start_time: u64,
}

impl Process {
    fn refresh(&mut self, proc: &sysinfo::Process) {
        let sysinfo::DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes } =
            proc.disk_usage();
        self.disk_usage =
            DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes };
        self.cpu_usage = proc.cpu_usage();
        self.mem_usage = proc.memory();
        self.start_time = proc.start_time();
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Mem {
    total: u64,
    available: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct SystemStats {
    sequence: u32,
    timestamp: u64,
    sysinfo: SysInfo,
    processes: HashMap<i32, Process>,
    processors: HashMap<String, Processor>,
    networks: HashMap<String, Network>,
    memory: Mem,
    disks: HashMap<String, Disk>,
    file_count: usize,
}

#[derive(Debug)]
pub struct StatCollector {
    sys: System,
    stats: SystemStats,
    config: Arc<Config>,
    stat_stream: Stream<SystemStats>,
}

impl StatCollector {
    pub fn new(config: Arc<Config>, stat_stream: Stream<SystemStats>) -> Self {
        let sys = System::default();

        let sysinfo = SysInfo::init(&sys);

        let mut disks = HashMap::new();
        for disk_data in sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            disks.insert(disk_name, Disk::init(disk_data));
        }

        let mut networks = HashMap::new();
        for (name, _) in sys.networks() {
            networks.insert(name.clone(), Network::default());
        }

        let mut processors = HashMap::new();
        for proc in sys.processors().iter() {
            processors.insert(proc.name().to_owned(), Processor::default());
        }

        let memory = Mem { total: sys.total_memory(), available: sys.available_memory() };
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let stats =
            SystemStats { timestamp, sysinfo, disks, networks, processors, memory, ..Default::default() };

        StatCollector { sys, stats, config, stat_stream }
    }

    pub async fn start(mut self) {
        loop {
            tokio::time::sleep(Duration::from_secs_f64(TIME_PERIOD_SECS)).await;

            let data = match self.refresh() {
                Ok(d) => d,
                Err(e) => {
                    error!("Faced error while refreshing telemetrics: {}", e);
                    return;
                }
            };

            if let Err(e) = self.stat_stream.fill(data).await {
                error!("Couldn't send telemetry: {}", e);
            }
        }
    }

    fn refresh(&mut self) -> Result<SystemStats, Error> {
        self.stats.sequence += 1;
        self.stats.sysinfo.refresh(&self.sys);

        for disk_data in self.sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            self.stats.disks.entry(disk_name).or_insert(Disk::init(disk_data)).refresh(disk_data);
        }

        // Refresh network byte rate info
        for (interface, net_data) in self.sys.networks() {
            let net = self.stats.networks.entry(interface.clone()).or_default();
            net.refresh(net_data)
        }

        // Refresh processor data
        for proc_data in self.sys.processors().iter() {
            let proc_name = proc_data.name().to_string();
            let proc = self.stats.processors.entry(proc_name).or_default();
            proc.refresh(proc_data)
        }

        // Refresh data on processes
        let mut processes = HashMap::new();
        for (&id, p) in self.sys.processes() {
            let mut proc = Process::default();
            proc.refresh(p);
            processes.insert(id, proc);
        }
        self.stats.processes = processes;

        // Extract file count from persistence directory
        self.stats.file_count = match std::fs::read_dir(&self.config.persistence.path) {
            Ok(d) => d.count(),
            Err(e) => {
                error!("Couldn't find file count: {}", e);
                return Err(Error::Io(e));
            }
        };

        self.stats.memory.available = self.sys.available_memory();
        self.stats.timestamp += TIME_PERIOD_SECS as u64; 

        // Refresh sysinfo counters
        self.sys.refresh_all();

        Ok(self.stats.clone())
    }
}

// TODO: Make changes to ensure this works properly
impl Point for SystemStats {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

// TODO: Make changes to ensure this works properly
impl Package for Buffer<SystemStats> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).map_or(vec![], |c| c)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        Some((self.anomalies.clone(), self.anomaly_count))
    }
}
