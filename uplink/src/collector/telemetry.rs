use flume::Sender;
use log::error;
use serde::Serialize;
use sysinfo::{DiskExt, NetworkData, NetworkExt, ProcessExt, ProcessorExt, System, SystemExt};

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::base::{Buffer, Config, Package, Point, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
}

const TIME_PERIOD_SECS: f64 = 10.0;

#[derive(Debug, Default, Serialize, Clone)]
struct LoadAvg {
    /// Average load within one minute.
    pub one: f64,
    /// Average load within five minutes.
    pub five: f64,
    /// Average load within fifteen minutes.
    pub fifteen: f64,
}

#[derive(Debug, Default, Serialize, Clone)]
struct SysInfo {
    kernel_version: String,
    uptime: u64,
    no_processes: usize,
    load_avg: LoadAvg,
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
        let sysinfo::LoadAvg { one, five, fifteen } = sys.load_average();
        self.load_avg = LoadAvg { one, five, fifteen };
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Network {
    name: String,
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
}

impl Network {
    fn init(name: String) -> Self {
        Network { name, ..Default::default() }
    }

    /// Update metrics values for network usage over time
    fn refresh(&mut self, data: &NetworkData) {
        self.incoming_data_rate = data.received() as f64 / TIME_PERIOD_SECS;
        self.outgoing_data_rate = data.transmitted() as f64 / TIME_PERIOD_SECS;
    }
}

// TODO: Make changes to ensure this works properly
impl Point for Network {
    fn sequence(&self) -> u32 {
        0
    }

    fn timestamp(&self) -> u64 {
        0
    }
}

// TODO: Make changes to ensure this works properly
impl Package for Buffer<Network> {
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

#[derive(Debug, Serialize, Clone)]
struct Disk {
    name: String,
    total: u64,
    available: u64,
}

impl Disk {
    fn init(name: String, disk: &sysinfo::Disk) -> Self {
        Disk { name, total: disk.total_space(), available: disk.available_space() }
    }

    fn refresh(&mut self, disk: &sysinfo::Disk) {
        self.available = disk.available_space();
    }
}

// TODO: Make changes to ensure this works properly
impl Point for Disk {
    fn sequence(&self) -> u32 {
        0
    }

    fn timestamp(&self) -> u64 {
        0
    }
}

// TODO: Make changes to ensure this works properly
impl Package for Buffer<Disk> {
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

#[derive(Debug, Default, Serialize, Clone)]
struct Processor {
    name: String,
    frequency: u64,
    usage: f32,
}

impl Processor {
    fn init(name: String) -> Self {
        Processor { name, ..Default::default() }
    }

    fn refresh(&mut self, proc: &sysinfo::Processor) {
        self.frequency = proc.frequency();
        self.usage = proc.cpu_usage();
    }
}

// TODO: Make changes to ensure this works properly
impl Point for Processor {
    fn sequence(&self) -> u32 {
        0
    }

    fn timestamp(&self) -> u64 {
        0
    }
}

// TODO: Make changes to ensure this works properly
impl Package for Buffer<Processor> {
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

#[derive(Debug, Default, Serialize, Clone)]
struct DiskUsage {
    total_written_bytes: u64,
    written_bytes: u64,
    total_read_bytes: u64,
    read_bytes: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
struct Process {
    id: i32,
    cpu_usage: f32,
    mem_usage: u64,
    disk_usage: DiskUsage,
    start_time: u64,
}

impl Process {
    fn init(id: i32, start_time: u64) -> Self {
        Process { id, start_time, ..Default::default() }
    }

    fn refresh(&mut self, proc: &sysinfo::Process) {
        let sysinfo::DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes } =
            proc.disk_usage();
        self.disk_usage =
            DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes };
        self.cpu_usage = proc.cpu_usage();
        self.mem_usage = proc.memory();
    }
}

// TODO: Make changes to ensure this works properly
impl Point for Process {
    fn sequence(&self) -> u32 {
        0
    }

    fn timestamp(&self) -> u64 {
        0
    }
}

// TODO: Make changes to ensure this works properly
impl Package for Buffer<Process> {
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
    memory: Mem,
    file_count: usize,
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

struct Streams {
    system: Stream<SystemStats>,
    processors: Stream<Processor>,
    processes: Stream<Process>,
    disks: Stream<Disk>,
    networks: Stream<Network>,
}

impl Streams {
    fn init(tx: Sender<Box<dyn Package>>, config: &Arc<Config>) -> Self {
        let system =
            Stream::dynamic("system_stats", &config.project_id, &config.device_id, tx.clone());

        let processors =
            Stream::dynamic("processor_stats", &config.project_id, &config.device_id, tx.clone());

        let processes =
            Stream::dynamic("process_stats", &config.project_id, &config.device_id, tx.clone());

        let disks =
            Stream::dynamic("disk_stats", &config.project_id, &config.device_id, tx.clone());

        let networks = Stream::dynamic("network_stats", &config.project_id, &config.device_id, tx);

        Streams { system, processors, processes, disks, networks }
    }
}

pub struct StatCollector {
    sys: System,
    stats: SystemStats,
    processes: HashMap<i32, Process>,
    processors: HashMap<String, Processor>,
    networks: HashMap<String, Network>,
    disks: HashMap<String, Disk>,
    config: Arc<Config>,
    streams: Streams,
}

impl StatCollector {
    pub fn new(config: Arc<Config>, tx: Sender<Box<dyn Package>>) -> Self {
        let sys = System::default();

        let sysinfo = SysInfo::init(&sys);

        let mut disks = HashMap::new();
        for disk_data in sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            disks.insert(disk_name.clone(), Disk::init(disk_name, disk_data));
        }

        let mut networks = HashMap::new();
        for (name, _) in sys.networks() {
            networks.insert(name.clone(), Network::init(name.clone()));
        }

        let mut processors = HashMap::new();
        for proc in sys.processors().iter() {
            let proc_name = proc.name().to_owned();
            processors.insert(proc_name.clone(), Processor::init(proc_name));
        }

        let processes = HashMap::new();

        let memory = Mem { total: sys.total_memory(), available: sys.available_memory() };
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let stats = SystemStats { timestamp, sysinfo, memory, ..Default::default() };

        let streams = Streams::init(tx, &config);

        StatCollector { sys, stats, config, processes, disks, networks, processors, streams }
    }

    pub fn start(mut self) {
        loop {
            std::thread::sleep(Duration::from_secs_f64(TIME_PERIOD_SECS));

            if let Err(e) = self.refresh() {
                error!("Faced error while refreshing telemetrics: {}", e);
                return;
            };
        }
    }

    fn refresh(&mut self) -> Result<(), Error> {
        self.stats.sequence += 1;
        self.stats.sysinfo.refresh(&self.sys);

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

        if let Err(e) = self.streams.system.push(self.stats.clone()) {
            error!("Couldn't send system stats: {}", e);
        }

        for disk_data in self.sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            let disk =
                self.disks.entry(disk_name.clone()).or_insert(Disk::init(disk_name, disk_data));
            disk.refresh(disk_data);

            if let Err(e) = self.streams.disks.push(disk.clone()) {
                error!("Couldn't send disk stats: {}", e);
            }
        }

        // Refresh network byte rate info
        for (interface, net_data) in self.sys.networks() {
            let net =
                self.networks.entry(interface.clone()).or_insert(Network::init(interface.clone()));
            net.refresh(net_data);

            if let Err(e) = self.streams.networks.push(net.clone()) {
                error!("Couldn't send network stats: {}", e);
            }
        }

        // Refresh processor data
        for proc_data in self.sys.processors().iter() {
            let proc_name = proc_data.name().to_string();
            let proc =
                self.processors.entry(proc_name.clone()).or_insert(Processor::init(proc_name));
            proc.refresh(proc_data);

            if let Err(e) = self.streams.processors.push(proc.clone()) {
                error!("Couldn't send processor stats: {}", e);
            }
        }

        // Refresh data on processes
        for (&id, p) in self.sys.processes() {
            let proc = self.processes.entry(id).or_insert(Process::init(id, p.start_time()));
            proc.refresh(p);

            if let Err(e) = self.streams.processes.push(proc.clone()) {
                error!("Couldn't send process stats: {}", e);
            }
        }

        // Refresh sysinfo counters
        self.sys.refresh_all();

        Ok(())
    }
}
