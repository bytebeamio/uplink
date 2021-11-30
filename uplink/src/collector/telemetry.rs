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
    #[serde(flatten)]
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

    fn update(&mut self, sys: &System) {
        self.uptime = sys.uptime();
        self.no_processes = sys.processes().len();
        let sysinfo::LoadAvg { one, five, fifteen } = sys.load_average();
        self.load_avg = LoadAvg { one, five, fifteen };
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Mem {
    total_memory: u64,
    available_memory: u64,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct SystemStats {
    sequence: u32,
    timestamp: u64,
    #[serde(flatten)]
    sysinfo: SysInfo,
    #[serde(flatten)]
    memory: Mem,
}

impl SystemStats {
    fn init(sys: &System) -> SystemStats {
        let sysinfo = SysInfo::init(sys);
        let memory = Mem { total_memory: sys.total_memory(), ..Default::default() };

        SystemStats { sysinfo, memory, ..Default::default() }
    }

    fn update(&mut self, sys: &System, timestamp: u64, sequence: u32) {
        self.sysinfo.update(sys);
        self.memory.available_memory = sys.available_memory();
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl Point for SystemStats {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<SystemStats> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Network {
    sequence: u32,
    timestamp: u64,
    name: String,
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
    #[serde(skip_serializing)]
    update_period: f64,
}

impl Network {
    fn init(name: String, update_period: f64) -> Self {
        Network { name, update_period, ..Default::default() }
    }

    /// Update metrics values for network usage over time
    fn update(&mut self, data: &NetworkData, timestamp: u64, sequence: u32) {
        self.incoming_data_rate = data.received() as f64 / self.update_period;
        self.outgoing_data_rate = data.transmitted() as f64 / self.update_period;
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl Point for Network {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Network> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}

#[derive(Debug, Serialize, Default, Clone)]
struct Disk {
    sequence: u32,
    timestamp: u64,
    name: String,
    total: u64,
    available: u64,
}

impl Disk {
    fn init(name: String, disk: &sysinfo::Disk) -> Self {
        Disk {
            name,
            total: disk.total_space(),
            available: disk.available_space(),
            ..Default::default()
        }
    }

    fn update(&mut self, disk: &sysinfo::Disk, timestamp: u64, sequence: u32) {
        self.available = disk.available_space();
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl Point for Disk {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Disk> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Processor {
    sequence: u32,
    timestamp: u64,
    name: String,
    frequency: u64,
    usage: f32,
}

impl Processor {
    fn init(name: String) -> Self {
        Processor { name, ..Default::default() }
    }

    fn update(&mut self, proc: &sysinfo::Processor, timestamp: u64, sequence: u32) {
        self.frequency = proc.frequency();
        self.usage = proc.cpu_usage();
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl Point for Processor {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Processor> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
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
    sequence: u32,
    timestamp: u64,
    pid: i32,
    name: String,
    cpu_usage: f32,
    mem_usage: u64,
    #[serde(flatten)]
    disk_usage: DiskUsage,
    start_time: u64,
}

impl Process {
    fn init(pid: i32, name: String, start_time: u64) -> Self {
        Process { pid, name, start_time, ..Default::default() }
    }

    fn update(&mut self, proc: &sysinfo::Process, timestamp: u64, sequence: u32) {
        let sysinfo::DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes } =
            proc.disk_usage();
        self.disk_usage =
            DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes };
        self.cpu_usage = proc.cpu_usage();
        self.mem_usage = proc.memory();
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl Point for Process {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Process> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}

type Sequence = u32;

/// Holds current sequence number values meant for different data streams
#[derive(Default)]
struct Sequences {
    system: Sequence,
    processors: Sequence,
    processes: Sequence,
    disks: Sequence,
    networks: Sequence,
}

/// Stream handles for the various data streams
struct Streams {
    system: Stream<SystemStats>,
    processors: Stream<Processor>,
    processes: Stream<Process>,
    disks: Stream<Disk>,
    networks: Stream<Network>,
}

impl Streams {
    fn init(tx: Sender<Box<dyn Package>>, config: &Arc<Config>) -> Self {
        Streams {
            system: Stream::dynamic(
                "system_stats",
                &config.project_id,
                &config.device_id,
                tx.clone(),
            ),
            processors: Stream::dynamic(
                "processor_stats",
                &config.project_id,
                &config.device_id,
                tx.clone(),
            ),
            processes: Stream::dynamic(
                "process_stats",
                &config.project_id,
                &config.device_id,
                tx.clone(),
            ),
            disks: Stream::dynamic("disk_stats", &config.project_id, &config.device_id, tx.clone()),
            networks: Stream::dynamic("network_stats", &config.project_id, &config.device_id, tx),
        }
    }
}

/// Collects and forward system information such as kernel version, memory and disk space usage,
/// information regarding running processes, network and processor usage, etc to an IoT platform.
pub struct StatCollector {
    /// Handle to sysinfo struct containing system information.
    sys: System,
    /// Frequently updated timestamp value, used to ensure idempotence.
    timestamp: u64,
    /// System information values to be serialized.
    stats: SystemStats,
    /// Information about running processes.
    processes: HashMap<i32, Process>,
    /// Individual Processor information.
    processors: HashMap<String, Processor>,
    /// Information regarding individual Network interfaces.
    networks: HashMap<String, Network>,
    /// Information regarding individual Disks.
    disks: HashMap<String, Disk>,
    /// Uplink configuration.
    config: Arc<Config>,
    /// Stream handles
    streams: Streams,
    /// Data point sequence numbers.
    sequences: Sequences,
}

impl StatCollector {
    /// Create and initialize a stat collector
    pub fn new(config: Arc<Config>, tx: Sender<Box<dyn Package>>) -> Self {
        let mut sys = System::new();
        sys.refresh_disks_list();
        sys.refresh_networks_list();

        let mut disks = HashMap::new();
        for disk_data in sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            disks.insert(disk_name.clone(), Disk::init(disk_name, disk_data));
        }

        let mut networks = HashMap::new();
        for (name, _) in sys.networks() {
            networks.insert(name.clone(), Network::init(name.clone(), config.stats.update_period));
        }

        let mut processors = HashMap::new();
        for proc in sys.processors().iter() {
            let proc_name = proc.name().to_owned();
            processors.insert(proc_name.clone(), Processor::init(proc_name));
        }

        let processes = HashMap::new();

        let stats = SystemStats::init(&sys);

        let streams = Streams::init(tx, &config);

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        StatCollector {
            sys,
            stats,
            config,
            processes,
            disks,
            networks,
            processors,
            streams,
            timestamp,
            sequences: Default::default(),
        }
    }

    /// Stat collector execution loop, sleeps for the duation of `config.stats.update_period` in seconds.
    pub fn start(mut self) {
        loop {
            std::thread::sleep(Duration::from_secs_f64(self.config.stats.update_period));
            self.timestamp += self.config.stats.update_period as u64;

            if let Err(e) = self.update() {
                error!("Faced error while refreshing telemetrics: {}", e);
                return;
            };
        }
    }

    /// Update system information values and increment sequence numbers, while sending to specific data streams.
    fn update(&mut self) -> Result<(), Error> {
        self.stats.update(&self.sys, self.timestamp, self.sequences.system);
        self.sequences.system += 1;

        if let Err(e) = self.streams.system.push(self.stats.clone()) {
            error!("Couldn't send system stats: {}", e);
        }

        // Refresh disk info
        for disk_data in self.sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            let disk =
                self.disks.entry(disk_name.clone()).or_insert(Disk::init(disk_name, disk_data));
            disk.update(disk_data, self.timestamp, self.sequences.disks);
            self.sequences.disks += 1;

            if let Err(e) = self.streams.disks.push(disk.clone()) {
                error!("Couldn't send disk stats: {}", e);
            }
        }

        // Refresh network byte rate info
        for (interface, net_data) in self.sys.networks() {
            let net = self
                .networks
                .entry(interface.clone())
                .or_insert(Network::init(interface.clone(), self.config.stats.update_period));
            net.update(net_data, self.timestamp, self.sequences.networks);
            self.sequences.networks += 1;

            if let Err(e) = self.streams.networks.push(net.clone()) {
                error!("Couldn't send network stats: {}", e);
            }
        }

        // Refresh processor info
        for proc_data in self.sys.processors().iter() {
            let proc_name = proc_data.name().to_string();
            let proc =
                self.processors.entry(proc_name.clone()).or_insert(Processor::init(proc_name));
            proc.update(proc_data, self.timestamp, self.sequences.processors);
            self.sequences.processors += 1;

            if let Err(e) = self.streams.processors.push(proc.clone()) {
                error!("Couldn't send processor stats: {}", e);
            }
        }

        // Refresh processes info
        for (&id, p) in self.sys.processes() {
            let name = p.name().to_owned();

            if self.config.stats.names.contains(&name) {
                let proc =
                    self.processes.entry(id).or_insert(Process::init(id, name, p.start_time()));
                proc.update(p, self.timestamp, self.sequences.processes);
                self.sequences.processes += 1;

                if let Err(e) = self.streams.processes.push(proc.clone()) {
                    error!("Couldn't send process stats: {}", e);
                }
            }
        }

        // Refresh sysinfo counters
        self.sys.refresh_all();
        self.sys.refresh_disks_list();
        self.sys.refresh_networks_list();

        Ok(())
    }
}
