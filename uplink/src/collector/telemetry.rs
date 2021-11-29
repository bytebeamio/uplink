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
struct Network {
    sequence: u32,
    timestamp: u64,
    name: String,
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
}

impl Network {
    fn init(name: String) -> Self {
        Network { name, ..Default::default() }
    }

    /// Update metrics values for network usage over time
    fn update(&mut self, data: &NetworkData, update_period: f64) {
        self.incoming_data_rate = data.received() as f64 / update_period;
        self.outgoing_data_rate = data.transmitted() as f64 / update_period;
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

    fn update(&mut self, disk: &sysinfo::Disk) {
        self.available = disk.available_space();
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

    fn update(&mut self, proc: &sysinfo::Processor) {
        self.frequency = proc.frequency();
        self.usage = proc.cpu_usage();
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

    fn update(&mut self, proc: &sysinfo::Process) {
        let sysinfo::DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes } =
            proc.disk_usage();
        self.disk_usage =
            DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes };
        self.cpu_usage = proc.cpu_usage();
        self.mem_usage = proc.memory();
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

#[derive(Debug, Default, Serialize, Clone)]
struct Mem {
    total: u64,
    available: u64,
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

type Sequence = u32;

#[derive(Default)]
struct Sequences {
    system: Sequence,
    processors: Sequence,
    processes: Sequence,
    disks: Sequence,
    networks: Sequence,
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

pub struct StatCollector {
    sys: System,
    timestamp: u64,
    stats: SystemStats,
    processes: HashMap<i32, Process>,
    processors: HashMap<String, Processor>,
    networks: HashMap<String, Network>,
    disks: HashMap<String, Disk>,
    config: Arc<Config>,
    streams: Streams,
    sequences: Sequences,
}

impl StatCollector {
    pub fn new(config: Arc<Config>, tx: Sender<Box<dyn Package>>) -> Self {
        let mut sys = System::new();
        sys.refresh_disks_list();
        sys.refresh_networks_list();

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

        let stats = SystemStats { sysinfo, memory, ..Default::default() };

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

    fn update(&mut self) -> Result<(), Error> {
        self.stats.sysinfo.update(&self.sys);
        self.stats.memory.available = self.sys.available_memory();
        self.stats.timestamp = self.timestamp;
        self.stats.sequence = self.sequences.system;
        self.sequences.system += 1;

        if let Err(e) = self.streams.system.push(self.stats.clone()) {
            error!("Couldn't send system stats: {}", e);
        }

        // Refresh disk info
        for disk_data in self.sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            let disk =
                self.disks.entry(disk_name.clone()).or_insert(Disk::init(disk_name, disk_data));
            disk.update(disk_data);
            disk.timestamp = self.timestamp;
            disk.sequence = self.sequences.disks;
            self.sequences.disks += 1;

            if let Err(e) = self.streams.disks.push(disk.clone()) {
                error!("Couldn't send disk stats: {}", e);
            }
        }

        // Refresh network byte rate info
        for (interface, net_data) in self.sys.networks() {
            let net =
                self.networks.entry(interface.clone()).or_insert(Network::init(interface.clone()));
            net.update(net_data, self.config.stats.update_period);
            net.timestamp = self.timestamp;
            net.sequence = self.sequences.networks;
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
            proc.update(proc_data);
            proc.timestamp = self.timestamp;
            proc.sequence = self.sequences.processors;
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
                proc.update(p);
                proc.timestamp = self.timestamp;
                proc.sequence = self.sequences.processes;
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
