use flume::Sender;
use log::error;
use serde::Serialize;
use sysinfo::{DiskExt, NetworkData, NetworkExt, PidExt, ProcessExt, ProcessorExt, SystemExt};
use tokio::time::Instant;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::base::{self, Buffer, Config, Package, Point, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
}

type Pid = u32;

#[derive(Debug, Default, Serialize, Clone)]
pub struct System {
    sequence: u32,
    timestamp: u64,
    kernel_version: String,
    uptime: u64,
    no_processes: usize,
    /// Average load within one minute.
    load_avg_one: f64,
    /// Average load within five minutes.
    load_avg_five: f64,
    /// Average load within fifteen minutes.
    load_avg_fifteen: f64,
    total_memory: u64,
    available_memory: u64,
    used_memory: u64,
}

impl System {
    fn init(sys: &sysinfo::System) -> System {
        System {
            kernel_version: match sys.kernel_version() {
                Some(kv) => kv,
                None => String::default(),
            },
            total_memory: sys.total_memory(),
            ..Default::default()
        }
    }

    fn update(&mut self, sys: &sysinfo::System, timestamp: u64) {
        self.sequence += 1;
        self.timestamp = timestamp;
        self.uptime = sys.uptime();
        self.no_processes = sys.processes().len();
        let sysinfo::LoadAvg { one, five, fifteen } = sys.load_average();
        self.load_avg_one = one;
        self.load_avg_five = five;
        self.load_avg_fifteen = fifteen;
        self.available_memory = sys.available_memory();
        self.used_memory = self.total_memory - self.available_memory;
    }
}

impl Point for System {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<System> {
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

struct SystemStats {
    stat: System,
    stream: Stream<System>,
}

impl SystemStats {
    fn push(&mut self, sys: &sysinfo::System, timestamp: u64) -> Result<(), base::Error> {
        self.stat.update(sys, timestamp);
        self.stream.push(self.stat.clone())
    }
}

#[derive(Debug, Serialize, Clone)]
struct Network {
    sequence: u32,
    timestamp: u64,
    name: String,
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
    #[serde(skip_serializing)]
    timer: Instant,
}

impl Network {
    fn init(name: String) -> Self {
        Network {
            sequence: 0,
            timestamp: 0,
            name,
            incoming_data_rate: 0.0,
            outgoing_data_rate: 0.0,
            timer: Instant::now(),
        }
    }

    /// Update metrics values for network usage over time
    fn update(&mut self, data: &NetworkData, timestamp: u64, sequence: u32) {
        let update_period = self.timer.elapsed().as_secs_f64();
        self.incoming_data_rate = data.total_received() as f64 / update_period;
        self.outgoing_data_rate = data.total_transmitted() as f64 / update_period;
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

struct NetworkStats {
    sequence: u32,
    map: HashMap<String, Network>,
    stream: Stream<Network>,
}

impl NetworkStats {
    fn push(
        &mut self,
        net_name: String,
        net_data: &sysinfo::NetworkData,
        timestamp: u64,
    ) -> Result<(), base::Error> {
        self.sequence += 1;
        let net = self.map.entry(net_name.clone()).or_insert(Network::init(net_name));
        net.update(net_data, timestamp, self.sequence);

        self.stream.push(net.clone())
    }
}

#[derive(Debug, Serialize, Default, Clone)]
struct Disk {
    sequence: u32,
    timestamp: u64,
    name: String,
    total: u64,
    available: u64,
    used: u64,
}

impl Disk {
    fn init(name: String, disk: &sysinfo::Disk) -> Self {
        Disk { name, total: disk.total_space(), ..Default::default() }
    }

    fn update(&mut self, disk: &sysinfo::Disk, timestamp: u64, sequence: u32) {
        self.available = disk.available_space();
        self.used = self.total - self.available;
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

struct DiskStats {
    sequence: u32,
    map: HashMap<String, Disk>,
    stream: Stream<Disk>,
}

impl DiskStats {
    fn push(&mut self, disk_data: &sysinfo::Disk, timestamp: u64) -> Result<(), base::Error> {
        self.sequence += 1;
        let disk_name = disk_data.name().to_string_lossy().to_string();
        let disk = self.map.entry(disk_name.clone()).or_insert(Disk::init(disk_name, disk_data));
        disk.update(disk_data, timestamp, self.sequence);

        self.stream.push(disk.clone())
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

struct ProcessorStats {
    sequence: u32,
    map: HashMap<String, Processor>,
    stream: Stream<Processor>,
}

impl ProcessorStats {
    fn push(&mut self, proc_data: &sysinfo::Processor, timestamp: u64) -> Result<(), base::Error> {
        let proc_name = proc_data.name().to_string();
        self.sequence += 1;
        let proc = self.map.entry(proc_name.clone()).or_insert(Processor::init(proc_name));
        proc.update(proc_data, timestamp, self.sequence);

        self.stream.push(proc.clone())
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Process {
    sequence: u32,
    timestamp: u64,
    pid: Pid,
    name: String,
    cpu_usage: f32,
    mem_usage: u64,
    disk_total_written_bytes: u64,
    disk_written_bytes: u64,
    disk_total_read_bytes: u64,
    disk_read_bytes: u64,
    start_time: u64,
}

impl Process {
    fn init(pid: Pid, name: String, start_time: u64) -> Self {
        Process { pid, name, start_time, ..Default::default() }
    }

    fn update(&mut self, proc: &sysinfo::Process, timestamp: u64, sequence: u32) {
        let sysinfo::DiskUsage { total_written_bytes, written_bytes, total_read_bytes, read_bytes } =
            proc.disk_usage();
        self.disk_total_written_bytes = total_written_bytes;
        self.disk_written_bytes = written_bytes;
        self.disk_total_read_bytes = total_read_bytes;
        self.disk_read_bytes = read_bytes;
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

struct ProcessStats {
    sequence: u32,
    map: HashMap<Pid, Process>,
    stream: Stream<Process>,
}

impl ProcessStats {
    fn push(
        &mut self,
        id: Pid,
        proc_data: &sysinfo::Process,
        name: String,
        timestamp: u64,
    ) -> Result<(), base::Error> {
        self.sequence += 1;
        let proc = self.map.entry(id).or_insert(Process::init(id, name, proc_data.start_time()));
        proc.update(proc_data, timestamp, self.sequence);

        self.stream.push(proc.clone())
    }
}

/// Collects and forward system information such as kernel version, memory and disk space usage,
/// information regarding running processes, network and processor usage, etc to an IoT platform.
pub struct StatCollector {
    /// Handle to sysinfo struct containing system information.
    sys: sysinfo::System,
    /// Frequently updated timestamp value, used to ensure idempotence.
    timestamp: u64,
    /// System information values to be serialized.
    system: SystemStats,
    /// Information about running processes.
    processes: ProcessStats,
    /// Individual Processor information.
    processors: ProcessorStats,
    /// Information regarding individual Network interfaces.
    networks: NetworkStats,
    /// Information regarding individual Disks.
    disks: DiskStats,
    /// Uplink configuration.
    config: Arc<Config>,
}

impl StatCollector {
    /// Create and initialize a stat collector
    pub fn new(config: Arc<Config>, tx: Sender<Box<dyn Package>>) -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_disks_list();
        sys.refresh_networks_list();
        sys.refresh_memory();

        let max_buf_size = match config.stats.stream_size {
            Some(stream_size) => stream_size,
            None => 10,
        };

        let mut map = HashMap::new();
        let stream = Stream::dynamic_with_size(
            "uplink_disk_stats",
            &config.project_id,
            &config.device_id,
            max_buf_size,
            tx.clone(),
        );
        for disk_data in sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            map.insert(disk_name.clone(), Disk::init(disk_name, disk_data));
        }
        let disks = DiskStats { sequence: 0, map, stream };

        let mut map = HashMap::new();
        let stream = Stream::dynamic_with_size(
            "uplink_network_stats",
            &config.project_id,
            &config.device_id,
            max_buf_size,
            tx.clone(),
        );
        for (net_name, _) in sys.networks() {
            map.insert(net_name.to_owned(), Network::init(net_name.to_owned()));
        }
        let networks = NetworkStats { sequence: 0, map, stream };

        let mut map = HashMap::new();
        let stream = Stream::dynamic_with_size(
            "uplink_processor_stats",
            &config.project_id,
            &config.device_id,
            max_buf_size,
            tx.clone(),
        );
        for proc in sys.processors().iter() {
            let proc_name = proc.name().to_owned();
            map.insert(proc_name.clone(), Processor::init(proc_name));
        }
        let processors = ProcessorStats { sequence: 0, map, stream };

        let stream = Stream::dynamic_with_size(
            "uplink_process_stats",
            &config.project_id,
            &config.device_id,
            max_buf_size,
            tx.clone(),
        );
        let processes = ProcessStats { sequence: 0, map: HashMap::new(), stream };

        let stream = Stream::dynamic_with_size(
            "uplink_system_stats",
            &config.project_id,
            &config.device_id,
            max_buf_size,
            tx.clone(),
        );
        let system = SystemStats { stat: System::init(&sys), stream };

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        StatCollector { sys, system, config, processes, disks, networks, processors, timestamp }
    }

    /// Stat collector execution loop, sleeps for the duation of `config.stats.update_period` in seconds.
    pub fn start(mut self) {
        loop {
            std::thread::sleep(Duration::from_secs(self.config.stats.update_period));
            self.timestamp += self.config.stats.update_period;

            if let Err(e) = self.update() {
                error!("Faced error while refreshing system statistics: {}", e);
                return;
            };
        }
    }

    /// Update system information values and increment sequence numbers, while sending to specific data streams.
    fn update(&mut self) -> Result<(), Error> {
        if let Err(e) = self.system.push(&self.sys, self.timestamp) {
            error!("Couldn't send system stats: {}", e);
        }
        self.sys.refresh_memory();

        // Refresh disk info
        for disk_data in self.sys.disks() {
            if let Err(e) = self.disks.push(disk_data, self.timestamp) {
                error!("Couldn't send disk stats: {}", e);
            }
        }
        self.sys.refresh_disks();

        // Refresh network byte rate info
        for (net_name, net_data) in self.sys.networks() {
            if let Err(e) = self.networks.push(net_name.to_owned(), net_data, self.timestamp) {
                error!("Couldn't send network stats: {}", e);
            }
        }
        self.sys.refresh_networks();

        // Refresh processor info
        for proc_data in self.sys.processors().iter() {
            if let Err(e) = self.processors.push(proc_data, self.timestamp) {
                error!("Couldn't send processor stats: {}", e);
            }
        }
        self.sys.refresh_cpu();

        // Refresh processes info
        // NOTE: This can be further optimized by storing pids of interested processes
        // at init and only collecting process information for them instead of iterating
        // over all running processes as is being done now.
        for (&id, p) in self.sys.processes() {
            let name = p.name().to_owned();

            if self.config.stats.process_names.contains(&name) {
                if let Err(e) = self.processes.push(id.as_u32(), p, name, self.timestamp) {
                    error!("Couldn't send process stats: {}", e);
                }
            }
        }
        self.sys.refresh_processes();

        Ok(())
    }
}
