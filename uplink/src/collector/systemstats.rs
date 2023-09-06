use log::error;
use serde::Serialize;
use serde_json::json;
use sysinfo::{
    ComponentExt, CpuExt, DiskExt, NetworkData, NetworkExt, PidExt, ProcessExt, SystemExt,
};
use tokio::time::Instant;

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    base::{
        bridge::{BridgeTx, Payload},
        clock,
    },
    Config,
};

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

impl From<&System> for Payload {
    fn from(value: &System) -> Self {
        let System {
            sequence,
            timestamp,
            kernel_version,
            uptime,
            no_processes,
            load_avg_one,
            load_avg_five,
            load_avg_fifteen,
            total_memory,
            available_memory,
            used_memory,
        } = value;

        Payload {
            stream: "uplink_system_stats".to_owned(),
            sequence: *sequence,
            timestamp: *timestamp,
            payload: json!({
                "kernel_version": kernel_version,
                "uptime": uptime,
                "no_processes": no_processes,
                "load_avg_one": load_avg_one,
                "load_avg_five": load_avg_five,
                "load_avg_fifteen": load_avg_fifteen,
                "total_memory": total_memory,
                "available_memory": available_memory,
                "used_memory": used_memory,
            }),
        }
    }
}

struct SystemStats {
    stat: System,
}

impl SystemStats {
    fn push(&mut self, sys: &sysinfo::System, timestamp: u64) -> Payload {
        self.stat.update(sys, timestamp);

        (&self.stat).into()
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

impl From<&mut Network> for Payload {
    fn from(value: &mut Network) -> Self {
        let Network { sequence, timestamp, name, incoming_data_rate, outgoing_data_rate, .. } =
            value;

        Payload {
            stream: "uplink_network_stats".to_owned(),
            sequence: *sequence,
            timestamp: *timestamp,
            payload: json!({
                "name": name,
                "incoming_data_rate": incoming_data_rate,
                "outgoing_data_rate": outgoing_data_rate,
            }),
        }
    }
}

struct NetworkStats {
    sequence: u32,
    map: HashMap<String, Network>,
}

impl NetworkStats {
    fn push(
        &mut self,
        net_name: String,
        net_data: &sysinfo::NetworkData,
        timestamp: u64,
    ) -> Payload {
        self.sequence += 1;
        let net = self.map.entry(net_name.clone()).or_insert_with(|| Network::init(net_name));
        net.update(net_data, timestamp, self.sequence);

        net.into()
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
        self.total = disk.total_space();
        self.available = disk.available_space();
        self.used = self.total - self.available;
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl From<&mut Disk> for Payload {
    fn from(value: &mut Disk) -> Self {
        let Disk { sequence, timestamp, name, total, available, used } = value;

        Payload {
            stream: "uplink_disk_stats".to_owned(),
            sequence: *sequence,
            timestamp: *timestamp,
            payload: json!({
                "name": name,
                "total": total,
                "available": available,
                "used": used,
            }),
        }
    }
}

struct DiskStats {
    sequence: u32,
    map: HashMap<String, Disk>,
}

impl DiskStats {
    fn push(&mut self, disk_data: &sysinfo::Disk, timestamp: u64) -> Payload {
        self.sequence += 1;
        let disk_name = disk_data.name().to_string_lossy().to_string();
        let disk =
            self.map.entry(disk_name.clone()).or_insert_with(|| Disk::init(disk_name, disk_data));
        disk.update(disk_data, timestamp, self.sequence);

        disk.into()
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

    fn update(&mut self, proc: &sysinfo::Cpu, timestamp: u64, sequence: u32) {
        self.frequency = proc.frequency();
        self.usage = proc.cpu_usage();
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl From<&mut Processor> for Payload {
    fn from(value: &mut Processor) -> Self {
        let Processor { sequence, timestamp, name, frequency, usage } = value;

        Payload {
            stream: "uplink_processor_stats".to_owned(),
            sequence: *sequence,
            timestamp: *timestamp,
            payload: json!({
                "name": name,
                "frequency": frequency,
                "usage": usage,
            }),
        }
    }
}

struct ProcessorStats {
    sequence: u32,
    map: HashMap<String, Processor>,
}

impl ProcessorStats {
    fn push(&mut self, proc_data: &sysinfo::Cpu, timestamp: u64) -> Payload {
        let proc_name = proc_data.name().to_string();
        self.sequence += 1;
        let proc = self.map.entry(proc_name.clone()).or_insert_with(|| Processor::init(proc_name));
        proc.update(proc_data, timestamp, self.sequence);

        proc.into()
    }
}

#[derive(Debug, Default, Serialize, Clone)]
struct Component {
    sequence: u32,
    timestamp: u64,
    label: String,
    temperature: f32,
}

impl Component {
    fn init(label: String) -> Self {
        Component { label, ..Default::default() }
    }

    fn update(&mut self, comp: &sysinfo::Component, timestamp: u64, sequence: u32) {
        self.temperature = comp.temperature();
        self.timestamp = timestamp;
        self.sequence = sequence;
    }
}

impl From<&mut Component> for Payload {
    fn from(value: &mut Component) -> Self {
        let Component { sequence, timestamp, label, temperature } = value;

        Payload {
            stream: "uplink_component_stats".to_owned(),
            sequence: *sequence,
            timestamp: *timestamp,
            payload: json!({
                "label": label, "temperature": temperature
            }),
        }
    }
}

struct ComponentStats {
    sequence: u32,
    map: HashMap<String, Component>,
}

impl ComponentStats {
    fn push(&mut self, comp_data: &sysinfo::Component, timestamp: u64) -> Payload {
        let comp_label = comp_data.label().to_string();
        self.sequence += 1;
        let comp =
            self.map.entry(comp_label.clone()).or_insert_with(|| Component::init(comp_label));
        comp.update(comp_data, timestamp, self.sequence);

        comp.into()
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

impl From<&mut Process> for Payload {
    fn from(value: &mut Process) -> Self {
        let Process {
            sequence,
            timestamp,
            pid,
            name,
            cpu_usage,
            mem_usage,
            disk_total_written_bytes,
            disk_written_bytes,
            disk_total_read_bytes,
            disk_read_bytes,
            start_time,
        } = value;

        Payload {
            stream: "uplink_process_stats".to_owned(),
            sequence: *sequence,
            timestamp: *timestamp,
            payload: json!({
                "pid": pid,
                "name": name,
                "cpu_usage": cpu_usage,
                "mem_usage": mem_usage,
                "disk_total_written_bytes": disk_total_written_bytes,
                "disk_written_bytes": disk_written_bytes,
                "disk_total_read_bytes": disk_total_read_bytes,
                "disk_read_bytes": disk_read_bytes,
                "start_time": start_time,
            }),
        }
    }
}

struct ProcessStats {
    sequence: u32,
    map: HashMap<Pid, Process>,
}

impl ProcessStats {
    fn push(
        &mut self,
        id: Pid,
        proc_data: &sysinfo::Process,
        name: String,
        timestamp: u64,
    ) -> Payload {
        self.sequence += 1;
        let proc =
            self.map.entry(id).or_insert_with(|| Process::init(id, name, proc_data.start_time()));
        proc.update(proc_data, timestamp, self.sequence);

        proc.into()
    }
}

/// Collects and forward system information such as kernel version, memory and disk space usage,
/// information regarding running processes, network and processor usage, etc to an IoT platform.
pub struct StatCollector {
    /// Handle to sysinfo struct containing system information.
    sys: sysinfo::System,
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
    /// Temperature information from individual components.
    components: ComponentStats,
    /// Uplink configuration.
    config: Arc<Config>,
    /// Handle to send stats as payload onto bridge
    bridge_tx: BridgeTx,
}

impl StatCollector {
    /// Create and initialize a stat collector
    pub fn new(config: Arc<Config>, bridge_tx: BridgeTx) -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_disks_list();
        sys.refresh_networks_list();
        sys.refresh_memory();
        sys.refresh_cpu();
        sys.refresh_components();

        let mut map = HashMap::new();
        for disk_data in sys.disks() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            map.insert(disk_name.clone(), Disk::init(disk_name, disk_data));
        }
        let disks = DiskStats { sequence: 0, map };

        let mut map = HashMap::new();
        for (net_name, _) in sys.networks() {
            map.insert(net_name.to_owned(), Network::init(net_name.to_owned()));
        }
        let networks = NetworkStats { sequence: 0, map };

        let mut map = HashMap::new();
        for proc in sys.cpus().iter() {
            let proc_name = proc.name().to_owned();
            map.insert(proc_name.clone(), Processor::init(proc_name));
        }
        let processors = ProcessorStats { sequence: 0, map };

        let processes = ProcessStats { sequence: 0, map: HashMap::new() };
        let components = ComponentStats { sequence: 0, map: HashMap::new() };

        let system = SystemStats { stat: System::init(&sys) };

        StatCollector {
            sys,
            system,
            config,
            processes,
            disks,
            networks,
            processors,
            components,
            bridge_tx,
        }
    }

    /// Stat collector execution loop, sleeps for the duation of `config.stats.update_period` in seconds.
    /// Update system information values and increment sequence numbers, while sending to specific data streams.
    pub fn start(mut self) {
        loop {
            std::thread::sleep(Duration::from_secs(self.config.system_stats.update_period));

            if let Err(e) = self.update_memory_stats() {
                error!("Error refreshing system memory statistics: {}", e);
            }

            if let Err(e) = self.update_disk_stats() {
                error!("Error refreshing disk statistics: {}", e);
            }

            if let Err(e) = self.update_network_stats() {
                error!("Error refreshing network statistics: {}", e);
            }

            if let Err(e) = self.update_cpu_stats() {
                error!("Error refreshing CPU statistics: {}", e);
            }

            if let Err(e) = self.update_component_stats() {
                error!("Error refreshing component statistics: {}", e);
            }

            if let Err(e) = self.update_process_stats() {
                error!("Error refreshing process statistics: {}", e);
            }
        }
    }

    // Refresh memory stats
    fn update_memory_stats(&mut self) -> Result<(), Error> {
        self.sys.refresh_memory();
        let timestamp = clock() as u64;
        let payload = self.system.push(&self.sys, timestamp);
        self.bridge_tx.send_payload_sync(payload);

        Ok(())
    }

    // Refresh disk stats
    fn update_disk_stats(&mut self) -> Result<(), Error> {
        self.sys.refresh_disks();
        let timestamp = clock() as u64;
        for disk_data in self.sys.disks() {
            let payload = self.disks.push(disk_data, timestamp);
            self.bridge_tx.send_payload_sync(payload);
        }

        Ok(())
    }

    // Refresh network byte rate stats
    fn update_network_stats(&mut self) -> Result<(), Error> {
        self.sys.refresh_networks();
        let timestamp = clock() as u64;
        for (net_name, net_data) in self.sys.networks() {
            let payload = self.networks.push(net_name.to_owned(), net_data, timestamp);
            self.bridge_tx.send_payload_sync(payload);
        }

        Ok(())
    }

    // Refresh processor stats
    fn update_cpu_stats(&mut self) -> Result<(), Error> {
        self.sys.refresh_cpu();
        let timestamp = clock() as u64;
        for proc_data in self.sys.cpus().iter() {
            let payload = self.processors.push(proc_data, timestamp);
            self.bridge_tx.send_payload_sync(payload);
        }

        Ok(())
    }

    // Refresh component stats
    fn update_component_stats(&mut self) -> Result<(), Error> {
        self.sys.refresh_components();
        let timestamp = clock() as u64;
        for comp_data in self.sys.components().iter() {
            let payload = self.components.push(comp_data, timestamp);
            self.bridge_tx.send_payload_sync(payload);
        }

        Ok(())
    }

    // Refresh processes info
    // NOTE: This can be further optimized by storing pids of interested processes
    // at init and only collecting process information for them instead of iterating
    // over all running processes as is being done now.
    fn update_process_stats(&mut self) -> Result<(), Error> {
        self.sys.refresh_processes();
        let timestamp = clock() as u64;
        for (&id, p) in self.sys.processes() {
            let name = p.cmd().get(0).map(|s| s.to_string()).unwrap_or(p.name().to_string());

            if self.config.system_stats.process_names.contains(&name) {
                let payload = self.processes.push(id.as_u32(), p, name, timestamp);
                self.bridge_tx.send_payload_sync(payload);
            }
        }

        Ok(())
    }
}
