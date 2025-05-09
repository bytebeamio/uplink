use std::collections::HashMap;
use std::time::Duration;
use anyhow::Context;
use flume::Sender;
use serde::Deserialize;
use serde_json::json;
use crate::base::bridge::Payload;
use crate::base::clock;
use crate::uplink_config::DockerStatsConfig;

pub struct DockerStatsCollector {
    pub config: DockerStatsConfig,
    pub docker_client: reqwest::Client,
    pub previous_network_stats: HashMap<String, HashMap<String, NetworkStats>>,
    pub data_tx: Sender<Payload>,
    pub sequence: u32,
}

impl DockerStatsCollector {
    pub async fn new(config: DockerStatsConfig, data_tx: Sender<Payload>) -> anyhow::Result<Self> {
        let docker_client = reqwest::Client::builder()
            .unix_socket("/var/run/docker.sock")
            .build()?;
        let mut previous_network_stats = HashMap::new();
        let containers = Self::get_running_containers(&docker_client).await?;
        for container in containers.into_iter() {
            let stats = Self::get_container_stats(&docker_client, &container).await?;
            previous_network_stats.insert(container, stats.networks);
        }
        Ok(Self { config, docker_client, previous_network_stats, data_tx, sequence: 0 })
    }
    
    pub async fn start(mut self) {
        loop {
            if let Err(e) = self.do_stuff().await {
                log::error!("couldn't fetch docker stas: {e:?}");
            }
            tokio::time::sleep(self.config.interval).await;
        }
    }

    async fn do_stuff(&mut self) -> anyhow::Result<()> {
        let containers = Self::get_running_containers(&self.docker_client).await
            .context("can't fetch running containers")?;
        for container in containers.into_iter() {
            let new_stats = Self::get_container_stats(&self.docker_client, &container).await?;
            let mut network_tx = 0;
            let mut network_rx = 0;
            if let Some(prev_network_stats) = self.previous_network_stats.get(&container) {
                for (iface, old_stats) in prev_network_stats.iter() {
                    if let Some(new_stats) = new_stats.networks.get(iface) {
                        network_tx += new_stats.tx_bytes - old_stats.tx_bytes;
                        network_rx += new_stats.rx_bytes - old_stats.rx_bytes;
                    }
                }
            }
            let payload = json!({
                "container_name": container,
                "used_pids": new_stats.pid_stats.current,
                "max_pids": new_stats.pid_stats.limit,
                "percentage_pids_usage": new_stats.pid_stats.current as f64 / new_stats.pid_stats.limit as f64 * 100.0,
                "used_memory": new_stats.memory_stats.usage,
                "max_memory": new_stats.memory_stats.limit,
                "percentage_memory_usage": new_stats.memory_stats.usage as f64 / new_stats.memory_stats.limit as f64 * 100.0,
                "percentage_cpu_usage": new_stats.cpu_usage_percentage,
                "network_tx": network_tx,
                "network_rx": network_rx,
            });
            self.previous_network_stats.insert(container, new_stats.networks);
            self.sequence += 1;
            self.data_tx.send(Payload {
                stream: "docker_container_stats".to_string(),
                sequence: self.sequence,
                timestamp: clock() as _,
                payload,
            })?;
        }

        Ok(())
    }
    
    async fn get_running_containers(client: &reqwest::Client) -> anyhow::Result<Vec<String>> {
        let response = client.get("http://localhost/containers/json")
            .send().await
            .context("couldn't connect to docker daemon")?
            .bytes().await
            .context("couldn't connect to docker daemon")?;
        #[derive(Deserialize)]
        struct GetContainersResponse {
            Names: Vec<String>,
        }
        let response = serde_json::from_slice::<Vec<GetContainersResponse>>(&response)
            .context("invalid '/containers/json' response from docker daemon")?;
        Ok(response.into_iter()
            .filter(|r| !r.Names.is_empty())
            .map(|r| r.Names.into_iter().next().unwrap())
            .map(|name| if let Some('/') = name.chars().next() { name[1..].to_owned() } else { name })
            .collect())
    }
    
    async fn get_container_stats(client: &reqwest::Client, name: &str) -> anyhow::Result<ContainerStats> {
        let response = client.get(format!("http://localhost/containers/{name}/stats?stream=0"))
            .send().await
            .context("couldn't connect to docker daemon")?
            .bytes().await
            .context("couldn't connect to docker daemon")?;
        let response = std::str::from_utf8(&response)
            .context("invalid utf8 received in '/containers/{name}/stats' response from docker daemon")?;
        let response = serde_json::from_str::<ContainerStatsResponse>(response)
            .context(format!("invalid '/containers/{name}/stats' response from docker daemon: {response:?}"))?;
        Ok(ContainerStats {
            pid_stats: response.pids_stats,
            memory_stats: response.memory_stats,
            cpu_usage_percentage: (response.cpu_stats.cpu_usage.total_usage - response.precpu_stats.cpu_usage.total_usage) as f64
                / (response.cpu_stats.system_cpu_usage - response.precpu_stats.system_cpu_usage) as f64
                * 100.0,
            networks: response.networks,
        })
    }
}

struct ContainerStats {
    pid_stats: PidStats,
    memory_stats: MemoryStats,
    cpu_usage_percentage: f64,
    networks: HashMap<String, NetworkStats>
}

#[derive(Deserialize)]
struct ContainerStatsResponse {
    pids_stats: PidStats,
    memory_stats: MemoryStats,
    cpu_stats: CpuStats,
    precpu_stats: CpuStats,
    networks: HashMap<String, NetworkStats>,
}

#[derive(Deserialize)]
struct PidStats {
    current: u64,
    limit: u64,
}

#[derive(Deserialize)]
struct MemoryStats {
    usage: u64,
    limit: u64,
}

#[derive(Deserialize)]
struct CpuStats {
    cpu_usage: CpuUsage,
    system_cpu_usage: u128,
}
#[derive(Deserialize)]
struct CpuUsage {
    total_usage: u128,
}

#[derive(Deserialize)]
struct NetworkStats {
    rx_bytes: u64,
    tx_bytes: u64,
}