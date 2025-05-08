use std::collections::HashMap;
use std::thread::sleep;
use serde::Deserialize;
use crate::uplink_config::DockerStatsConfig;

pub struct DockerStatsCollector {
    pub config: DockerStatsConfig,
    pub docker_client: reqwest::Client,
}

impl DockerStatsCollector {
    pub fn new(config: DockerStatsConfig) -> anyhow::Result<Self> {
        let docker_client = reqwest::Client::builder()
            .unix_socket("/var/run/docker.sock")
            .build()?;
        Ok(Self { config, docker_client })
    } 
    
    
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        loop {
            
            tokio::time::sleep(self.config.interval).await;
        }
    }
    
    async fn get_running_containers(&self) -> anyhow::Result<Vec<String>> {
        let response = self.docker_client.get("http://localhost/containers/json")
            .send().await?
            .bytes().await?;
        #[derive(Deserialize)]
        struct GetContainersResponse {
            Names: Vec<String>,
        }
        let response = serde_json::from_slice::<Vec<GetContainersResponse>>(&response)?;
        Ok(response.into_iter()
            .filter(|r| !r.Names.is_empty())
            .map(|r| r.Names.into_iter().next().unwrap())
            .collect())
    }
    
    async fn get_container_stats(&self, name: &str) -> anyhow::Result<Vec<String>> {
        let response = self.docker_client.get(format!("http://localhost/containers/{name}/stats?stream=0"))
            .send().await?
            .bytes().await?;
        let response = serde_json::from_slice::<ContainerStatsResponse>(&response)?;

        //
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