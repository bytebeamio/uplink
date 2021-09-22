use anyhow::{Context, Error};
use figment::providers::{Data, Json, Toml};
use figment::Figment;
use simplelog::{CombinedLogger, LevelFilter, LevelPadding, TermLogger, TerminalMode};
use structopt::StructOpt;

use std::{fs, sync::Arc};

use crate::base::Config;

const B: &str = r#"
    ░█░▒█░▄▀▀▄░█░░░▀░░█▀▀▄░█░▄
    ░█░▒█░█▄▄█░█░░░█▀░█░▒█░█▀▄
    ░░▀▀▀░█░░░░▀▀░▀▀▀░▀░░▀░▀░▀
"#;

const DEFAULT_CONFIG: &str = r#"
    bridge_port = 5555
    max_packet_size = 102400
    max_inflight = 100
    
    # Whitelist of binaries which uplink can spawn as a process
    # This makes sure that user is protected against random actions
    # triggered from cloud.
    actions = ["tunshell"]
    
    [persistence]
    path = "/tmp/uplink"
    max_file_size = 104857600 # 100MB
    max_file_count = 3
    
    [streams.metrics]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/metrics/jsonarray"
    buf_size = 10
    
    # Action status stream from status messages from bridge
    [streams.action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    buf_size = 1
"#;

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
    /// Binary's version
    #[structopt(skip = env!("VERGEN_BUILD_SEMVER"))]
    version: String,
    /// Build's profile
    #[structopt(skip= env!("VERGEN_CARGO_PROFILE"))]
    profile: String,
    /// SHA of commit
    #[structopt(skip= env!("VERGEN_GIT_SHA"))]
    commit_sha: String,
    /// Date of commit
    #[structopt(skip= env!("VERGEN_GIT_COMMIT_TIMESTAMP"))]
    commit_date: String,
    /// Path to config file
    #[structopt(short = "c", help = "Config file")]
    config: Option<String>,
    /// Path to authorization file
    #[structopt(short = "a", help = "Authentication file")]
    auth: String,
    /// Whether to use simulator
    #[structopt(short = "s", long = "Enable uplink simulator")]
    pub enable_simulator: bool,
    /// Log level / Verbosity (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "Verbosity of logger", parse(from_occurrences))]
    verbose: u8,
    /// List of modules to log
    #[structopt(short = "m", long = "Modules to be logged")]
    modules: Vec<String>,
}

impl CommandLine {
    /// Reads config file to generate config struct and replaces places holders
    /// for device id and data version
    pub fn initalize_config(&self) -> Result<Config, Error> {
        let mut config = Figment::new().merge(Data::<Toml>::string(DEFAULT_CONFIG));

        if let Some(c) = &self.config {
            config = config.merge(Data::<Toml>::file(c));
        }

        let mut config: Config = config
            .join(Data::<Json>::file(&self.auth))
            .extract()
            .with_context(|| "Config error".to_string())?;

        fs::create_dir_all(&config.persistence.path)?;

        let tenant_id = config.project_id.trim();
        let device_id = config.device_id.trim();
        for config in config.streams.values_mut() {
            let topic = str::replace(&config.topic, "{tenant_id}", tenant_id);
            config.topic = topic;

            let topic = str::replace(&config.topic, "{device_id}", device_id);
            config.topic = topic;
        }

        Ok(config)
    }

    /// Configure the commandline to use the proper verbosity for logging
    pub fn initialize_logging(&self) {
        let level = match self.verbose {
            0 => LevelFilter::Warn,
            1 => LevelFilter::Info,
            2 => LevelFilter::Debug,
            _ => LevelFilter::Trace,
        };

        let mut logger_config = simplelog::ConfigBuilder::new();
        logger_config
            .set_location_level(LevelFilter::Off)
            .set_target_level(LevelFilter::Error)
            .set_thread_level(LevelFilter::Error)
            .set_level_padding(LevelPadding::Right);

        if self.modules.is_empty() {
            logger_config.add_filter_allow_str("uplink").add_filter_allow_str("disk");
        } else {
            for module in self.modules.iter() {
                logger_config.add_filter_allow(module.to_string());
            }
        }

        let loggers = TermLogger::new(level, logger_config.build(), TerminalMode::Mixed);
        CombinedLogger::init(vec![loggers]).unwrap();
    }

    /// Print the banner with metadata from Uplink instance
    pub fn banner(&self, config: &Arc<Config>) {
        println!("{}", B);
        println!("    version: {}", self.version);
        println!("    profile: {}", self.profile);
        println!("    commit_sha: {}", self.commit_sha);
        println!("    commit_date: {}", self.commit_date);
        println!("    project_id: {}", config.project_id);
        println!("    device_id: {}", config.device_id);
        println!("    remote: {}:{}", config.broker, config.port);
        println!("    secure_transport: {}", config.authentication.is_some());
        println!("    max_packet_size: {}", config.max_packet_size);
        println!("    max_inflight_messages: {}", config.max_inflight);
        println!("    persistence_dir: {}", config.persistence.path);
        println!("    persistence_max_segment_size: {}", config.persistence.max_file_size);
        println!("    persistence_max_segment_count: {}", config.persistence.max_file_count);
        println!("\n");
    }
}
