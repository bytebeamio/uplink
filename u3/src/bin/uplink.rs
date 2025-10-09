use std::cmp::{max, min};
use log::error;
use std::time::{Duration, SystemTime};
use backtrace::Backtrace;
use structopt::StructOpt;
use tokio::select;
use u3::config::parse_config;
use u3::{uplink_task};
use u3::utils::num_cores;

fn main() {
    let args = Cli::from_args();
    initialize_logging(args.verbosity, args.log_filters_file_path);
    let (cfg, auth) = match parse_config(&args.config, &args.authentication) {
        Ok(r) => r,
        Err(e) => {
            error!("{e}");
            return;
        }
    };

    let (actions_tx, _actions_rx) = flume::bounded(8);
    let (_data_tx, data_rx) = flume::bounded(128);
    let task = Box::pin(uplink_task(cfg, auth, actions_tx, data_rx));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(max(4, num_cores()))
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");
    runtime.block_on(async move {
        select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = task => {}
        }
    });
}

#[derive(StructOpt)]
struct Cli {
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbosity: u8,
    #[structopt(long)]
    pub log_filters_file_path: Option<String>,
    #[structopt(short, long)]
    pub config: String,
    #[structopt(short, long)]
    pub authentication: String,
}

pub fn initialize_logging(verbosity: u8, file_path: Option<String>) {
    let filter_str = match verbosity {
        0 => "info,u3=warn",
        1 => "info,u3=info",
        2 => "info,u3=debug",
        _ => "info,u3=trace",
    };
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .compact()
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(filter_str)
        .with_filter_reloading();
    let reload_handle = builder.reload_handle();
    builder.try_init().unwrap();

    if let Some(file_path) = file_path {
        tokio::spawn(async move {
            if let Err(e) = std::fs::write(&file_path, filter_str) {
                println!("Couldn't write log filters file, log level cannot be reloaded: {e}");
                return;
            }

            let mut last_modified = SystemTime::now();
            loop {
                match std::fs::metadata(&file_path) {
                    Ok(metadata) => {
                        let modified = match metadata.modified() {
                            Ok(md) => md,
                            Err(e) => {
                                println!("couldn't read {file_path}: {e:?}");
                                continue;
                            }
                        };
                        if modified > last_modified {
                            last_modified = modified;
                            match std::fs::read_to_string(&file_path) {
                                Ok(new_filter_str) => {
                                    let new_filter =
                                        tracing_subscriber::EnvFilter::new(new_filter_str.trim());
                                    println!("reloading {file_path}");
                                    if let Err(e) = reload_handle.reload(new_filter) {
                                        println!("Failed to reload filter: {e:?}");
                                    }
                                }
                                Err(e) => {
                                    println!("Failed to read {file_path}: {e:?}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to get file metadata: {e:?}");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
