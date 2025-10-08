use log::error;
use std::time::{Duration, SystemTime};
use backtrace::Backtrace;
use structopt::StructOpt;
use tokio::select;
use u3::config::parse_config;

#[tokio::main]
async fn main() {
    // clean_stacktraces();
    let args = Cli::from_args();
    initialize_logging(args.verbosity, args.log_filters_file_path);
    let (cfg, auth) = match parse_config(&args.config, &args.authentication) {
        Ok(r) => r,
        Err(e) => {
            error!("{e}");
            return;
        }
    };

    let (_action_rx, _data_tx, task) = u3::start_uplink(cfg, auth);
    let task = Box::into_pin(task);
    select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = task => {}
    }
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
        0 => "info,platform=warn",
        1 => "info,platform=info",
        2 => "info,platform=debug",
        _ => "info,platform=trace",
    };
    if let Some(file_path) = file_path.as_ref() {
        if let Err(e) = std::fs::write(file_path, filter_str) {
            println!("Couldn't write log filters file, log level cannot be reloaded: {e}")
        }
    }
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(filter_str)
        .with_filter_reloading();
    let reload_handle = builder.reload_handle();
    builder.try_init().unwrap();

    if let Some(file_path) = file_path {
        tokio::spawn(async move {
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

pub fn clean_stacktraces() {
    std::panic::set_hook(Box::new(move |info| {
        let orig = format!("{:?}", Backtrace::new());
        println!("{orig}");
        log::error!(
            "=> panic occurred: {}\n=> backtrace:\n{}",
            info,
            clean_stacktrace_impl(&orig)
        );
    }));
}

fn clean_stacktrace_impl(s: &str) -> String {
    let lines = s.lines().collect::<Vec<_>>();
    let mut resp = String::new();
    let mut first = true;
    for idx in 0..lines.len() {
        let line = lines[idx];
        if line.trim().starts_with("at ") && line.contains("/uplink/") {
            if first {
                first = false;
            } else {
                if idx > 0 {
                    resp.push_str(lines[idx - 1]);
                    resp.push('\n');
                }
                resp.push_str(line);
                resp.push('\n');
            }
        }
    }
    resp
}
