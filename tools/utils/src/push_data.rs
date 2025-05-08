use futures_util::SinkExt;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::time::Duration;
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LinesCodec};
use uplink::base::clock;

#[derive(Debug, StructOpt)]
#[structopt(name = "random_sender", about = "Send random JSON messages over TCP.")]
struct Opt {
    #[structopt(long)]
    uplink_port: String,
    /// Time between messages in seconds (can be a float)
    #[structopt(long)]
    interval: f64,
    /// Stream name
    #[structopt(long)]
    stream: String,
    /// Data format as comma-separated field:type pairs,
    /// e.g., "f1:int,f2:float,f3:string,f4:iso date time"
    #[structopt(long)]
    format: String,
    #[structopt(short = "v", long = "verbose")]
    verbose: bool,
}

enum FieldType {
    Int,
    Float,
    String,
    IsoDateTime,
}

struct FieldSpec {
    name: String,
    ftype: FieldType,
}

/// Parses a format string like "f1:int,f2:float,f3:string,f4:iso date time"
/// and returns a vector of FieldSpec.
fn parse_format(format: &str) -> Vec<FieldSpec> {
    let mut specs = Vec::new();
    for token in format.split(',') {
        let parts: Vec<&str> = token.splitn(2, ':').collect();
        if parts.len() != 2 {
            panic!("Invalid field spec: {}", token);
        }
        let name = parts[0].trim().to_string();
        let type_str = parts[1].trim().to_lowercase();
        let ftype = if type_str.starts_with("int") {
            FieldType::Int
        } else if type_str.starts_with("float") {
            FieldType::Float
        } else if type_str.starts_with("string") {
            FieldType::String
        } else if type_str.contains("iso") {
            FieldType::IsoDateTime
        } else {
            panic!("Unknown type: {}", type_str);
        };
        specs.push(FieldSpec { name, ftype });
    }
    specs
}

#[tokio::main]
async fn main() {
    // Parse CLI arguments.
    let opt = Opt::from_args();
    let interval_duration = Duration::from_secs_f64(opt.interval);
    let field_specs = parse_format(&opt.format);

    // Connect to the TCP server (using the default "127.0.0.1:5050").
    let stream = TcpStream::connect(&opt.uplink_port).await.unwrap();
    let mut framed = Framed::new(stream, LinesCodec::new());

    let mut seq: u64 = 0;
    loop {
        seq += 1;
        let mut payload = serde_json::Map::new();
        // Always include the provided stream name and an incrementing sequence number.
        payload.insert("stream".to_string(), serde_json::Value::String(opt.stream.clone()));
        payload.insert("sequence".to_string(), serde_json::Value::Number(seq.into()));
        payload.insert("timestamp".to_string(), serde_json::Value::Number(serde_json::Number::from_u128(clock()).unwrap()));

        // For each field spec, generate a random value based on the type.
        for spec in &field_specs {
            let value = match spec.ftype {
                FieldType::Int => {
                    let v = rand::thread_rng().gen_range(0..100);
                    serde_json::Value::Number(v.into())
                }
                FieldType::Float => {
                    let v: f64 = rand::thread_rng().gen_range(0.0..100.0);
                    serde_json::Number::from_f64(v)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                }
                FieldType::String => {
                    let len = rand::thread_rng().gen_range(10..21);
                    let s: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(len)
                        .map(char::from)
                        .collect();
                    serde_json::Value::String(s)
                }
                FieldType::IsoDateTime => {
                    
                    let dt = Utc::now().to_rfc3339();
                    serde_json::Value::String(dt)
                }
            };
            payload.insert(spec.name.clone(), value);
        }

        let data_s = serde_json::to_string(&payload).unwrap();
        if opt.verbose {
            println!("{data_s}");
        }
        framed.send(data_s).await.unwrap();
        sleep(interval_duration).await;
    }
}
