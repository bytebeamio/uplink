use lz4_flex::frame::FrameEncoder;
use rand::{Rng};
use serde_json::{json, Value};
use std::io::Write;
use rand::distr::Alphanumeric;

fn lz4_compress(payload: &mut Vec<u8>) {
    let mut compressor = FrameEncoder::new(vec![]);
    compressor.write_all(payload).unwrap();
    *payload = compressor.finish().unwrap();
}

fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn main() {
    let mut rng = rand::thread_rng();
    let mut items = Vec::with_capacity(1000);

    for i in 0..1000 {
        let cat = ["alpha","beta","gamma","delta"][rng.gen_range(0..4)];
        let st = ["ok","warn","fail"][rng.gen_range(0..3)];
        let mut obj = json!({
            "id": i,
            "name": format!("device-{}", random_string(6)),
            "active": rng.gen_bool(0.7),
            "count": rng.gen_range(0..1000),
            "score": rng.gen_range(0.0..9999.0),
            "timestamp": format!("2025-10-{:02}T{:02}:{:02}:{:02}Z", rng.gen_range(1..=30), rng.gen_range(0..24), rng.gen_range(0..60), rng.gen_range(0..60)),
            "category": cat,
            "location": format!("zone-{}", rng.gen_range(1..6)),
            "temperature": rng.gen_range(-10.0..40.0),
            "status": st
        });

        if let Value::Object(ref mut map) = obj {
            for key in ["count", "score", "temperature", "status", "category"] {
                if rng.gen_bool(0.2) {
                    map.remove(key);
                }
            }
        }

        items.push(obj);
    }

    let json_string = serde_json::to_string(&items).unwrap();
    let mut payload = json_string.into_bytes();
    let original_size = payload.len();

    lz4_compress(&mut payload);

    let compressed_size = payload.len();
    let ratio = compressed_size as f64 / original_size as f64;

    println!(
        "original: {} bytes, compressed: {} bytes, ratio: {:.4} ({:.2}%)",
        original_size,
        compressed_size,
        ratio,
        ratio * 100.0
    );
}
