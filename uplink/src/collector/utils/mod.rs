mod delaymap;
mod streams;

use std::time::{SystemTime, UNIX_EPOCH};

pub use streams::Streams;

pub fn clock() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}
