pub mod delaymap;
pub mod array_map;

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

pub fn byte_offset_to_position(
    content: &str,
    offset: usize,
) -> Result<(usize, usize), &'static str> {
    let mut line = 1;
    let mut column = 1;
    let mut current_byte = 0;

    for c in content.chars() {
        if current_byte < offset {
            if c == '\n' {
                line += 1;
                column = 1;
            } else if c != '\r' {
                column += 1;
            }
        }

        current_byte += c.len_utf8();
    }

    Ok((line, column))
}

pub fn clock() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub mod path_parser {
    use serde::Serialize;
    use serde::de::Deserialize;
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use std::path::PathBuf;

    pub fn serialize<S>(p: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        p.as_ref().map(|p| p.as_os_str()).serialize(serializer)
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Some(PathBuf::deserialize(deserializer)?))
    }
}

pub fn num_cores() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

pub struct TaskManager<T>(pub JoinHandle<T>);
impl<T> TaskManager<T> {
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(future);
        Self(handle)
    }
}

impl<T> Drop for TaskManager<T> {
    fn drop(&mut self) {
        if !self.0.is_finished() {
            self.0.abort();
        }
    }
}
