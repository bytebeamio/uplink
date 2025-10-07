pub mod delaymap;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn byte_offset_to_position(content: &str, offset: usize) -> Result<(usize, usize), &'static str> {
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
    use std::path::PathBuf;
    use serde::de::Deserializer;
    use serde::de::Deserialize;
    use serde::ser::Serializer;
    use serde::Serialize;

    pub fn serialize<S>(p: &PathBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        p.as_os_str().serialize(serializer)
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(PathBuf::deserialize(deserializer)?)
    }
}
