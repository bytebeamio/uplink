use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

mod storage;
pub use storage::Storage;

struct NextFile {
    path: PathBuf,
    file: File,
    deleted: Option<u64>,
}

/// Converts file path to file id
fn id(path: &Path) -> io::Result<u64> {
    if let Some(file_name) = path.file_name() {
        let file_name = format!("{:?}", file_name);
        if !file_name.contains("backup@") {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Not a backup file"));
        }
    }

    let id: Vec<&str> = path.file_name().unwrap().to_str().unwrap().split('@').collect();
    let id: u64 = id[1].parse().unwrap();
    Ok(id)
}

/// Gets list of file ids in the disk. Id of file backup@10 is 10.
/// Storing ids instead of full paths enables efficient indexing
fn get_file_ids(path: &Path) -> io::Result<Vec<u64>> {
    let mut file_ids = Vec::new();
    let files = fs::read_dir(path)?;
    for file in files.into_iter() {
        let path = file?.path();

        // ignore directories
        if path.is_dir() {
            continue;
        }

        match id(&path) {
            Ok(id) => file_ids.push(id),
            Err(_) => continue,
        }
    }

    file_ids.sort_unstable();
    Ok(file_ids)
}
