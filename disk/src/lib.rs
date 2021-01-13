use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub struct AsyncClient {
    client: rumqttc::AsyncClient,
    index_file: File,
    /// list of backlog file ids. Mutated only be the serialization part of the sender
    backlog_file_ids: Vec<u64>,
    /// persistence path
    backup_path: PathBuf,
    /// maximum allowed file size
    max_file_size: usize,
    /// maximum number of files before deleting old file
    max_file_count: usize,
    /// current open file
    current_write_file: Option<File>,
    /// current_read_file
    current_read_file: Option<File>,
    /// current read file index. pointer into current reader id index in backlog_file_ids
    current_read_file_index: Option<usize>,
    /// size of file that's being filled
    current_write_file_size: usize,
    /// flag to detect slow receiver
    slow_receiver: bool,
}

#[derive(Debug)]
struct File {
    path: PathBuf,
    fd: fs::File,
}

impl File {
    pub fn new(path: PathBuf, fd: fs::File) -> File {
        File { path, fd }
    }
}

/// Gets list of file ids in the disk. Id of file backup@10 is 10. Storing ids instead of full
/// paths enables efficient indexing
pub fn get_file_ids(path: &Path) -> io::Result<Vec<u64>> {
    let mut file_ids = Vec::new();
    let files = fs::read_dir(path)?;
    for file in files.into_iter() {
        let path = file?.path();

        // ignore directories
        if path.is_dir() {
            continue;
        }

        if let Some(file_name) = path.file_name() {
            let file_name = format!("{:?}", file_name);
            if !file_name.contains("backup@") {
                continue;
            }
        }

        let id: Vec<&str> = path.file_name().unwrap().to_str().unwrap().split("@").collect();
        let id: u64 = id[1].parse().unwrap();

        file_ids.push(id);
    }

    file_ids.sort();
    Ok(file_ids)
}

/// Parses and gets meaningful data from the index file
fn parse_index_file(backup_path: &Path) -> io::Result<(File, Option<usize>)> {
    let index_file_path = backup_path.join("backlog.idx");
    let index_file = OpenOptions::new().write(true).create(true).open(&index_file_path)?;

    let index = fs::read_to_string(&index_file_path)?;
    let index_file = File::new(index_file_path, index_file);

    let reader_id = match index.as_str() {
        "" => return Ok((index_file, None)),
        number => number,
    };

    let id = match reader_id.parse::<usize>() {
        Ok(id) => id,
        Err(_) => panic!("Invalid index file. Expecting empty or number. Found {}", reader_id),
    };

    Ok((index_file, Some(id)))
}
