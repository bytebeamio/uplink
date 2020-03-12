#[macro_use]
extern crate log;

use derive_more::From;

use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;
use std::path::PathBuf;

use tokio::sync::mpsc;

mod sender;

#[derive(Debug, From)]
pub enum Error<T> {
    NonDirBackupPath,
    Io(io::Error),
    Sender(sender::Error<T>),
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

/// Create a disk aware channel and return tx and rx handles
pub fn channel<T, P>(
    backlog_dir: P,
    channel_size: usize,
    max_file_size: usize,
    max_file_count: usize,
) -> Result<(Sender<T>, mpsc::Receiver<T>), Error<T>>
where
    P: AsRef<Path>,
    T: Into<Vec<u8>>,
    Vec<u8>: Into<T>,
{
    let (tx, rx) = mpsc::channel(channel_size);
    let tx = Sender::new(backlog_dir.as_ref(), max_file_size, max_file_count, tx)?;
    Ok((tx, rx))
}

/// Upgrades an existing tokio sender with disk awareness
pub fn upgrade<T, P>(
    backlog_dir: P,
    tx: mpsc::Sender<T>,
    max_file_size: usize,
    max_file_count: usize,
) -> Result<Sender<T>, Error<T>>
where
    P: AsRef<Path>,
    T: Into<Vec<u8>>,
    Vec<u8>: Into<T>,
{
    let tx = Sender::new(backlog_dir.as_ref(), max_file_size, max_file_count, tx)?;
    Ok(tx)
}

pub use sender::Sender;
