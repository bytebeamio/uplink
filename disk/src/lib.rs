#[macro_use]
extern crate log;

use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

pub struct Storage {
    /// index
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
    current_write_file: BytesMut,
    /// current_read_file
    current_read_file: BytesMut,
    /// current read file index. pointer into current reader id index in backlog_file_ids
    current_read_file_index: Option<usize>,
    /// flag to detect slow receiver
    slow_receiver: bool,
}

impl Storage {
    pub fn new(backlog_dir: &Path, max_file_size: usize, max_file_count: usize) -> io::Result<Storage> {
        let backlog_file_ids = get_file_ids(backlog_dir)?;
        let (index_file, current_read_file_index) = parse_index_file(backlog_dir)?;

        Ok(Storage {
            index_file,
            backlog_file_ids,
            backup_path: PathBuf::from(backlog_dir),
            max_file_size,
            max_file_count,
            current_write_file: BytesMut::with_capacity(max_file_size),
            current_read_file: BytesMut::with_capacity(max_file_size),
            current_read_file_index,
            slow_receiver: false,
        })
    }

    fn decrement_reader_index(&mut self) -> io::Result<()> {
        let index = if let Some(index) = self.current_read_file_index { index as isize - 1 } else { return Ok(()) };

        if index < 0 {
            self.current_read_file_index = None;
            self.index_file.seek(SeekFrom::Start(0))?;
            self.index_file.write_all(format!("").as_bytes())?;
        } else {
            self.current_read_file_index = Some(index as usize);
            self.index_file.seek(SeekFrom::Start(0))?;
            self.index_file.write_all(format!("{}", index).as_bytes())?;
        }

        Ok(())
    }

    fn increment_reader_index(&mut self) -> io::Result<()> {
        let index = if let Some(index) = self.current_read_file_index { index + 1 } else { 0 };

        self.current_read_file_index = Some(index);
        // update the index of file which we are done reading
        // (so that the increment of happens correctly during first read after boot)
        if index > 0 {
            self.index_file.seek(SeekFrom::Start(0))?;
            self.index_file.write_all(format!("{}", index - 1).as_bytes())?;
        } else {
            self.index_file.seek(SeekFrom::Start(0))?;
            self.index_file.write_all(format!("").as_bytes())?;
        }

        Ok(())
    }

    /// Opens next file to write to by incrementing current_write_file_id
    /// Handles retention
    fn open_next_write_file(&mut self) -> io::Result<File> {
        let next_file_id = match self.backlog_file_ids.last() {
            Some(id) => id + 1,
            None => 0,
        };

        let next_file_path = self.backup_path.join(&format!("backup@{}", next_file_id));
        let next_file = OpenOptions::new().write(true).create(true).open(&next_file_path)?;
        self.backlog_file_ids.push(next_file_id);

        let backlog_files_count = self.backlog_file_ids.len();
        // TODO testcases for max no. of files = 0 and 1
        if backlog_files_count > self.max_file_count {
            // count here will always be > 0 due to above if statement. safe. doesn't panic
            let id = self.backlog_file_ids.remove(0);
            let file = self.backup_path.join(&format!("backup@{}", id));

            warn!("file limit reached. deleting {:?}", file);
            fs::remove_file(file)?;
            self.decrement_reader_index()?;
        }

        Ok(next_file)
    }

    /// Checks current write buffer size and flushes it to disk when the size
    /// exceeds configured size
    pub fn flush_on_overflow(&mut self) -> io::Result<()> {
        if self.current_write_file.len() >= self.max_file_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Flushes what ever is in current write buffer into a new file on the disk
    #[inline]
    pub fn flush(&mut self) -> io::Result<()> {
        let mut next_file = self.open_next_write_file()?;
        next_file.write_all(&self.current_write_file[..])?;
        Ok(())
    }
}

/// Gets list of file ids in the disk. Id of file backup@10 is 10.
/// Storing ids instead of full paths enables efficient indexing
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
