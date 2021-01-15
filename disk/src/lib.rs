#[macro_use]
extern crate log;

use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub struct Storage {
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
    /// flag to detect slow receiver
    slow_receiver: bool,
}

impl Storage {
    pub fn new(backlog_dir: &Path, max_file_size: usize, max_file_count: usize) -> io::Result<Storage> {
        let backlog_file_ids = get_file_ids(backlog_dir)?;

        Ok(Storage {
            backlog_file_ids,
            backup_path: PathBuf::from(backlog_dir),
            max_file_size,
            max_file_count,
            current_write_file: BytesMut::with_capacity(max_file_size),
            current_read_file: BytesMut::with_capacity(max_file_size),
            slow_receiver: false,
        })
    }

    pub fn writer(&mut self) -> &mut BytesMut {
        &mut self.current_write_file
    }

    pub fn reader(&mut self) -> &mut BytesMut {
        &mut self.current_read_file
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
        if backlog_files_count > self.max_file_count {
            // count here will always be > 0 due to above if statement. safe. doesn't panic
            let id = self.backlog_file_ids.remove(0);
            let file = self.backup_path.join(&format!("backup@{}", id));

            warn!("file limit reached. deleting {:?}", file);
            fs::remove_file(file)?;
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
        self.current_write_file.clear();
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

#[cfg(test)]
mod test {
    use super::*;
    use mqttbytes::*;
    use std::thread;
    use std::time::Duration;
    use tempdir::TempDir;

    fn init_backup_folders() -> TempDir {
        let backup = TempDir::new("/tmp/persist").unwrap();

        if !backup.path().is_dir() {
            panic!("Folder does not exist");
        }

        backup
    }

    #[test]
    fn flush_creates_new_file_after_size_limit() {
        // 1036 is the size of a publish message with topic = "hello", qos = 1, payload = 1024 bytes
        let backup = init_backup_folders();
        let mut storage = Storage::new(backup.path(), 10 * 1036, 10).unwrap();

        // 2 files on disk and a partially filled in memory buffer
        for _ in 0..101 {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![1; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }

        // 1 message in in memory writer
        assert_eq!(storage.writer().len(), 1036);

        // other messages on disk
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn old_file_is_deleted_after_limit() {
        let backup = init_backup_folders();
        let mut storage = Storage::new(backup.path(), 10 * 1036, 10).unwrap();

        // 11 files created. 10 on disk
        for _ in 0..110 {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![1; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // 11 files created. 10 on disk
        for _ in 0..10 {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![1; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }

        assert_eq!(storage.writer().len(), 0);
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }
}
