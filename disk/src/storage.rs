use bytes::{Buf, BufMut, BytesMut};
use log::{info, warn};

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::{mem, path::PathBuf};

use super::{get_file_ids, NextFile};

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
}

impl Storage {
    pub fn new<P: Into<PathBuf>>(
        backlog_dir: P,
        max_file_size: usize,
        max_file_count: usize,
    ) -> io::Result<Storage> {
        let backup_path = backlog_dir.into();

        Ok(Storage {
            backlog_file_ids: get_file_ids(&backup_path)?,
            backup_path,
            max_file_size,
            max_file_count,
            current_write_file: BytesMut::with_capacity(max_file_size * 2),
            current_read_file: BytesMut::with_capacity(max_file_size * 2),
        })
    }

    pub fn writer(&mut self) -> &mut BytesMut {
        &mut self.current_write_file
    }

    pub fn reader(&mut self) -> &mut BytesMut {
        &mut self.current_read_file
    }

    /// Removes a file with provided id
    fn remove(&self, id: u64) -> io::Result<()> {
        let path = self.backup_path.join(&format!("backup@{}", id));
        fs::remove_file(path)?;
        Ok(())
    }

    /// Initializes read buffer before reading data from the file
    fn prepare_current_read_buffer(&mut self, file_len: usize) {
        self.current_read_file.clear();
        let init = vec![0u8; file_len];
        self.current_read_file.put_slice(&init[..]);
    }

    /// Opens file to flush current inmemory write buffer to disk.
    /// Also handles retention of previous files on disk
    fn open_next_write_file(&mut self) -> io::Result<NextFile> {
        let next_file_id = self.backlog_file_ids.last().map_or(0, |id| id + 1);
        let next_file_path = self.backup_path.join(&format!("backup@{}", next_file_id));
        let next_file = OpenOptions::new().write(true).create(true).open(&next_file_path)?;
        self.backlog_file_ids.push(next_file_id);

        let mut next = NextFile { path: next_file_path, file: next_file, deleted: None };

        let backlog_files_count = self.backlog_file_ids.len();
        if backlog_files_count > self.max_file_count {
            // Backlog should always be > 0 given the earliest push. doesn't panic
            let id = self.backlog_file_ids.remove(0);
            warn!("file limit reached. deleting backup@{}", id);
            next.deleted = Some(id);
            self.remove(id)?;
        }

        Ok(next)
    }

    /// Flushes what ever is in current write buffer into a new file on the disk
    #[inline]
    fn flush(&mut self) -> io::Result<Option<u64>> {
        let mut next_file = self.open_next_write_file()?;
        info!("Flushing data to disk!! {:?}", next_file.path);
        next_file.file.write_all(&self.current_write_file[..])?;
        next_file.file.flush()?;
        self.current_write_file.clear();
        Ok(next_file.deleted)
    }

    /// Checks current write buffer size and flushes it to disk when the size
    /// exceeds configured size
    pub fn flush_on_overflow(&mut self) -> io::Result<Option<u64>> {
        if self.current_write_file.len() >= self.max_file_size {
            return self.flush();
        }

        Ok(None)
    }

    /// Reloads next buffer even if there is pending data in current buffer
    pub fn reload(&mut self) -> io::Result<bool> {
        // Swap read buffer with write buffer to read data in inmemory write
        // buffer when all the backlog disk files are done
        if self.backlog_file_ids.is_empty() {
            mem::swap(&mut self.current_read_file, &mut self.current_write_file);

            // If read buffer is 0 after swapping, all the data is caught up
            return if self.current_read_file.is_empty() { Ok(true) } else { Ok(false) };
        }

        // Len always > 0 because of above if. Doesn't panic
        let id = self.backlog_file_ids.remove(0);
        let next_file_path = self.backup_path.join("backup@".to_owned() + &id.to_string());
        let mut file = OpenOptions::new().read(true).open(&next_file_path)?;

        // Load file into memory and delete it
        let metadata = fs::metadata(&next_file_path).expect("unable to read metadata");
        self.prepare_current_read_buffer(metadata.len() as usize);
        file.read_exact(&mut self.current_read_file[..])?;
        self.remove(id)?;

        Ok(false)
    }

    /// Loads head file to current inmemory read buffer. Deletes
    /// the file after loading. If all the disk data is caught up,
    /// swaps current write buffer to current read buffer if there
    /// is pending data in memory write buffer.
    /// Returns true if all the messages are caught up
    pub fn reload_on_eof(&mut self) -> io::Result<bool> {
        // Don't reload if there is data in current read file
        if self.current_read_file.has_remaining() {
            return Ok(false);
        }

        self.reload()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use mqttbytes::*;
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
        let files = get_file_ids(backup.path()).unwrap();
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

        let files = get_file_ids(backup.path()).unwrap();
        assert_eq!(files, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // 11 files created. 10 on disk
        for _ in 0..10 {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![1; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }

        assert_eq!(storage.writer().len(), 0);
        let files = get_file_ids(backup.path()).unwrap();
        assert_eq!(files, vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn reload_loads_correct_file_into_memory() {
        let backup = init_backup_folders();
        let mut storage = Storage::new(backup.path(), 10 * 1036, 10).unwrap();

        // 10 files on disk
        for i in 0..100 {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![i; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }

        let mut publishes = Vec::new();

        // breaks after 100th iteration due to `reload_on_eof` break
        for _ in 0..1234 {
            // Done reading all the pending files
            if storage.reload_on_eof().unwrap() {
                break;
            }

            match read(storage.reader(), 1048).unwrap() {
                Packet::Publish(publish) => publishes.push(publish),
                packet => unreachable!("{:?}", packet),
            }
        }

        assert_eq!(publishes.len(), 100);
        for (i, publish) in publishes.iter().enumerate() {
            assert_eq!(&publish.payload[..], vec![i as u8; 1024].as_slice());
        }
    }

    #[test]
    fn reload_loads_partially_written_write_buffer_correctly() {
        let backup = init_backup_folders();
        let mut storage = Storage::new(backup.path(), 10 * 1036, 10).unwrap();

        // 10 files on disk and partially filled current write buffer
        for i in 0..105 {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![i; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }

        let mut publishes = Vec::new();

        // breaks after 100th iteration due to `reload_on_eof` break
        for _i in 0..12345 {
            // Done reading all the pending files
            if storage.reload_on_eof().unwrap() {
                break;
            }

            match read(storage.reader(), 1048).unwrap() {
                Packet::Publish(publish) => publishes.push(publish),
                packet => unreachable!("{:?}", packet),
            }
        }

        assert_eq!(storage.current_write_file.len(), 0);
        assert_eq!(publishes.len(), 105);
        for (i, publish) in publishes.iter().enumerate() {
            assert_eq!(&publish.payload[..], vec![i as u8; 1024].as_slice());
        }
    }
}
