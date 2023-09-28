use bytes::{Buf, BufMut, BytesMut};
use log::{self, debug, error, info, warn};
use seahash::hash;

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error: {0}")]
    Io(#[from] io::Error),
    #[error("Not a backup file")]
    NotBackup,
    #[error("Corrupted backup file")]
    CorruptedFile,
}

pub struct Storage {
    name: String,
    /// maximum allowed file size
    max_file_size: usize,
    /// current open file
    current_write_file: BytesMut,
    /// current_read_file
    current_read_file: BytesMut,
    /// disk persistence
    persistence: Option<Persistence>,
}

impl Storage {
    pub fn new(name: impl Into<String>, max_file_size: usize) -> Storage {
        Storage {
            name: name.into(),
            max_file_size,
            current_write_file: BytesMut::with_capacity(max_file_size * 2),
            current_read_file: BytesMut::with_capacity(max_file_size * 2),
            persistence: None,
        }
    }

    pub fn set_persistence<P: Into<PathBuf>>(
        &mut self,
        backup_path: P,
        max_file_count: usize,
    ) -> Result<(), Error> {
        let persistence = Persistence::new(backup_path, max_file_count)?;
        self.persistence = Some(persistence);
        Ok(())
    }

    pub fn set_non_destructive_read(&mut self, switch: bool) {
        self.persistence.as_mut().unwrap().non_destructive_read = switch;
    }

    pub fn writer(&mut self) -> &mut BytesMut {
        &mut self.current_write_file
    }

    pub fn reader(&mut self) -> &mut BytesMut {
        &mut self.current_read_file
    }

    pub fn file_count(&self) -> usize {
        match &self.persistence {
            Some(p) => p.backlog_files.len(),
            None => 0,
        }
    }

    pub fn inmemory_read_size(&self) -> usize {
        self.current_read_file.len()
    }

    pub fn inmemory_write_size(&self) -> usize {
        self.current_write_file.len()
    }

    /// Checks current write buffer size and flushes it to disk when the size
    /// exceeds configured size
    pub fn flush_on_overflow(&mut self) -> Result<Option<u64>, Error> {
        if self.current_write_file.len() < self.max_file_size {
            return Ok(None);
        }

        match &mut self.persistence {
            Some(persistence) => {
                let hash = hash(&self.current_write_file[..]);
                let mut next_file = persistence.open_next_write_file()?;
                info!(
                    "Flushing data to disk for stoarge: {}; path = {:?}",
                    self.name, next_file.path
                );

                next_file.file.write_all(&hash.to_be_bytes())?;
                next_file.file.write_all(&self.current_write_file[..])?;
                next_file.file.flush()?;
                self.current_write_file.clear();
                Ok(next_file.deleted)
            }
            None => {
                // TODO(RT): Make sure that disk files starts with id 1 to represent in memory file
                // with id 0
                self.current_write_file.clear();
                warn!(
                    "Persistence disabled for storage: {}. Deleted in-memory buffer on overflow",
                    self.name
                );
                Ok(Some(0))
            }
        }
    }

    /// Loads head file to current inmemory read buffer. Deletes
    /// the file after loading. If all the disk data is caught up,
    /// swaps current write buffer to current read buffer if there
    /// is pending data in memory write buffer.
    /// Returns true if all the messages are caught up
    pub fn reload_on_eof(&mut self) -> Result<bool, Error> {
        // Don't reload if there is data in current read file
        if self.current_read_file.has_remaining() {
            return Ok(false);
        }

        if let Some(persistence) = &mut self.persistence {
            // Remove read file on completion in destructive-read mode
            let read_is_destructive = !persistence.non_destructive_read;
            let read_file_id = persistence.current_read_file_id.take();
            if let Some(id) = read_is_destructive.then_some(read_file_id).flatten() {
                let deleted_file = persistence.remove(id)?;
                debug!("Completed reading a persistence file, deleting it; storage = {}, path = {deleted_file:?}", self.name);
            }

            // Swap read buffer with write buffer to read data in inmemory write
            // buffer when all the backlog disk files are done
            if persistence.backlog_files.is_empty() {
                mem::swap(&mut self.current_read_file, &mut self.current_write_file);
                // If read buffer is 0 after swapping, all the data is caught up
                return Ok(self.current_read_file.is_empty());
            }

            if let Err(e) = persistence.load_next_read_file(&mut self.current_read_file) {
                self.current_read_file.clear();
                persistence.current_read_file_id.take();
                return Err(e);
            }

            Ok(false)
        } else {
            mem::swap(&mut self.current_read_file, &mut self.current_write_file);
            // If read buffer is 0 after swapping, all the data is caught up
            Ok(self.current_read_file.is_empty())
        }
    }
}

/// Converts file path to file id
fn id(path: &Path) -> Result<u64, Error> {
    if let Some(file_name) = path.file_name() {
        let file_name = format!("{file_name:?}");
        if !file_name.contains("backup@") {
            return Err(Error::NotBackup);
        }
    }

    let id: Vec<&str> = path.file_stem().unwrap().to_str().unwrap().split('@').collect();
    let id: u64 = id[1].parse().unwrap();
    Ok(id)
}

/// Gets list of file ids in the disk. Id of file backup@10 is 10.
/// Storing ids instead of full paths enables efficient indexing
fn get_file_ids(path: &Path) -> Result<VecDeque<u64>, Error> {
    let mut file_ids = Vec::new();
    let files = fs::read_dir(path)?;
    for file in files {
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
    let file_ids = VecDeque::from(file_ids);

    Ok(file_ids)
}

struct NextFile {
    path: PathBuf,
    file: File,
    deleted: Option<u64>,
}

struct Persistence {
    /// Backup path
    path: PathBuf,
    /// maximum number of files before deleting old file
    max_file_count: usize,
    /// list of backlog file ids. Mutated only be the serialization part of the sender
    backlog_files: VecDeque<u64>,
    /// id of file being read, delete it on read completion
    current_read_file_id: Option<u64>,
    // /// Deleted file id
    // deleted: Option<u64>,
    non_destructive_read: bool,
}

impl Persistence {
    fn new<P: Into<PathBuf>>(path: P, max_file_count: usize) -> Result<Self, Error> {
        let path = path.into();
        let backlog_files = get_file_ids(&path)?;
        info!("List of file ids loaded from disk: {backlog_files:?}");

        Ok(Persistence {
            path,
            max_file_count,
            backlog_files,
            current_read_file_id: None,
            // deleted: None,
            non_destructive_read: false,
        })
    }

    fn path(&self, id: u64) -> Result<PathBuf, Error> {
        let file_name = format!("backup@{id}");
        let path = self.path.join(file_name);

        Ok(path)
    }

    /// Removes a file with provided id
    fn remove(&self, id: u64) -> Result<PathBuf, Error> {
        let path = self.path(id)?;
        fs::remove_file(&path)?;

        Ok(path)
    }

    /// Move corrupt file to special directory
    fn handle_corrupt_file(&self) -> Result<(), Error> {
        let id = self.current_read_file_id.expect("There is supposed to be a file here");
        let path_src = self.path(id)?;
        let dest_dir = self.path.join("corrupted");
        fs::create_dir_all(&dest_dir)?;

        let file_name = path_src.file_name().expect("The file name should exist");
        let path_dest = dest_dir.join(file_name);

        warn!("Moving corrupted file from {path_src:?} to {path_dest:?}");
        fs::rename(path_src, path_dest)?;

        Ok(())
    }

    /// Opens file to flush current inmemory write buffer to disk.
    /// Also handles retention of previous files on disk
    fn open_next_write_file(&mut self) -> Result<NextFile, Error> {
        let next_file_id = self.backlog_files.back().map_or(0, |id| id + 1);
        let file_name = format!("backup@{next_file_id}");
        let next_file_path = self.path.join(file_name);
        let next_file = OpenOptions::new().write(true).create(true).open(&next_file_path)?;

        self.backlog_files.push_back(next_file_id);

        let mut next = NextFile { path: next_file_path, file: next_file, deleted: None };
        let mut backlog_files_count = self.backlog_files.len();

        // File being read is also to be considered
        if self.current_read_file_id.is_some() {
            backlog_files_count += 1
        }

        // Return next file details if backlog is within limits
        if backlog_files_count <= self.max_file_count {
            return Ok(next);
        }

        // Remove file being read, or first in backlog
        // NOTE: keeps read buffer unchanged
        let id = match self.current_read_file_id.take() {
            Some(id) => id,
            _ => self.backlog_files.pop_front().unwrap(),
        };

        next.deleted = Some(id);
        if !self.non_destructive_read {
            let deleted_file = self.remove(id)?;
            warn!("file limit reached. deleting backup@{}; path = {deleted_file:?}", id);
        }

        Ok(next)
    }

    fn load_next_read_file(&mut self, current_read_file: &mut BytesMut) -> Result<(), Error> {
        // Len always > 0 because of above if. Doesn't panic
        let id = self.backlog_files.pop_front().unwrap();
        let next_file_path = self.path(id)?;

        let mut file = OpenOptions::new().read(true).open(&next_file_path)?;

        // Load file into memory and store its id for deleting in the future
        let metadata = fs::metadata(&next_file_path)?;

        // Initialize next read file with 0s
        current_read_file.clear();
        let init = vec![0u8; metadata.len() as usize];
        current_read_file.put_slice(&init);

        file.read_exact(&mut current_read_file[..])?;
        self.current_read_file_id = Some(id);

        // Verify with checksum
        if current_read_file.len() < 8 {
            self.handle_corrupt_file()?;
            return Err(Error::CorruptedFile);
        }

        let expected_hash = current_read_file.get_u64();
        let actual_hash = hash(&current_read_file[..]);
        if actual_hash != expected_hash {
            self.handle_corrupt_file()?;
            return Err(Error::CorruptedFile);
        }

        Ok(())
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

    fn write_n_publishes(storage: &mut Storage, n: u8) {
        for i in 0..n {
            let mut publish = Publish::new("hello", QoS::AtLeastOnce, vec![i; 1024]);
            publish.pkid = 1;
            publish.write(storage.writer()).unwrap();
            storage.flush_on_overflow().unwrap();
        }
    }

    fn read_n_publishes(storage: &mut Storage, n: usize) -> Vec<Publish> {
        let mut publishes = vec![];
        for _ in 0..n {
            // Done reading all the pending files
            if storage.reload_on_eof().unwrap() {
                break;
            }

            match read(storage.reader(), 1048).unwrap() {
                Packet::Publish(p) => publishes.push(p),
                packet => unreachable!("{:?}", packet),
            }
        }

        publishes
    }

    #[test]
    fn flush_creates_new_file_after_size_limit() {
        // 1036 is the size of a publish message with topic = "hello", qos = 1, payload = 1024 bytes
        let backup = init_backup_folders();
        let mut storage = Storage::new("test", 10 * 1036);
        storage.set_persistence(backup.path(), 10).unwrap();

        // 2 files on disk and a partially filled in memory buffer
        write_n_publishes(&mut storage, 101);

        // 1 message in in memory writer
        assert_eq!(storage.writer().len(), 1036);

        // other messages on disk
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn old_file_is_deleted_after_limit() {
        let backup = init_backup_folders();
        let mut storage = Storage::new("test", 10 * 1036);
        storage.set_persistence(backup.path(), 10).unwrap();

        // 11 files created. 10 on disk
        write_n_publishes(&mut storage, 110);

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // 11 files created. 10 on disk
        write_n_publishes(&mut storage, 10);

        assert_eq!(storage.writer().len(), 0);
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn reload_loads_correct_file_into_memory() {
        let backup = init_backup_folders();
        let mut storage = Storage::new("test", 10 * 1036);
        storage.set_persistence(backup.path(), 10).unwrap();

        // 10 files on disk
        write_n_publishes(&mut storage, 100);

        // breaks after 100th iteration due to `reload_on_eof` break
        let publishes = read_n_publishes(&mut storage, 1234);

        assert_eq!(publishes.len(), 100);
        for (i, publish) in publishes.iter().enumerate() {
            assert_eq!(&publish.payload[..], vec![i as u8; 1024].as_slice());
        }
    }

    #[test]
    fn reload_loads_partially_written_write_buffer_correctly() {
        let backup = init_backup_folders();
        let mut storage = Storage::new("test", 10 * 1036);
        storage.set_persistence(backup.path(), 10).unwrap();

        // 10 files on disk and partially filled current write buffer
        write_n_publishes(&mut storage, 105);

        // breaks after 100th iteration due to `reload_on_eof` break
        let publishes = read_n_publishes(&mut storage, 12345);

        assert_eq!(storage.current_write_file.len(), 0);
        assert_eq!(publishes.len(), 105);
        for (i, publish) in publishes.iter().enumerate() {
            assert_eq!(&publish.payload[..], vec![i as u8; 1024].as_slice());
        }
    }

    #[test]
    fn ensure_file_remove_on_read_completion_only() {
        let backup = init_backup_folders();
        let mut storage = Storage::new("test", 10 * 1036);
        storage.set_persistence(backup.path(), 10).unwrap();
        // 10 files on disk and partially filled current write buffer, 10 publishes per file
        write_n_publishes(&mut storage, 105);

        // Initially not on a read file
        assert_eq!(storage.persistence.as_ref().unwrap().current_read_file_id, None);

        // Ensure unread files are all present before read
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // Successfully read 10 files with files still in storage after 10 reads
        for i in 0..10 {
            read_n_publishes(&mut storage, 10);
            let file_id = storage.persistence.as_ref().unwrap().current_read_file_id.unwrap();
            assert_eq!(file_id, i);
            // Ensure partially read file is still present in backup dir
            let files = get_file_ids(&backup.path()).unwrap();
            assert!(files.contains(&i));
        }

        // All read files should be deleted just after 1 more read
        read_n_publishes(&mut storage, 1);
        assert_eq!(storage.persistence.as_ref().unwrap().current_read_file_id, None);

        // Ensure read files are all present before read
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![]);
    }

    #[test]
    fn ensure_files_including_read_removed_post_flush_on_overflow() {
        let backup = init_backup_folders();
        let mut storage = Storage::new("test", 10 * 1036);
        storage.set_persistence(backup.path(), 10).unwrap();
        // 10 files on disk and partially filled current write buffer, 10 publishes per file
        write_n_publishes(&mut storage, 105);

        // Initially not on a read file
        assert_eq!(storage.persistence.as_ref().unwrap().current_read_file_id, None);

        // Successfully read a single file
        read_n_publishes(&mut storage, 10);
        let file_id = storage.persistence.as_ref().unwrap().current_read_file_id.unwrap();
        assert_eq!(file_id, 0);

        // Ensure all persistance files still exist
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // Write 10 more files onto disk, 10 publishes per file
        write_n_publishes(&mut storage, 100);

        // Ensure none of the earlier files exist on disk
        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(files, vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
    }
}
