use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, info, warn};
use seahash::hash;

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::{self, copy, Write};
use std::mem;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid backup file format")]
    InvalidBackupFile,
    #[error("Corrupted backup file")]
    CorruptedBackupFile,
    #[error("Write buffer is empty")]
    EmptyWriteBuffer,
    #[error("No more backups available")]
    NoMoreBackups,
}

pub struct Storage {
    name: String,
    max_file_size: usize,
    write_buffer: BytesMut,
    read_buffer: BytesMut,
    persistence: Option<Persistence>,
}

impl Storage {
    pub fn new(name: impl Into<String>, max_file_size: usize) -> Self {
        Self {
            name: name.into(),
            max_file_size,
            write_buffer: BytesMut::with_capacity(max_file_size * 2),
            read_buffer: BytesMut::with_capacity(max_file_size * 2),
            persistence: None,
        }
    }

    pub fn enable_persistence<P: Into<PathBuf>>(
        &mut self,
        backup_path: P,
        max_files: usize,
    ) -> Result<(), Error> {
        let persistence = Persistence::new(backup_path, max_files)?;
        self.persistence = Some(persistence);
        Ok(())
    }

    pub fn set_non_destructive_read(&mut self, non_destructive: bool) {
        if let Some(persistence) = &mut self.persistence {
            persistence.non_destructive_read = non_destructive;
        }
    }

    pub fn writer(&mut self) -> &mut BytesMut {
        &mut self.write_buffer
    }

    pub fn reader(&mut self) -> &mut BytesMut {
        &mut self.read_buffer
    }

    pub fn file_count(&self) -> usize {
        self.persistence.as_ref().map_or(0, |p| p.backlog_files.len())
    }

    pub fn disk_usage(&self) -> usize {
        self.persistence.as_ref().map_or(0, |p| p.bytes_occupied)
    }

    pub fn inmemory_read_size(&self) -> usize {
        self.read_buffer.len()
    }

    pub fn inmemory_write_size(&self) -> usize {
        self.write_buffer.len()
    }

    /// Flushes write buffer to disk if it exceeds `max_file_size`
    pub fn flush_on_overflow(&mut self) -> Result<Option<u64>, Error> {
        if self.write_buffer.len() < self.max_file_size {
            return Ok(None);
        }
        self.flush()
    }

    /// Flushes write buffer to disk forcefully
    pub fn flush(&mut self) -> Result<Option<u64>, Error> {
        if self.write_buffer.is_empty() {
            return Err(Error::EmptyWriteBuffer);
        }

        let Some(persistence) = &mut self.persistence else {
            warn!("Persistence disabled for storage: {}. Clearing in-memory buffer", self.name);
            self.write_buffer.clear();
            return Ok(Some(0));
        };

        let NextFile { file, deleted_file } = persistence.prepare_next_write_file()?;
        info!("Flushing data to disk for storage: {}; path = {:?}", self.name, file.path());
        file.write(&mut self.write_buffer)?;

        persistence.bytes_occupied += 8 + self.write_buffer.len(); // 8 bytes for hash
        self.write_buffer.clear();
        Ok(deleted_file)
    }

    /// Loads the next file into the read buffer, or swaps write buffer if disk files are finished
    pub fn reload_on_eof(&mut self) -> Result<(), Error> {
        if self.read_buffer.has_remaining() {
            return Ok(());
        }

        let Some(persistence) = &mut self.persistence else {
            mem::swap(&mut self.read_buffer, &mut self.write_buffer);
            if self.read_buffer.is_empty() {
                return Err(Error::NoMoreBackups);
            }
            return Ok(());
        };

        if let Some(id) = persistence.current_read_file_id.take() {
            if !persistence.non_destructive_read {
                let deleted_path = persistence.remove_file(id)?;
                debug!("Deleted read file after completion: {:?}", deleted_path);
            }
        }

        if persistence.backlog_files.is_empty() {
            mem::swap(&mut self.read_buffer, &mut self.write_buffer);
            if self.read_buffer.is_empty() {
                return Err(Error::NoMoreBackups);
            }
            return Ok(());
        }

        persistence.load_next_read_file(&mut self.read_buffer)?;
        Ok(())
    }
}

/// Gets a sorted list of file IDs from the backup directory
fn collect_file_ids(path: &Path) -> Result<VecDeque<u64>, Error> {
    let mut file_ids: Vec<u64> = fs::read_dir(path)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let file_name = entry.file_name().into_string().ok()?;
            // Check if the file name starts with "backup@" and parse the ID
            file_name.strip_prefix("backup@").and_then(|id_str| id_str.parse::<u64>().ok())
        })
        .collect();

    // Sort IDs in ascending order and return as VecDeque
    file_ids.sort_unstable();
    Ok(VecDeque::from(file_ids))
}

pub struct PersistenceFile<'a> {
    dir: &'a Path,
    file_name: String,
}

impl<'a> PersistenceFile<'a> {
    pub fn new(dir: &'a Path, file_name: String) -> Result<Self, Error> {
        Ok(Self { dir, file_name })
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join(&self.file_name)
    }

    fn move_to_corrupted(&self) -> Result<(), Error> {
        let dest_dir = self.dir.join("corrupted");
        fs::create_dir_all(&dest_dir)?;
        fs::rename(self.path(), dest_dir.join(&self.file_name))?;
        warn!("Moved corrupted file to {:?}", dest_dir);
        Ok(())
    }

    pub fn read(&self, buf: &mut BytesMut) -> Result<(), Error> {
        let mut file = OpenOptions::new().read(true).open(self.path())?;
        buf.clear();
        copy(&mut file, &mut buf.writer())?;

        if buf.len() < 8 {
            self.move_to_corrupted()?;
            return Err(Error::CorruptedBackupFile);
        }

        let expected_hash = buf.get_u64();
        let actual_hash = hash(&buf[..]);
        if actual_hash != expected_hash {
            self.move_to_corrupted()?;
            return Err(Error::CorruptedBackupFile);
        }

        Ok(())
    }

    pub fn write(&self, buf: &mut BytesMut) -> Result<(), Error> {
        let mut file =
            OpenOptions::new().write(true).create(true).truncate(true).open(self.path())?;
        file.write_all(&hash(&buf[..]).to_be_bytes())?;
        file.write_all(&buf[..])?;
        file.flush()?;
        Ok(())
    }

    pub fn delete(&self) -> Result<u64, Error> {
        let path = self.path();
        let size = fs::metadata(&path)?.len();
        fs::remove_file(path)?;
        Ok(size)
    }
}

struct NextFile<'a> {
    file: PersistenceFile<'a>,
    deleted_file: Option<u64>,
}

struct Persistence {
    path: PathBuf,
    max_files: usize,
    backlog_files: VecDeque<u64>,
    current_read_file_id: Option<u64>,
    non_destructive_read: bool,
    bytes_occupied: usize,
}

impl Persistence {
    fn new<P: Into<PathBuf>>(path: P, max_files: usize) -> Result<Self, Error> {
        let path = path.into();
        let backlog_files = collect_file_ids(&path)?;
        let bytes_occupied = backlog_files
            .iter()
            .map(|id| {
                let file = path.join(format!("backup@{id}"));
                fs::metadata(&file).map(|meta| meta.len() as usize).unwrap_or(0)
            })
            .sum();

        Ok(Self {
            path,
            max_files,
            backlog_files,
            current_read_file_id: None,
            non_destructive_read: false,
            bytes_occupied,
        })
    }

    fn remove_file(&mut self, id: u64) -> Result<PathBuf, Error> {
        let file = PersistenceFile::new(&self.path, format!("backup@{id}"))?;
        self.bytes_occupied -= file.delete()? as usize;
        Ok(file.path())
    }

    fn prepare_next_write_file(&mut self) -> Result<NextFile, Error> {
        let new_file_id = self.backlog_files.back().map_or(0, |&id| id + 1);
        self.backlog_files.push_back(new_file_id);

        let deleted_file = if self.backlog_files.len() + self.current_read_file_id.map_or(0, |_| 1)
            > self.max_files
        {
            let id_to_delete = self
                .current_read_file_id
                .take()
                .or_else(|| self.backlog_files.pop_front())
                .unwrap();
            if !self.non_destructive_read {
                let deleted_file = self.remove_file(id_to_delete)?;
                warn!("File limit reached. Deleted {:?}", deleted_file);
            }
            Some(id_to_delete)
        } else {
            None
        };

        let new_file = PersistenceFile::new(&self.path, format!("backup@{new_file_id}"))?;
        Ok(NextFile { file: new_file, deleted_file })
    }

    fn load_next_read_file(&mut self, buffer: &mut BytesMut) -> Result<(), Error> {
        if let Some(id) = self.backlog_files.pop_front() {
            let file = PersistenceFile::new(&self.path, format!("backup@{id}"))?;
            file.read(buffer)?;
            self.current_read_file_id = Some(id);
            Ok(())
        } else {
            self.current_read_file_id.take();
            Err(Error::NoMoreBackups)
        }
    }
}

#[cfg(test)]
mod test;
