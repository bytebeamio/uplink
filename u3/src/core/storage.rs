use crate::config::PersistenceConfig;
use anyhow::Context;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::error;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Default, Debug)]
pub struct StorageMetrics {
    pub read_buffer_size: u64,
    pub write_buffer_size: u64,
    pub bytes_on_disk: u64,
    pub files_count: u32,
    pub lost_files: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DiskQueue {
    /// only touch this path on disk if persistence.max_file_count == 0
    dir: PathBuf,
    files_queue: VecDeque<i32>,
    write_buffer: BytesMut,
    read_buffer: BytesMut,
    persistence: PersistenceConfig,
    bytes_on_disk: u64,
    lost_files: u32,
}

fn load_file_ids(
    dir: &Path,
    persistence: &PersistenceConfig,
) -> anyhow::Result<(VecDeque<i32>, u64)> {
    let mut file_ids = Vec::new();
    let files = std::fs::read_dir(dir)?;
    for file in files {
        let (path, size) = if let Ok(file) = file {
            if let Ok(metadata) = file.metadata() {
                (file.path(), metadata.len())
            } else {
                continue;
            }
        } else {
            continue;
        };
        if !path.is_file() {
            continue;
        }

        let Some(id) = id(&path) else { continue };
        file_ids.push((path, id, size));
    }

    file_ids.sort_by_key(|(_, id, _)| *id);
    if file_ids.len() > persistence.max_file_count {
        let to_drop = file_ids.len() - persistence.max_file_count;
        for (path, _, _) in file_ids.iter().take(to_drop) {
            if let Err(e) = std::fs::remove_file(path) {
                log::error!("couldn't delete excess persistence files from {dir:?}: {e}");
            }
        }
        file_ids.drain(0..to_drop);
    }
    let files_queue = file_ids.iter().map(|(_, id, _)| id).cloned().collect();
    let mut bytes_on_disk = 0;
    for (_, _, size) in file_ids.iter() {
        bytes_on_disk += size;
    }

    Ok((files_queue, bytes_on_disk))
}

impl DiskQueue {
    pub fn new(dir: PathBuf, persistence: PersistenceConfig) -> Self {
        let mut result = Self {
            dir,
            files_queue: Default::default(),
            read_buffer: BytesMut::with_capacity(persistence.max_file_size * 3 / 2),
            write_buffer: BytesMut::with_capacity(persistence.max_file_size * 3 / 2),
            persistence,
            bytes_on_disk: 0,
            lost_files: 0,
        };
        if result.persistence.max_file_count > 0 {
            if let Err(e) = std::fs::create_dir_all(&result.dir) {
                error!("couldn't create persistence directory({:?}): {e:?}", result.dir);
                result.to_in_memory();
            } else {
                match load_file_ids(&result.dir, &result.persistence) {
                    Ok((files, size)) => {
                        result.files_queue = files;
                        result.bytes_on_disk = size;
                    }
                    Err(e) => {
                        error!("failed to load persistence files from ({:?}): {e:?}", result.dir);
                        result.to_in_memory();
                    }
                }
            }
        }
        result
    }

    pub fn name(&self) -> &str {
        self.dir.to_str().unwrap_or("{}")
    }

    /// if read_buffer_is_empty() {
    ///     if is_in_memory || there_are_no_persistence_files() {
    ///         if write_buffer_is_empty() {
    ///           return Err::Empty;
    ///         } else {
    ///           swap_read_and_write_buffers();
    ///         }
    ///     } else {
    ///         load_first_persistence_file_into_memory(); // on error, switch to in memory error
    ///     }
    /// }
    /// pull_data_from_read_buffer();
    pub fn read_packet(&mut self) -> Result<Publish, StorageReadError> {
        if self.read_buffer.is_empty() {
            let next_file = self.files_queue.pop_front();
            if self.persistence.max_file_count == 0 || next_file.is_none() {
                if self.write_buffer.is_empty() {
                    return Err(StorageReadError::Empty);
                } else {
                    std::mem::swap(&mut self.read_buffer, &mut self.write_buffer);
                }
            } else {
                let pf = PersistenceFile::new(
                    self.dir.as_path(),
                    format!("backup@{}", next_file.unwrap()),
                );
                match pf.load_into(&mut self.read_buffer) {
                    Ok(deleted_size) => {
                        self.bytes_on_disk -= deleted_size;
                    }
                    Err(PersistenceError::CorruptedFile(_path)) => {}
                    Err(PersistenceError::IoError(e)) => {
                        log::error!(
                            "encountered file system error when loading persistence file({:?}): {e:?}",
                            pf.path()
                        );
                        self.to_in_memory();
                        return self.read_packet();
                    }
                }
            }
        }
        match Publish::read(&mut self.read_buffer) {
            Ok(r) => Ok(r),
            Err(e) => {
                self.read_buffer.clear();
                Err(StorageReadError::InvalidPacket(format!("{e:?}")))
            }
        }
    }

    /// if no_space_in_write_buffer() {
    ///     if is_in_memory {
    ///         swap_read_and_write_buffers();
    ///     } else {
    ///         if persistence_file_count_limit_reached() {
    ///             drop_read_buffer();
    ///             load_oldest_persistence_file_into_read_buffer();
    ///         }
    ///         flush_write_buffer_to_disk();
    ///     }
    /// }
    /// append_to_write_buffer();
    pub fn write_packet(&mut self, packet: Publish) {
        if self.write_buffer.len() >= self.persistence.max_file_size {
            if self.persistence.max_file_count == 0 {
                std::mem::swap(&mut self.read_buffer, &mut self.write_buffer);
            } else {
                if self.files_queue.len() >= self.persistence.max_file_count {
                    self.read_buffer.clear();
                    self.lost_files += 1;
                    let id = self.files_queue.pop_front().unwrap();
                    let pf = PersistenceFile::new(self.dir.as_path(), format!("backup@{id}"));
                    match pf.load_into(&mut self.read_buffer) {
                        Ok(deleted_size) => {
                            log::info!(
                                "File count reached, deleted oldest persistence file ({}):({})",
                                self.name(),
                                pf.file_name
                            );
                            self.bytes_on_disk -= deleted_size;
                        }
                        Err(PersistenceError::CorruptedFile(_path)) => {}
                        Err(PersistenceError::IoError(e)) => {
                            log::error!(
                                "encountered file system error when loading persistence file({:?}): {e:?}",
                                pf.path()
                            );
                            self.to_in_memory();
                            return self.write_packet(packet);
                        }
                    }
                }
                let next_id = self.files_queue.iter().last().map(|id| id + 1).unwrap_or(1);
                let pf = PersistenceFile::new(self.dir.as_path(), format!("backup@{next_id}"));
                log::info!("Flushing data to disk ({}):({})", self.name(), pf.file_name);
                match pf.write(&mut self.write_buffer) {
                    Ok(_) => {
                        self.bytes_on_disk += self.write_buffer.len() as u64 + 8;
                        self.write_buffer.clear();
                        self.files_queue.push_back(next_id);
                    }
                    Err(e) => {
                        log::error!(
                            "encountered file system error when flushing persistence file({:?}): {e:?}",
                            pf.path()
                        );
                        self.to_in_memory();
                        return self.write_packet(packet);
                    }
                }
            }
        }

        packet.write(&mut self.write_buffer);
    }

    /// save_read_buffer_to_disk();
    /// save_write_buffer_to_disk();
    pub fn flush(&mut self) {
        if self.persistence.max_file_count == 0 {
            return;
        }
        if !self.read_buffer.is_empty() {
            let read_file_id = self.files_queue.iter().next().cloned().unwrap_or(1) - 1;
            let pf = PersistenceFile::new(self.dir.as_path(), format!("backup@{read_file_id}"));
            if let Err(e) = pf.write(&mut self.read_buffer) {
                error!("failed to flush read buffer to {:?}: {e:?}", pf.path());
            }
        }
        if !self.write_buffer.is_empty() {
            let write_file_id = self.files_queue.iter().last().cloned().unwrap_or(1) + 1;
            let pf = PersistenceFile::new(self.dir.as_path(), format!("backup@{write_file_id}"));
            if let Err(e) = pf.write(&mut self.write_buffer) {
                error!("failed to flush write buffer to {:?}: {e:?}", pf.path());
            }
        }
    }

    fn to_in_memory(&mut self) {
        self.persistence.max_file_count = 0;
        self.bytes_on_disk = 0;
        self.files_queue.drain(..);
    }

    pub fn metrics(&self) -> StorageMetrics {
        StorageMetrics {
            read_buffer_size: self.read_buffer.len() as _,
            write_buffer_size: self.write_buffer.len() as _,
            bytes_on_disk: self.bytes_on_disk,
            files_count: self.files_queue.len() as _,
            lost_files: self.lost_files,
        }
    }
}

// format:
// u8 -> compressed
// u32 -> payload_len
// buffer size and content
#[derive(Clone)]
pub struct Publish {
    pub payload: Vec<u8>,
    pub compressed: bool,
}
impl Publish {
    pub fn write(&self, buf: &mut BytesMut) {
        buf.put_u8(self.compressed as _);
        buf.put_u32(self.payload.len() as _);
        buf.put_slice(&self.payload);
    }

    pub fn read(buf: &mut BytesMut) -> Result<Self, &'static str> {
        let compressed = buf.try_get_u8().map_err(|_| "insufficient bytes")? != 0;
        let payload_len = buf.get_u32() as usize;
        if buf.len() < payload_len {
            return Err("insufficient bytes");
        }
        let payload = buf.split_to(payload_len).to_vec();

        Ok(Self { compressed, payload })
    }
}

#[derive(Debug)]
pub enum StorageReadError {
    /// Nothing left in storage, poll the storage with lower priority
    Empty,
    /// Should never happen because we write valid packets to storage and files on disk have a checksum,
    /// If this is returned that means the buffer with this packet has been cleared, try polling again
    InvalidPacket(String),
}

#[derive(Debug)]
pub enum StorageFlushError {
    FileSystemError(std::io::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn id(path: &Path) -> Option<i32> {
    if let Some(file_name) = path.file_name() {
        let file_name = file_name.to_str()?;
        if !file_name.starts_with("backup@") { None } else { file_name[7..].parse().ok() }
    } else {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A utility handle to a persistence file on disk
/// You can do operations like, read, write, delete, save it to a special location if corrupted
/// TODO: unexpected files and dirs
pub struct PersistenceFile<'a> {
    /// Path to the persistence directory
    dir: &'a Path,
    /// Name of the file e.g. `backup@1`
    file_name: String,
}

impl<'a> PersistenceFile<'a> {
    pub fn new(dir: &'a Path, file_name: String) -> Self {
        Self { dir, file_name }
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join(&self.file_name)
    }

    fn handle_corrupt_file(&self) -> anyhow::Result<()> {
        let src_file = self.path();
        let dest_file = self.dir.join("backup@corrupted");
        if dest_file.exists() {
            std::fs::remove_file(&dest_file)?;
        }

        log::warn!("Moving corrupted file from {src_file:?} to {dest_file:?}");
        std::fs::rename(src_file, &dest_file)?;

        Ok(())
    }

    /// Read contents of the persistence file from disk into buffer in memory
    pub fn read(&self, buf: &mut BytesMut) -> Result<(), PersistenceError> {
        let path = self.path();
        let mut file = OpenOptions::new().read(true).open(path)?;

        // Initialize buffer and load next read file
        buf.clear();
        std::io::copy(&mut file, &mut buf.writer())?;

        // Verify with checksum
        if buf.len() < 8 {
            if let Err(e) = self.handle_corrupt_file() {
                log::error!("Couldn't save corrupted file: {e}");
            }
            return Err(PersistenceError::CorruptedFile(format!("{:?}", self.path())));
        }

        let expected_hash = buf.get_u64();
        let actual_hash = seahash::hash(&buf[..]);
        if actual_hash != expected_hash {
            if let Err(e) = self.handle_corrupt_file() {
                log::error!("Couldn't save corrupted file: {e}");
            }
            return Err(PersistenceError::CorruptedFile(format!("{:?}", self.path())));
        }

        Ok(())
    }

    /// Write contents of buffer from memory onto the persistence file in disk
    pub fn write(&self, buf: &mut BytesMut) -> Result<(), std::io::Error> {
        let path = self.path();
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;

        let hash = seahash::hash(&buf[..]);
        file.write_all(&hash.to_be_bytes())?;
        file.write_all(&buf[..])?;
        file.flush()?;

        Ok(())
    }

    /// Deletes the persistence file from disk
    pub fn delete(&self) -> Result<u64, std::io::Error> {
        let path = self.path();

        // Query the fs to track size of removed persistence file
        let metadata = std::fs::metadata(&path)?;
        let bytes_occupied = metadata.len();

        std::fs::remove_file(&path)?;

        Ok(bytes_occupied)
    }

    /// Reads the file into the buffer and deletes it
    pub fn load_into(&self, buf: &mut BytesMut) -> Result<u64, PersistenceError> {
        self.read(buf)?;
        Ok(self.delete()?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Corrupted persistence file ({0})")]
    CorruptedFile(String),
    #[error("Io error: ({0})")]
    IoError(#[from] std::io::Error),
}
