use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use anyhow::Context;
use bytes::{Buf, BufMut, BytesMut};
use rumqttc::{Packet, Publish};

/// A persistent queue abstraction used by Serializer
/// Data coming from system is written to it using `write_packet`
/// `read_packet` is used to pull data from it and send to the mqtt module
/// There are three implementations, in memory, disk backed, and a wrapper for prioritizing live data
pub trait Storage {
    fn name(&self) -> &str;

    /// Read a packet from the queue, returning an error if it's empty
    fn read_packet(&mut self) -> Result<Publish, StorageReadError>;

    /// Push a packet to the queue
    fn write_packet(&mut self, packet: Publish) -> Result<(), StorageWriteError>;

    /// Flush everything to the disk
    /// Called during shutdown or crash
    fn flush(&mut self) -> Result<(), StorageFlushError>;

    /// Consume and convert into an in memory storage
    /// Used in case of file system errors
    fn to_in_memory(&self) -> Box<dyn Storage>;

    fn metrics(&self) -> StorageMetrics;
}

pub struct StorageMetrics {
    pub read_buffer_size: u64,
    pub write_buffer_size: u64,
    pub bytes_on_disk: u64,
    pub files_count: u32,
    pub lost_files: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Incoming data is written to `write_buffer`
/// `read_buffer` is used for reading
/// They are swapped if `write_buffer` becomes full or `read_buffer` becomes empty
#[derive(Clone)]
pub struct InMemoryStorage {
    name: String,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    buf_size: usize,
    max_packet_size: usize,
    lost_files: u32,
}

impl InMemoryStorage {
    pub fn new<S: Into<String>>(name: S, buf_size: usize, max_packet_size: usize) -> Self {
        InMemoryStorage {
            name: name.into(),
            read_buffer: BytesMut::with_capacity(buf_size + buf_size / 2),
            write_buffer: BytesMut::with_capacity(buf_size + buf_size / 2),
            buf_size,
            max_packet_size,
            lost_files: 0,
        }
    }
}

impl Storage for InMemoryStorage {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn read_packet(&mut self) -> Result<Publish, StorageReadError> {
        if self.read_buffer.is_empty() {
            if self.write_buffer.is_empty() {
                return Err(StorageReadError::Empty);
            } else {
                std::mem::swap(&mut self.read_buffer, &mut self.write_buffer);
                return self.read_packet();
            }
        }
        match Packet::read(&mut self.read_buffer, self.max_packet_size) {
            Ok(Packet::Publish(packet)) => {
                return Ok(packet);
            }
            Ok(_p) => {
                return Err(StorageReadError::UnsupportedPacketType);
            }
            Err(e) => {
                self.read_buffer.clear();
                return Err(StorageReadError::InvalidPacket(e));
            }
        }
    }

    fn write_packet(&mut self, packet: Publish) -> Result<(), StorageWriteError> {
        if packet.size() >= self.max_packet_size {
            return Err(StorageWriteError::InvalidPacket(rumqttc::Error::OutgoingPacketTooLarge { pkt_size: packet.size(), max: self.max_packet_size }));
        }
        if self.write_buffer.len() + packet.size() > self.buf_size {
            log::info!("Storage::write_packet({}) Full! Rotating in memory buffers.", self.name);
            std::mem::swap(&mut self.read_buffer, &mut self.write_buffer);
            if !self.write_buffer.is_empty() {
                self.lost_files += 1;
            }
            self.write_buffer.clear();
        }
        match packet.write(&mut self.write_buffer) {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(StorageWriteError::InvalidPacket(e))
            }
        }
    }

    fn flush(&mut self) -> Result<(), StorageFlushError> {
        self.read_buffer.clear();
        self.write_buffer.clear();
        Ok(())
    }

    fn to_in_memory(&self) -> Box<dyn Storage> {
        Box::new(self.clone())
    }

    fn metrics(&self) -> StorageMetrics {
        let result = StorageMetrics {
            read_buffer_size: self.read_buffer.len() as _,
            write_buffer_size: self.write_buffer.len() as _,
            bytes_on_disk: 0,
            files_count: 0,
            // lost files for in memory storage is the number of times non-empty read buffer had to be cleared
            lost_files: self.lost_files,
        };
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DirectoryStorage {
    dir: PathBuf,
    files_queue: VecDeque<i32>,
    write_buffer: BytesMut,
    read_buffer: BytesMut,
    max_file_size: usize,
    max_file_count: usize,
    max_packet_size: usize,
    bytes_on_disk: u64,
    lost_files: u32,
}

impl DirectoryStorage {
    pub fn new(dir: PathBuf, max_file_size: usize, max_file_count: usize, max_packet_size: usize) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)
            .context(format!("couldn't create persistence directory: {dir:?}"))?;
        let mut result = Self {
            dir,
            files_queue: VecDeque::with_capacity(max_file_count),
            read_buffer: BytesMut::with_capacity(max_file_size + max_file_size / 2),
            write_buffer: BytesMut::with_capacity(max_file_size + max_file_size / 2),
            max_file_size,
            max_file_count,
            max_packet_size,
            bytes_on_disk: 0,
            lost_files: 0,
        };
        result.load_file_ids()
            .context(format!("Failed to load persistence files from ({:?})", result.dir))?;
        log::info!("Loaded file ids: {:?}", result.files_queue);
        Ok(result)
    }

    pub fn load_file_ids(&mut self) -> anyhow::Result<()> {
        let mut file_ids = Vec::new();
        let files = std::fs::read_dir(self.dir.as_path())?;
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
        if file_ids.len() > self.max_file_count {
            let to_drop = file_ids.len() - self.max_file_count;
            for (path, _, _) in file_ids.iter().take(to_drop) {
                if let Err(e) = std::fs::remove_file(path) {
                    log::error!("Storage::load_file_ids({}) Couldn't delete excess persistence files: {e}", self.name());
                }
            }
            file_ids.drain(0..to_drop);
        }
        self.files_queue.extend(file_ids.iter().map(|(_, id, _)| id));
        for (_, _, size) in file_ids.iter() {
            self.bytes_on_disk += size;
        }

        Ok(())
    }
}

impl Storage for DirectoryStorage {
    fn name(&self) -> &str {
        self.dir.file_name()
            .and_then(|c| c.to_str())
            .unwrap_or("{}")
    }

    /// if read_buffer_is_empty() {
    ///     if there_are_no_persistence_files() {
    ///         if write_buffer_is_empty() {
    ///           return Err::Empty;
    ///         } else {
    ///           swap_read_and_write_buffers();
    ///         }
    ///     } else {
    ///         load_first_persistence_file_into_memory();
    ///     }
    /// }
    /// pull_data_from_read_buffer();
    fn read_packet(&mut self) -> Result<Publish, StorageReadError> {
        if self.read_buffer.is_empty() {
            match self.files_queue.pop_front() {
                None => {
                    if self.write_buffer.is_empty() {
                        return Err(StorageReadError::Empty);
                    } else {
                        std::mem::swap(&mut self.read_buffer, &mut self.write_buffer);
                    }
                }
                Some(id) => {
                    match PersistenceFile::new(self.dir.as_path(), format!("backup@{id}"))
                        .load_into(&mut self.read_buffer) {
                        Ok(deleted_size) => {
                            self.bytes_on_disk -= deleted_size;
                        }
                        Err(PersistenceError::CorruptedFile(_path)) => {}
                        Err(PersistenceError::IoError(e)) => {
                            return Err(StorageReadError::FileSystemError(e));
                        }
                    }
                }
            }
        }
        match Packet::read(&mut self.read_buffer, self.max_packet_size) {
            Ok(Packet::Publish(packet)) => {
                Ok(packet)
            }
            Ok(_p) => {
                Err(StorageReadError::UnsupportedPacketType)
            }
            Err(e) => {
                self.read_buffer.clear();
                Err(StorageReadError::InvalidPacket(e))
            }
        }
    }

    /// if no_space_in_write_buffer() {
    ///     if persitence_file_count_limit_reached() {
    ///         drop_read_buffer();
    ///         load_oldest_persistence_file_into_read_buffer();
    ///     }
    ///     flush_write_buffer_to_disk();
    /// }
    /// append_to_write_buffer();
    fn write_packet(&mut self, packet: Publish) -> Result<(), StorageWriteError> {
        if packet.size() >= self.max_packet_size {
            return Err(StorageWriteError::InvalidPacket(rumqttc::Error::OutgoingPacketTooLarge { pkt_size: packet.size(), max: self.max_packet_size }));
        }
        if self.write_buffer.len() >= self.max_file_size {
            if self.files_queue.len() >= self.max_file_count {
                self.read_buffer.clear();
                self.lost_files += 1;
                let id = self.files_queue.pop_front().unwrap();
                let pf = PersistenceFile::new(self.dir.as_path(), format!("backup@{id}"));
                match pf.load_into(&mut self.read_buffer) {
                    Ok(deleted_size) => {
                        log::info!("File count reached, deleted oldest persistence file ({}):({})", self.name(), pf.file_name);
                        self.bytes_on_disk -= deleted_size;
                    }
                    Err(PersistenceError::CorruptedFile(_path)) => {}
                    Err(PersistenceError::IoError(e)) => {
                        return Err(StorageWriteError::FileSystemError(e));
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
                    return Err(StorageWriteError::FileSystemError(e));
                }
            }
        }

        match packet.write(&mut self.write_buffer) {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(StorageWriteError::InvalidPacket(e))
            }
        }
    }

    /// save_read_buffer_to_disk();
    /// save_write_buffer_to_disk();
    fn flush(&mut self) -> Result<(), StorageFlushError> {
        let mut result = Ok(());
        if ! self.read_buffer.is_empty() {
            let read_file_id = self.files_queue.iter().next().cloned().unwrap_or(1) - 1;
            if let Err(e) = PersistenceFile::new(self.dir.as_path(), format!("backup@{read_file_id}"))
                .write(&mut self.read_buffer) {
                result = Err(StorageFlushError::FileSystemError(e));
            }
        }
        if ! self.write_buffer.is_empty() {
            let write_file_id = self.files_queue.iter().last().cloned().unwrap_or(1) + 1;
            if let Err(e) = PersistenceFile::new(self.dir.as_path(), format!("backup@{write_file_id}"))
                .write(&mut self.write_buffer) {
                result = Err(StorageFlushError::FileSystemError(e));
            }
        }
        return result;
    }

    fn to_in_memory(&self) -> Box<dyn Storage> {
        Box::new(InMemoryStorage {
            name: self.name().to_owned(),
            read_buffer: self.read_buffer.clone(),
            write_buffer: self.write_buffer.clone(),
            buf_size: self.max_file_size,
            max_packet_size: self.max_packet_size,
            lost_files: self.lost_files,
        })
    }

    fn metrics(&self) -> StorageMetrics {
        StorageMetrics {
            read_buffer_size: self.read_buffer.len() as _,
            write_buffer_size: self.write_buffer.len() as _,
            bytes_on_disk: self.bytes_on_disk,
            files_count: self.files_queue.len() as _,
            lost_files: self.lost_files,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LiveDataPrioritizingStorage {
    inner: Box<dyn Storage>,
    latest_data: Option<Publish>,
}

impl LiveDataPrioritizingStorage {
    pub fn wrap(inner: Box<dyn Storage>) -> Self {
        Self {
            inner,
            latest_data: None,
        }
    }
}

impl Storage for LiveDataPrioritizingStorage {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn read_packet(&mut self) -> Result<Publish, StorageReadError> {
        if let Some(packet) = self.latest_data.take() {
            Ok(packet)
        } else {
            self.inner.read_packet()
        }
    }

    fn write_packet(&mut self, new_packet: Publish) -> Result<(), StorageWriteError> {
        if let Some(last_packet) = self.latest_data.replace(new_packet) {
            self.inner.write_packet(last_packet)
        } else {
            Ok(())
        }
    }

    fn flush(&mut self) -> Result<(), StorageFlushError> {
        if let Some(packet) = self.latest_data.take() {
            if let Err(e) = self.inner.write_packet(packet) {
                log::error!("(Storage::{}) failed to save live data when flushing: {e:?}", self.name());
            }
        }
        self.inner.flush()
    }

    fn to_in_memory(&self) -> Box<dyn Storage> {
        Box::new(
            Self {
                latest_data: self.latest_data.clone(),
                inner: self.inner.to_in_memory(),
            }
        )
    }

    fn metrics(&self) -> StorageMetrics {
        self.inner.metrics()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum StorageReadError {
    /// Nothing left in storage, poll the storage with lower priority
    Empty,
    /// Encountered a file system error (permission etc), log error and revert to in memory persistence
    FileSystemError(std::io::Error),
    /// Should never happen because we write valid packets to storage and files on disk have a checksum,
    /// If this is returned that means the buffer with this packet has been cleared, try polling again
    InvalidPacket(rumqttc::Error),
    /// Should never happen because we only push Publish packets to storage, try polling again
    /// TODO: add packet info to this type
    UnsupportedPacketType,
}

#[derive(Debug)]
pub enum StorageWriteError {
    /// Log warning and ignore
    InvalidPacket(rumqttc::Error),
    /// Encountered a file system error (permission etc), log error and revert to in memory persistence
    FileSystemError(std::io::Error),
}

#[derive(Debug)]
pub enum StorageFlushError {
    FileSystemError(std::io::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn id(path: &Path) -> Option<i32> {
    if let Some(file_name) = path.file_name() {
        let file_name = file_name.to_str()?;
        if !file_name.starts_with("backup@") {
            None
        } else {
            file_name[7..]
                .parse()
                .ok()
        }
    } else {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A utility handle to a persistence file on disk
/// You can do operations like, read, write, delete, save it to a special location if corrupted
pub struct PersistenceFile<'a> {
    /// Path to the persistence directory
    dir: &'a Path,
    /// Name of the file e.g. `backup@1`
    file_name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Corrupted persistence file ({0})")]
    CorruptedFile(String),
    #[error("Io error: ({0})")]
    IoError(#[from] std::io::Error),
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
