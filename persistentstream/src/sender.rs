use crate::{get_file_ids, parse_index_file};
use crate::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write, SeekFrom, Seek};

use derive_more::From;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};



/// The Sender half of the persistent channel API. Calls to send method will not block indefinitely.
/// Rotates the data to disk when the channel is full.
pub struct Sender<T> {
    index_file: File,
    /// last failed send used to track orchestration between disk and channel
    last_failed: Option<T>,
    /// list of backlog file ids. Mutated only be the serialization part of the sender
    backlog_file_ids: Vec<u64>,
    /// handle to put data into the queue
    tx: mpsc::Sender<T>,
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


#[derive(Debug, From)]
pub enum Error<T> {
    #[from]
    Io(io::Error),
    Closed(T),
    CrcMisMatch
}

impl<T> Sender<T> where T: Into<Vec<u8>>, Vec<u8>: Into<T> {
    pub fn new(
        backup_path: &Path,
        max_file_size: usize,
        max_file_count: usize,
        tx: mpsc::Sender<T>,
    ) -> Result<Self, Error<T>> {
        let backlog_file_ids = get_file_ids(backup_path)?;
        let (index_file, current_read_file_index) = parse_index_file(backup_path)?;

        debug!("Previous session file count = {:?}", backlog_file_ids.len());
        let sender = Sender {
            index_file,
            last_failed: None,
            backlog_file_ids,
            max_file_size,
            tx,
            backup_path: PathBuf::from(backup_path),
            max_file_count,
            current_read_file_index,
            current_write_file: None,
            current_write_file_size: 0,
            current_read_file: None,
            slow_receiver: false
        };

        Ok(sender)
    }

    /// Opens next file to write to by incrementing current_write_file_id
    /// Handles retention
    fn open_next_write_file(&mut self) -> Result<(), Error<T>> {
        let next_file_id = match self.backlog_file_ids.last() {
            Some(id) => id + 1,
            None => 0
        };

        let next_file_name = format!("backup@{}", next_file_id);
        let next_file_path = self.backup_path.join(&next_file_name);
        let next_file = OpenOptions::new().write(true).create(true).open(&next_file_path)?;
        let next_file = File::new(next_file_path, next_file);

        self.current_write_file = Some(next_file);
        self.backlog_file_ids.push(next_file_id);

        let backlog_files_count = self.backlog_file_ids.len();
        // TODO testcases for max no. of files = 0 and 1
        if backlog_files_count > self.max_file_count {
            // count here will alway be > 0 due to above if statement. safe. doesn't panic
            let id = self.backlog_file_ids.remove(0);
            let file = self.backup_path.join(&format!("backup@{}", id));
            warn!("file limit reached. deleting {:?}", file);
            let file = self.backup_path.join(&file);
            fs::remove_file(file)?;
            self.decrement_reader_index()?;
        }

        debug!("opened next file {:?} to write", self.current_write_file.as_ref().unwrap().path);
        Ok(())
    }

    /// Serializes data to the disk.
    /// Manages segmentation cooperatively. When the current write file size is full, resets
    /// `current_write_file` and `current_write_file_size` to indicate the next iteration of
    /// of this method to open next file to serialize data to
    /// Also manages retention by deleting old files when file retention count is crossed
    /// Deletes the old file right after opening the next write file
    fn serialize_data_to_disk(&mut self, data: Vec<u8>) -> Result<(), Error<T>> {
        if self.current_write_file.is_none() {
            self.open_next_write_file()?;
        }

        if let Some(file) = &mut self.current_write_file {
            debug!("writing data to disk. file {:?} data -> {}", file.path, data[0]);
            // TODO wrap this file into a bufwrite in the sender
            let hash = seahash::hash(&data);
            let len = data.len();

            file.fd.write_u64::<LittleEndian>(hash)?;
            file.fd.write_u32::<LittleEndian>(len as u32)?;
            file.fd.write_all(&data)?;

            // file.fd.flush().await?;

            self.current_write_file_size += len;
            if self.current_write_file_size >= self.max_file_size {
                debug!("file {:?} full!!", file.path);
                // reset file name and size so that next iteration creates new file
                self.current_write_file = None;
                self.current_write_file_size = 0;
                return Ok(())
            }
        }

        Ok(())
    }

    fn decrement_reader_index(&mut self) -> Result<(), Error<T>> {
        let index = if let Some(index) = self.current_read_file_index {
            index as isize - 1
        } else {
            return Ok(())
        };

        if index < 0 {
            self.current_read_file_index = None;
            self.index_file.fd.seek(SeekFrom::Start(0))?;
            self.index_file.fd.write_all(format!("").as_bytes())?;
        } else {
            self.current_read_file_index = Some(index as usize);
            self.index_file.fd.seek(SeekFrom::Start(0))?;
            self.index_file.fd.write_all(format!("{}", index).as_bytes())?;
        }

        Ok(())
    }

    fn increment_reader_index(&mut self) -> Result<(), Error<T>> {
        let index = if let Some(index) = self.current_read_file_index {
            index + 1
        } else {
            0 
        };

        self.current_read_file_index = Some(index);
        // update the index of file which we are done reading 
        // (so that the increment of happens corretly during first read after boot)
        if index > 0 {
            self.index_file.fd.seek(SeekFrom::Start(0))?;
            self.index_file.fd.write_all(format!("{}", index - 1).as_bytes())?;
        } else {
            self.index_file.fd.seek(SeekFrom::Start(0))?;
            self.index_file.fd.write_all(format!("").as_bytes())?;
        }

        Ok(())
    }

    /// Update next read file and delete the last read file 
    fn open_next_read_file(&mut self) -> Result<bool, Error<T>> {
        // TODO: This condition might not hit. Reevaluate
        if self.backlog_file_ids.len() == 0 {
            return Ok(false)
        }

        // get the next backup file id and move the index of current read file
        let id = match self.current_read_file_index {
            Some(index) => if let Some(id) = self.backlog_file_ids.get(index + 1) {
                let id = *id;
                self.increment_reader_index()?;
                id
            } else {
                return Ok(false)
            }
            None => {
                self.current_read_file_index = Some(0);
                let id = self.backlog_file_ids.get(0).unwrap();
                *id
            }
        };

        let next_file_path = self.backup_path.join(&format!("backup@{}", id));
        let next_file = OpenOptions::new().read(true).open(&next_file_path)?;
        let next_file = File::new(next_file_path, next_file);

        debug!("opened next file {:?} to read", next_file.path);
        self.current_read_file = Some(next_file);
        return Ok(true)
    }

    /// Deserializes next entry on the disk.
    /// Jumps from one file to another when the current file is at EOF
    /// Also handles reads on file which also being currently written
    /// Deletes the last read file just before moving to next read file
    fn deserialize_data_from_disk(&mut self) -> Result<Option<Vec<u8>>, Error<T>> {
        loop {
            let current_read_file = if let Some(file) = &mut self.current_read_file {
                file
            } else {
                // opens next file for reading or returns None if there aren't any more files to
                // read
                if !self.open_next_read_file()? {
                    return Ok(None)
                } else {
                    continue
                }
            };

            // get crc, payload length and payload
            let crc = current_read_file.fd.read_u64::<LittleEndian>();
            let crc = match crc {
                Ok(c) => c,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Don't 'continue' to 'open next read file'. That closes current file
                    // which the writer is not done with yet
                    // TODO: Leaves a stray file. Move to index based reading. Removes the need to
                    // delete the file after done reading the file
                    if let Some(f) = &self.current_write_file {
                        if f.path == current_read_file.path {
                            return Ok(None)
                        }
                    };

                    // done reading the current file. mark current_read_file = None to
                    // (cooperatively) open next file to read in the next iteration
                    // NOTE read/write(retention) deletes are lazy. old file is deleted just during
                    // the move to next file
                    debug!("done reading current read file {:?}", current_read_file.path);
                    self.current_read_file = None;
                    continue
                }
                Err(e) => return Err(e.into())
            };

            let len  = current_read_file.fd.read_u32::<LittleEndian>()?;
            let mut payload_buf = vec![0u8; len as usize];
            current_read_file.fd.read_exact(&mut payload_buf)?;

            // verify the checksum
            let de_crc = seahash::hash(&payload_buf);
            if crc != de_crc {
                return Err(Error::CrcMisMatch);
            }

            debug!("reading from disk. file {:?} data -> {}", current_read_file.path, payload_buf[0]);
            return Ok(Some(payload_buf))
        }
    }


    pub async fn sync(&mut self) -> Result<(), Error<T>> {
        if let Some(last_failed) = self.last_failed.take() {
            let mut next_message = last_failed;

            // after writing the current element to the disk, it might be possible the receiver has
            // picked up. Start filling the channel starting with last failed message and
            // proceed to get elements from the disk until all the slots in the channel are full.
            // This makes sure that receiver cathes up with all the backlog files when possible
            loop {
                match self.tx.try_send(next_message) {
                    Ok(_) => {
                        match self.deserialize_data_from_disk()? {
                            Some(data) => {
                                debug!("sent slow data to imq -> {}", data[0]);
                                next_message = data.into();
                            }
                            None => {
                                debug!("disk empty. toggling to fast data mode");
                                self.slow_receiver = false;
                                break
                            },
                        }
                    }
                    Err(TrySendError::Full(data)) => {
                        debug!("failed to send data to imq");
                        self.last_failed = Some(data);
                        break
                    } 
                    Err(TrySendError::Closed(data)) => return Err(Error::Closed(data))
                };
            }
        }

        Ok(())
    }

    /// Sends data to the receiver via inmemeory cahnnel. When the channel limit is
    /// hit, writes current data to disk and changes to slow receiver mode
    /// where all the next iteration writes new elements to disk and try_sends
    /// max possible old elements from file to channel. If all the backlog files
    /// are done, toggle slow_receiver mode where data is directly written to the channel
    pub async fn send(&mut self, data: T) -> Result<(), Error<T>> {
        if self.slow_receiver {
            // write the next element to the disk. 
            self.serialize_data_to_disk(data.into())?;
            // after writing the current element to the disk, it might be possible the receiver has
            // picked up. Start filling the channel starting with last failed message and
            // proceed to get elements from the disk until all the slots in the channel are full.
            // This makes sure that receiver cathes up with all the backlog files when possible
            self.sync().await?;
        } else {
            match self.tx.try_send(data) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(data)) => {
                    debug!("imq full. toggling to slow data mode");
                    // keep the last failed element in memeory. This makes the channel look like it
                    // has capacity + 1 elements. This failed message in the state is used to
                    // instruct the next iteration to start writing next data to the disk.
                    self.last_failed = Some(data);
                    self.slow_receiver = true;
                }
                Err(TrySendError::Closed(data)) => return Err(Error::Closed(data))
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tokio::sync::mpsc;
    use tokio::task;
    use tempdir::TempDir;
    use rand::{distributions::Uniform, Rng};
    use super::Sender;
    use crate::get_file_ids;

    fn init_backup_folders() -> TempDir {
        let backup = TempDir::new("/tmp/persist").unwrap();

        if !backup.path().is_dir() {
            panic!("Folder does not exist");
        } 

        backup
    }

    fn generate_data(size: usize) -> Vec<u8> {
        let range = Uniform::from(0..255);
        rand::thread_rng().sample_iter(&range).take(size).collect()
    }

    #[tokio::test]
    async fn receiver_receives_data_correctly() {
        let backup = init_backup_folders();
        let (tx, mut rx) = mpsc::channel(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        task::spawn(async move {
            for i in 0..10 {
                tx.send(vec![i]).await.unwrap();
            }
        });

        for i in 0..10 {
            let data = rx.recv().await.unwrap();
            assert_eq!(vec![i], data);
        }

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn current_write_file_names_and_sizes_are_incremented_and_reset_correctly_at_edges() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        // Max possible data on disk = 10 * 10K - inmemory count size
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        assert!(tx.current_write_file.is_none());
        assert!(tx.backlog_file_ids.last().is_none());
        for i in 0..100 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();

            match i {
                0..=8 => {
                    let name = format!("backup@{}", 0);
                    let file = backup.path().join(name);
                    assert_eq!(tx.current_write_file.as_ref().unwrap().path, file);
                    assert_eq!(tx.current_write_file_size, (i + 1) * 1024);
                    assert_eq!(*tx.backlog_file_ids.last().unwrap(), 0);
                }
                9 => {
                    // 9th element causes file size to be 10K. this should reset the current write
                    // file name and size so that next iteration of serialize_data_to_disk will
                    // open the next file and write to it
                    assert!(tx.current_write_file.is_none());
                    assert_eq!(tx.current_write_file_size, 0);
                    // this is incremented while opening the next file
                    assert_eq!(*tx.backlog_file_ids.last().unwrap(), 0);
                }
                10..=18 => {
                    let name = format!("backup@{}", 1);
                    let file = backup.path().join(name);
                    assert_eq!(tx.current_write_file.as_ref().unwrap().path, file);
                    assert_eq!(tx.current_write_file_size, (i % 10 + 1) * 1024);
                    assert_eq!(*tx.backlog_file_ids.last().unwrap(), 1);
                }
                19 => {
                    // 9th element causes file size to be 10K. this should reset the current write
                    // file name and size so that next iteration of serialize_data_to_disk will
                    // open the next file and write to it
                    assert!(tx.current_write_file.is_none());
                    assert_eq!(tx.current_write_file_size, 0);
                    // this is incremented while opening the next file
                    assert_eq!(*tx.backlog_file_ids.last().unwrap(), 1);
                }
                _ => ()
            }

        }
    }

    #[tokio::test]
    async fn serializer_cretes_new_file_after_the_current_file_is_full() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        for _ in 0..10u32 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(1, files.len());

        for _ in 0..10u32 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(2, files.len());
    }

    #[tokio::test]
    async fn deletes_old_file_when_file_limit_reached() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        for _i in 0..100u32 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(10, files.len());

        for _i in 0..10u32 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(10, files.len());
    }


    #[tokio::test]
    async fn writer_updates_index_of_reader_correctly_when_it_deletes_a_file() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        // results in 10 files backup@0 to backup@9 with data 0..=99
        for i in 0..100 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(10, files.len());

        // progress the reader to backup@1
        for i in 0..15 {
            let data = tx.deserialize_data_from_disk().unwrap().unwrap();
            assert_eq!(data[0], i);
        }
        
        // results in 11th file backup@10 100..=109. also deletes backup@0
        // the delete moves the stored indexes of `backlog_file_ids`
        for i in 100..110 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(10, files.len());
        
        // progress the reader from backup@1
        for i in 15..110 {
            let data = tx.deserialize_data_from_disk().unwrap().unwrap();
            assert_eq!(data[0], i);
        }
    }


    #[tokio::test]
    async fn next_boot_should_continue_with_correct_file_name_to_write() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        for _i in 0..100u32 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(10, files.len());

        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();
        for _i in 0..10u32 {
            let data = generate_data(1024);
            tx.serialize_data_to_disk(data).unwrap();
        }

        let last_file_id = tx.backlog_file_ids.pop().unwrap();
        assert_eq!(last_file_id, 10);
    }

    #[tokio::test]
    async fn next_boot_should_continue_with_correct_file_to_read() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        // write data to disk. 100K data. 10 files. backup@0..9
        for i in 0..100 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.serialize_data_to_disk(data).unwrap();
        }

        // read 5 files. backup@0..4
        for i in 0..50 {
            let data = tx.deserialize_data_from_disk().unwrap().unwrap();
            assert_eq!(data[0], i);
        }

        // read one more to more read to backup@5 and delete 4
        let data = tx.deserialize_data_from_disk().unwrap().unwrap();
        assert_eq!(data[0], 50);

        // next boot should read from backup@5 and data 50
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();
        for i in 50..100 {
            let data = tx.deserialize_data_from_disk().unwrap().unwrap();
            assert_eq!(data[0], i);
        }
    }

    #[tokio::test]
    async fn read_should_extract_data_from_disk_correctly() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        for i in 0..100 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.serialize_data_to_disk(data).unwrap();
        }

        let files = get_file_ids(&backup.path()).unwrap();
        assert_eq!(10, files.len());

        for i in 0..100 {
            let o = tx.deserialize_data_from_disk().unwrap();
            let data = o.unwrap();
            assert_eq!(i, data[0]);
        }
    }

    #[tokio::test]
    async fn serializing_and_deserializing_the_same_current_file_should_work_correctly() {
        let backup = init_backup_folders();
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(10);
        let mut tx = Sender::new(backup.path(), 10 * 1024, 10, tx).unwrap();

        // file would be partially full at the end of this loop
        for i in 0..5 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.serialize_data_to_disk(data).unwrap();
        }

        // done reading the file which still held for serialization
        for i in 0..5 {
            let o = tx.deserialize_data_from_disk().unwrap();
            let data = o.unwrap();
            assert_eq!(i, data[0]);
        }

        // file would be partially full at the end of this loop
        for i in 5..10 {
            let mut data = generate_data(1024);
            data[0] = i;
            tx.serialize_data_to_disk(data).unwrap();
        }

        // done reading the file which still held for serialization
        for i in 5..10 {
            let o = tx.deserialize_data_from_disk().unwrap();
            let data = o.unwrap();
            assert_eq!(i, data[0]);
        }
    }
}

