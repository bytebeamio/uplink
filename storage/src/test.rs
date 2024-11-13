use super::*;
use rumqttc::{Packet, Publish, QoS};
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
        if let Err(Error::NoMoreBackups) = storage.reload_on_eof() {
            break;
        }

        match Packet::read(storage.reader(), 1048).unwrap() {
            Packet::Publish(p) => publishes.push(p),
            packet => unreachable!("{packet:?}"),
        }
    }

    publishes
}

#[test]
fn flush_creates_new_file_after_size_limit() {
    // 1036 is the size of a publish message with topic = "hello", qos = 1, payload = 1024 bytes
    let backup = init_backup_folders();
    let mut storage = Storage::new("test", 10 * 1036);
    storage.enable_persistence(backup.path(), 10).unwrap();

    // 2 files on disk and a partially filled in memory buffer
    write_n_publishes(&mut storage, 101);

    // 1 message in in memory writer
    assert_eq!(storage.writer().len(), 1036);

    // other messages on disk
    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn old_file_is_deleted_after_limit() {
    let backup = init_backup_folders();
    let mut storage = Storage::new("test", 10 * 1036);
    storage.enable_persistence(backup.path(), 10).unwrap();

    // 11 files created. 10 on disk
    write_n_publishes(&mut storage, 110);

    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // 11 files created. 10 on disk
    write_n_publishes(&mut storage, 10);

    assert_eq!(storage.writer().len(), 0);
    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
}

#[test]
fn reload_loads_correct_file_into_memory() {
    let backup = init_backup_folders();
    let mut storage = Storage::new("test", 10 * 1036);
    storage.enable_persistence(backup.path(), 10).unwrap();

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
    storage.enable_persistence(backup.path(), 10).unwrap();

    // 10 files on disk and partially filled current write buffer
    write_n_publishes(&mut storage, 105);

    // breaks after 100th iteration due to `reload_on_eof` break
    let publishes = read_n_publishes(&mut storage, 12345);

    assert!(storage.write_buffer.is_empty());
    assert_eq!(publishes.len(), 105);
    for (i, publish) in publishes.iter().enumerate() {
        assert_eq!(&publish.payload[..], vec![i as u8; 1024].as_slice());
    }
}

#[test]
fn ensure_file_remove_on_read_completion_only() {
    let backup = init_backup_folders();
    let mut storage = Storage::new("test", 10 * 1036);
    storage.enable_persistence(backup.path(), 10).unwrap();
    // 10 files on disk and partially filled current write buffer, 10 publishes per file
    write_n_publishes(&mut storage, 105);

    // Initially not on a read file
    assert_eq!(storage.persistence.as_ref().unwrap().current_read_file_id, None);

    // Ensure unread files are all present before read
    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // Successfully read 10 files with files still in storage after 10 reads
    for i in 0..10 {
        read_n_publishes(&mut storage, 10);
        let file_id = storage.persistence.as_ref().unwrap().current_read_file_id.unwrap();
        assert_eq!(file_id, i);
        // Ensure partially read file is still present in backup dir
        let files = collect_file_ids(&backup.path()).unwrap();
        assert!(files.contains(&i));
    }

    // All read files should be deleted just after 1 more read
    read_n_publishes(&mut storage, 1);
    assert_eq!(storage.persistence.as_ref().unwrap().current_read_file_id, None);

    // Ensure read files are all present before read
    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![]);
}

#[test]
fn ensure_files_including_read_removed_post_flush_on_overflow() {
    let backup = init_backup_folders();
    let mut storage = Storage::new("test", 10 * 1036);
    storage.enable_persistence(backup.path(), 10).unwrap();
    // 10 files on disk and partially filled current write buffer, 10 publishes per file
    write_n_publishes(&mut storage, 105);

    // Initially not on a read file
    assert_eq!(storage.persistence.as_ref().unwrap().current_read_file_id, None);

    // Successfully read a single file
    read_n_publishes(&mut storage, 10);
    let file_id = storage.persistence.as_ref().unwrap().current_read_file_id.unwrap();
    assert_eq!(file_id, 0);

    // Ensure all persistance files still exist
    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // Write 10 more files onto disk, 10 publishes per file
    write_n_publishes(&mut storage, 100);

    // Ensure none of the earlier files exist on disk
    let files = collect_file_ids(&backup.path()).unwrap();
    assert_eq!(files, vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
}
