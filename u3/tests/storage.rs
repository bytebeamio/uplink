use bytes::Bytes;
use rumqttc::{Publish, QoS};
use u3::core::storage::{DirectoryStorage, Storage, StorageReadError};

#[test]
fn corrupted_files() {
    let dir = tempdir::TempDir::new("uplink").unwrap();
    std::fs::write(dir.path().join("backup@1"), "invalid content").unwrap();
    std::fs::write(dir.path().join("backup@2"), "invalid content").unwrap();
    let mut storage = DirectoryStorage::new(dir.path().to_owned(), 100, 10, 1024 * 1000).unwrap();
    storage.write_packet(create_publish("test_topic", 1)).unwrap();
    match storage.read_packet() {
        Err(StorageReadError::InvalidPacket(_)) => {}
        _ => panic!("boo"),
    }
    let files = std::fs::read_dir(dir.path()).unwrap().collect::<Vec<_>>();
    assert_eq!(files.len(), 2);
    match storage.read_packet() {
        Err(StorageReadError::InvalidPacket(_)) => {}
        _ => panic!("boo"),
    }
    let files = std::fs::read_dir(dir.path()).unwrap().collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert_eq!(files.first().unwrap().as_ref().unwrap().file_name(), "backup@corrupted");
    match storage.read_packet() {
        Ok(p) => {
            assert_eq!(p.topic, "test_topic");
            assert_eq!(std::str::from_utf8(p.payload.as_ref()).unwrap(), "1");
        }
        _ => panic!("boo"),
    }
}

#[test]
fn extra_files_in_backup() {
    let dir = tempdir::TempDir::new("uplink").unwrap();
    let mut storage = DirectoryStorage::new(dir.path().to_owned(), 100, 10, 1024 * 1000).unwrap();
    for idx in 0..10 {
        storage.write_packet(create_publish("test_topic", idx)).unwrap();
    }
    storage.flush().unwrap();
    drop(storage);

    let mut files =
        std::fs::read_dir(dir.path()).unwrap().filter_map(|e| e.ok()).collect::<Vec<_>>();
    assert_eq!(files.len(), 2);
    files.sort_by_key(|e| e.file_name());
    let file = files.get(1).unwrap().path();
    std::fs::copy(file.as_path(), dir.path().join("backup@3")).unwrap();
    std::fs::copy(file.as_path(), dir.path().join("backup@4")).unwrap();
    std::fs::copy(file.as_path(), dir.path().join("extra_file")).unwrap();

    let mut storage = DirectoryStorage::new(dir.path().to_owned(), 100, 3, 1024 * 1000).unwrap();
    let files = std::fs::read_dir(dir.path()).unwrap().filter_map(|e| e.ok()).collect::<Vec<_>>();
    assert_eq!(files.len(), 4);

    for idx in (7..10).chain(7..10).chain(7..10) {
        let p = storage.read_packet().unwrap();
        assert_eq!(p.topic, "test_topic");
        assert_eq!(std::str::from_utf8(p.payload.as_ref()).unwrap(), idx.to_string());
    }

    assert!(matches!(storage.read_packet(), Err(StorageReadError::Empty)));
}

fn create_publish(topic: &str, i: u32) -> Publish {
    Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.to_owned(),
        pkid: 1,
        payload: Bytes::from(i.to_string()),
    }
}
