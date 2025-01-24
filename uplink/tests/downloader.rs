use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use flume::bounded;
use serde_json::json;
use tempdir::TempDir;

use uplink::{
    base::bridge::{BridgeTx, DataTx, StatusTx},
    collector::downloader::{DownloadFile, FileDownloader},
    uplink_config::{ActionRoute, Config, DownloaderConfig},
    Action,
};

// Prepare config
fn test_config(temp_dir: &Path, test_name: &str) -> Config {
    let mut path = PathBuf::from(temp_dir);
    path.push(test_name);
    let mut config = Config::default();
    config.downloader = DownloaderConfig {
        actions: vec![ActionRoute {
            name: "firmware_update".to_owned(),
            cancellable: true,
        }],
        path,
    };

    config
}

#[test]
// Test file downloading capabilities of FileDownloader by downloading the uplink logo from GitHub
fn download_file() {
    let temp_dir = TempDir::new("download_file").unwrap();
    let config = test_config(temp_dir.path(), "download_file");
    let mut downloader_path = config.downloader.path.clone();

    let (tx, _) = bounded(2);
    let (inner, status_rx) = bounded(2);
    let bridge_tx = BridgeTx { data_tx: DataTx { inner: tx }, status_tx: StatusTx { inner } };

    // Create channels to forward and push actions on
    let (download_tx, download_rx) = bounded(1);
    let (_, ctrl_rx) = bounded(1);
    let downloader = FileDownloader::new(
        Arc::new(config),
        &None,
        download_rx,
        bridge_tx,
        ctrl_rx,
        Arc::new(Mutex::new(false)),
    )
    .unwrap();

    // Start FileDownloader in separate thread
    std::thread::spawn(|| downloader.start());

    // Create a firmware update action
    let download_update = DownloadFile {
        url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
        content_length: 296658,
        file_name: "test.txt".to_string(),
        download_path: None,
        checksum: None,
    };
    let mut expected_forward = download_update.clone();
    downloader_path.push("firmware_update");
    downloader_path.push("test.txt");
    expected_forward.download_path = Some(downloader_path);
    let download_action = Action {
        action_id: "1".to_string(),
        name: "firmware_update".to_string(),
        payload: json!(download_update).to_string(),
    };

    std::thread::sleep(Duration::from_millis(10));

    // Send action to FileDownloader with Sender<Action>
    download_tx.try_send(download_action).unwrap();

    // Collect action_status and ensure it is as expected
    let status = status_rx.recv().unwrap();
    assert_eq!(status.state, "Downloading");
    let mut progress = 0;

    // Collect and ensure forwarded action contains expected info
    loop {
        let status = status_rx.recv().unwrap();

        assert!(progress <= status.progress);
        progress = status.progress;

        if status.is_done() {
            let fwd_action = status.done_response.unwrap();
            let fwd = serde_json::from_str(&fwd_action.payload).unwrap();
            assert_eq!(expected_forward, fwd);
            break;
        }
    }
}

#[test]
// Once a file is downloaded FileDownloader must check it's checksum value against what is provided
fn checksum_of_file() {
    let temp_dir = TempDir::new("file_checksum").unwrap();
    let config = test_config(temp_dir.path(), "file_checksum");

    let (tx, _) = bounded(2);
    let (inner, status_rx) = bounded(2);
    let bridge_tx = BridgeTx { data_tx: DataTx { inner: tx }, status_tx: StatusTx { inner } };

    // Create channels to forward and push action_status on
    let (download_tx, download_rx) = bounded(1);
    let (_, ctrl_rx) = bounded(1);
    let downloader = FileDownloader::new(
        Arc::new(config),
        &None,
        download_rx,
        bridge_tx,
        ctrl_rx,
        Arc::new(Mutex::new(false)),
    )
    .unwrap();

    // Start FileDownloader in separate thread
    std::thread::spawn(|| downloader.start());

    std::thread::sleep(Duration::from_millis(10));

    // Correct firmware update action
    let correct_update = DownloadFile {
        url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
        content_length: 296658,
        file_name: "logo.png".to_string(),
        download_path: None,
        checksum: Some(
            "e22d4a7cf60ad13bf885c6d84af2f884f0c044faf0ee40b2e3c81896b226b2fc".to_string(),
        ),
    };
    let correct_action = Action {
        action_id: "1".to_string(),
        name: "firmware_update".to_string(),
        payload: json!(correct_update).to_string(),
    };

    // Send the correct action to FileDownloader
    download_tx.try_send(correct_action).unwrap();

    // Collect action_status and ensure it is as expected
    let status = status_rx.recv().unwrap();
    assert_eq!(status.state, "Downloading");
    let mut progress = 0;

    // Collect and ensure forwarded action contains expected info
    loop {
        let status = status_rx.recv().unwrap();

        assert!(progress <= status.progress);
        progress = status.progress;

        if status.is_done() {
            if status.state != "Downloaded" {
                panic!("unexpected status={status:?}")
            }
            break;
        }
    }

    // Wrong firmware update action
    let wrong_update = DownloadFile {
        url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
        content_length: 296658,
        file_name: "logo.png".to_string(),
        download_path: None,
        checksum: Some("abcd1234efgh5678".to_string()),
    };
    let wrong_action = Action {
        action_id: "1".to_string(),
        name: "firmware_update".to_string(),
        payload: json!(wrong_update).to_string(),
    };

    // Send the wrong action to FileDownloader
    download_tx.try_send(wrong_action).unwrap();

    // Collect action_status and ensure it is as expected
    let status = status_rx.recv().unwrap();
    assert_eq!(status.state, "Downloading");
    let mut progress = 0;

    // Collect and ensure forwarded action contains expected info
    loop {
        let status = status_rx.recv().unwrap();

        assert!(progress <= status.progress);
        progress = status.progress;

        if status.is_done() {
            assert!(status.is_failed());
            assert_eq!(status.errors, vec!["Downloaded file has unexpected checksum"]);
            break;
        }
    }
}
