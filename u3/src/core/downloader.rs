use flume::{Receiver, Sender};

pub struct DownloadInfo {
    pub url: String,
    pub checksum: [u8; 32],
    pub size: u64,
    pub uncompressed_size: u64,
}
pub enum DownloadUpdate {
    Progress(u8, String),
    Completed(String),
    Error(String),
}
/// downloads will be idempotent, downloader will keep the download target file in sync with a metadata.json file
/// this metadata.json file will have the checksum of (maybe partially) downloaded file for verification
/// both will be updated together to ensure proper continuation on interruption, or discarding the corrupted file and retrying
/// in case a clean shutdown didn't happen.
///
/// Collectors that requests a download will listen for updates, send action responses to cloud, and persist the active action to disk
/// When these collectors boot, they'll check if there was an active download when the shutdown happened, if so, they'll try downloading it again
/// and downloader will handle continuations properly.
///
/// component that listens for active actions will keep track of latest action received by the device
/// * if the incoming action is same as or older than the active action, it'll be ignored
/// * on boot, it'll find out the latest action id by checking the persisted actions by other components
///
/// Conditions under which downloads will be aborted:
/// * request fails with a 4xx or 5xx status code
/// * there is zero progress after attempting for 10 minutes
pub async fn download_task(download_requests: Receiver<(DownloadInfo, Sender<DownloadUpdate>)>) {
    loop {
        let request = download_requests.recv_async().await.unwrap();
    }
}
