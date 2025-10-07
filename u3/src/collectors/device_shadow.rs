use crate::utils::clock;
use crate::{DataRow, PublishItem};
use flume::Sender;
use serde_json::json;
use std::time::Duration;

pub async fn device_shadow_task(data_tx: Sender<DataRow>) {
    let mut sequence = 1;
    loop {
        let _ = data_tx
            .send_async(DataRow {
                stream: "device_shadow".to_owned(),
                data: PublishItem {
                    sequence,
                    timestamp: clock(),
                    data: json!({
                        "uplink_version": env!("CARGO_PKG_VERSION")
                    }),
                },
            })
            .await;
        sequence += 1;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
