use flume::{Receiver, Sender};
use crate::{PublishPayload, DataRow};

pub async fn data_task(data_rx: Receiver<DataRow>, batch_tx: Sender<(String, Vec<PublishPayload>)>) {
    while let Ok(msg) = data_rx.recv_async().await {
        let _ = batch_tx.send_async((msg.stream, vec![msg.data])).await;
    }
}