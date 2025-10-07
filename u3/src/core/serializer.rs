use flume::{Receiver, Sender};
use rumqttc::{AsyncClient, QoS};
use crate::{PublishItem, DataRow};

pub async fn data_task(data_rx: Receiver<DataRow>, batch_tx: AsyncClient) {
    while let Ok(msg) = data_rx.recv_async().await {
        let data = serde_json::to_vec(&[msg.data]).unwrap();
        let _ = batch_tx.publish(String::new(), QoS::AtLeastOnce, false, data).await;
    }
}