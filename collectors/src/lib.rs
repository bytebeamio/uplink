use base::{Action, ActionResponse, CollectorRx, CollectorTx, Payload};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[cfg(feature = "simulator")]
pub mod simulator;

pub struct DataTx {
    pub data_tx: SplitSink<Framed<TcpStream, LinesCodec>, String>,
}

#[async_trait::async_trait]
impl CollectorTx for DataTx {
    async fn send_action_response(&mut self, response: ActionResponse) -> Result<(), ()> {
        self.data_tx.send(serde_json::to_string(&response).unwrap()).await.map_err(|_| ())
    }

    async fn send_payload(&mut self, payload: Payload) -> Result<(), ()> {
        self.data_tx.send(serde_json::to_string(&payload).unwrap()).await.map_err(|_| ())
    }

    fn send_payload_sync(&mut self, _: Payload) -> Result<(), ()> {
        unimplemented!();
    }
}

pub struct ActionsRx {
    pub actions_rx: SplitStream<Framed<TcpStream, LinesCodec>>,
}

#[async_trait::async_trait]
impl CollectorRx for ActionsRx {
    async fn recv_action(&mut self) -> Result<Action, ()> {
        let line = self.actions_rx.next().await.ok_or(())?.map_err(|_| ())?;
        serde_json::from_str(&line).unwrap()
    }
}
