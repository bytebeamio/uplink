use std::collections::HashMap;

use flume::{bounded, Receiver, Sender};
use log::{error, warn};
use serde_json::{json, Map, Value};
use tokio::{select, task::JoinSet, time::interval};

use crate::{
    base::{
        bridge::{BridgeTx, Payload},
        clock,
    },
    config::{Field, JoinConfig, NoDataAction, PushInterval, SelectConfig},
};

type Json = Map<String, Value>;

pub struct Router {
    map: HashMap<String, Vec<Sender<(String, Json)>>>,
    pub tasks: JoinSet<()>,
}

impl Router {
    pub async fn new(
        configs: Vec<JoinConfig>,
        bridge_tx: BridgeTx,
        back_tx: Sender<Payload>,
    ) -> Self {
        let mut map: HashMap<String, Vec<Sender<(String, Json)>>> = HashMap::new();
        let mut tasks = JoinSet::new();
        for config in configs {
            let (tx, rx) = bounded(1);
            let mut fields = HashMap::new();
            for stream in &config.construct_from {
                if let SelectConfig::Fields(selected_fields) = &stream.select_fields {
                    let renames: &mut HashMap<String, Field> =
                        fields.entry(stream.input_stream.to_owned()).or_default();
                    for field in selected_fields {
                        renames.insert(field.original.to_owned(), field.to_owned());
                    }
                }
                if let Some(senders) = map.get_mut(&stream.input_stream) {
                    senders.push(tx.clone());
                    continue;
                }
                map.insert(stream.input_stream.to_owned(), vec![tx.clone()]);
            }
            let joiner = Joiner {
                rx,
                joined: Map::new(),
                config,
                tx: bridge_tx.clone(),
                fields,
                back_tx: back_tx.clone(),
                sequence: 0,
            };
            tasks.spawn(joiner.start());
        }

        Router { map, tasks }
    }

    pub async fn map(&mut self, input_stream: String, json: Json) {
        let Some(iter) = self.map.get(&input_stream) else { return };
        for tx in iter {
            _ = tx.send_async((input_stream.clone(), json.clone())).await;
        }
    }
}

struct Joiner {
    rx: Receiver<(String, Json)>,
    joined: Json,
    config: JoinConfig,
    fields: HashMap<String, HashMap<String, Field>>,
    tx: BridgeTx,
    back_tx: Sender<Payload>,
    sequence: u32,
}

impl Joiner {
    async fn start(mut self) {
        let PushInterval::OnTimeout(period) = self.config.push_interval_s else {
            loop {
                match self.rx.recv_async().await {
                    Ok((stream_name, json)) => self.update(stream_name, json),
                    Err(e) => {
                        error!("{e}");
                        return;
                    }
                }
                self.send_data().await;
            }
        };
        let mut ticker = interval(period);
        loop {
            select! {
                r = self.rx.recv_async() => {
                    match r {
                        Ok((stream_name, json)) => self.update(stream_name, json),
                        Err(e) => {
                            error!("{e}");
                            return;
                        }
                    }
                }

                _ = ticker.tick() => {
                    self.send_data().await
                }
            }
        }
    }

    // Use data sequence and timestamp if data is to be pushed instantly
    fn is_insertable(&self, key: &str) -> bool {
        match key {
            "timestamp" | "sequence" => self.config.push_interval_s == PushInterval::OnNewData,
            _ => true,
        }
    }

    fn update(&mut self, stream_name: String, json: Json) {
        if let Some(map) = self.fields.get(&stream_name) {
            for (mut key, value) in json {
                // drop unenumerated keys from json
                let Some(field) = map.get(&key) else { continue };
                if let Some(name) = &field.renamed {
                    name.clone_into(&mut key);
                }

                if self.is_insertable(&key) {
                    self.joined.insert(key, value);
                }
            }
        } else {
            // Select All if no mapping exists
            for (key, value) in json {
                if self.is_insertable(&key) {
                    self.joined.insert(key, value);
                }
            }
        }
    }

    async fn send_data(&mut self) {
        if self.joined.is_empty() {
            return;
        }

        // timestamp and sequence values should be passed as is for instant push, else use generated values
        let timestamp = self
            .joined
            .remove("timestamp")
            .and_then(|value| {
                value.as_i64().map_or_else(
                    || {
                        warn!(
                            "timestamp: {value:?} has unexpected type; defaulting to system time"
                        );
                        None
                    },
                    |v| Some(v as u64),
                )
            })
            .unwrap_or_else(|| clock() as u64);
        let sequence = self
            .joined
            .remove("sequence")
            .and_then(|value| {
                value.as_i64().map_or_else(
                    || {
                        warn!(
                            "sequence: {value:?} has unexpected type; defaulting to internal sequence"
                        );
                        None
                    },
                    |v| Some(v as u32),
                )
            })
            .unwrap_or_else(|| {
                self.sequence += 1;
                self.sequence
            });
        let payload = Payload {
            stream: self.config.name.clone(),
            sequence,
            timestamp,
            payload: json!(self.joined),
        };
        if self.config.publish_on_service_bus {
            _ = self.back_tx.send_async(payload.clone()).await;
        }
        self.tx.send_payload(payload).await;
        if self.config.no_data_action == NoDataAction::Null {
            self.joined.clear();
        }
    }
}
