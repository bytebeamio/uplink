#![allow(
    clippy::enum_variant_names,
    clippy::unused_unit,
    clippy::let_and_return,
    clippy::not_unsafe_ptr_arg_deref,
    clippy::cast_lossless,
    clippy::blacklisted_name,
    clippy::too_many_arguments,
    clippy::trivially_copy_pass_by_ref,
    clippy::let_unit_value,
    clippy::clone_on_copy
)]
mod jni_c_header;
use jni_c_header::*;

use std::collections::HashMap;
use std::sync::Arc;

use figment::providers::{Data, Json, Toml};
use figment::Figment;
use flume::Receiver;
use log::info;

use uplink::{spawn_uplink, Action, ActionResponse, Config, Payload, Stream};

const DEFAULT_CONFIG: &'static str = r#"
    bridge_port = 5555
    max_packet_size = 102400
    max_inflight = 100
    
    # Whitelist of binaries which uplink can spawn as a process
    # This makes sure that user is protected against random actions
    # triggered from cloud.
    actions = ["tunshell"]
    
    [persistence]
    path = "/tmp/uplink"
    max_file_size = 104857600 # 100MB
    max_file_count = 3
    
    [streams.metrics]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/metrics/jsonarray"
    buf_size = 10
    
    # Action status stream from status messages from bridge
    [streams.action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    buf_size = 1

    [ota]
    enabled = false
    path = "/var/tmp/ota-file"

    [stats]
    enabled = true
    process_names = ["uplink"]
    update_period = 5
"#;

pub struct Uplink {
    action_stream: Stream<ActionResponse>,
    streams: HashMap<String, Stream<Payload>>,
    bridge_rx: Receiver<Action>,
}

impl Uplink {
    pub fn new(auth_config: String) -> Uplink {
        #[cfg(target_os = "android")]
        android_logger::init_once(
            android_logger::Config::default().with_min_level(log::Level::Debug).with_tag("Hello"),
        );
        log_panics::init();
        info!("init log system - done");

        let config: Arc<Config> = Arc::new(
            Figment::new()
                .merge(Data::<Toml>::string(&DEFAULT_CONFIG))
                .merge(Data::<Json>::string(&auth_config))
                .extract()
                .unwrap(),
        );

        let (bridge_rx, tx, action_stream) = spawn_uplink(config.clone()).unwrap();

        let mut streams = HashMap::new();
        for (stream, cfg) in config.streams.iter() {
            streams.insert(
                stream.to_owned(),
                Stream::new(stream.to_owned(), cfg.topic.to_owned(), cfg.buf_size, tx.clone()),
            );
        }

        Uplink { action_stream, streams, bridge_rx }
    }

    pub fn send(&mut self, data: String, stream: String) {
        let data: Payload = serde_json::from_str(&data).unwrap();
        self.streams.get_mut(&stream).unwrap().push(data).unwrap()
    }

    pub fn respond(&mut self, response: String) {
        let response: ActionResponse = serde_json::from_str(&response).unwrap();
        self.action_stream.push(response).unwrap()
    }

    // TODO: this shold become a callback
    pub fn recv(&mut self) -> String {
        let action = self.bridge_rx.recv().unwrap();
        serde_json::to_string(&action).unwrap()
    }
}

include!(concat!(env!("OUT_DIR"), "/java_glue.rs"));
