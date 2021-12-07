use std::sync::Arc;
use std::thread;

use figment::providers::{Data, Toml};
use figment::Figment;
use flume::Receiver;
use jni::objects::{JClass, JString};
use jni::JNIEnv;

use jni::sys::jstring;
use uplink::{spawn_uplink, ActionResponse, Config, Stream, Action};

const DEFAULT_CONFIG: &'static str = r#"
    device_id = "123"
    project_id = "demo"
    broker = "localhost"
    port = 1883
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

pub struct UplinkConnector {
    stream: Stream<ActionResponse>,
    bridge_rx: Receiver<Action>,
}

impl UplinkConnector {}

#[no_mangle]
pub extern "system" fn Java_Uplink_start(_env: JNIEnv, _class: JClass) -> *mut UplinkConnector {
    let config: Arc<Config> =
        Arc::new(Figment::new().merge(Data::<Toml>::string(DEFAULT_CONFIG)).extract().unwrap());

    let (bridge_tx, bridge_rx) = flume::bounded(10);
    let (collector_tx, collector_rx) = flume::bounded(10);
    let action_status_topic = &config.streams.get("action_status").unwrap().topic;
    let action_status = Stream::new("action_status", action_status_topic, 1, collector_tx.clone());

    let conn = UplinkConnector { stream: action_status.clone(), bridge_rx };

    let rt = tokio::runtime::Runtime::new().unwrap();
    thread::spawn(move || {
        rt.block_on(spawn_uplink(config, bridge_tx, collector_rx, collector_tx, action_status))
            .unwrap();
    });

    Box::into_raw(Box::new(conn))
}

#[no_mangle]
/// # Safety
pub unsafe extern "system" fn Java_Uplink_send(
    env: JNIEnv,
    _class: JClass,
    obj: *mut UplinkConnector,
    response: JString,
) {
    if obj.is_null() {
        return;
    }

    let response: String = env.get_string(response).expect("Couldn't get java string!").into();
    let data: ActionResponse = serde_json::from_str(&response).unwrap();
    (*obj).stream.push(data).unwrap()
}

#[no_mangle]
/// # Safety
pub unsafe extern "system" fn Java_Uplink_recv(
    env: JNIEnv,
    _class: JClass,
    obj: *mut UplinkConnector,
) -> jstring {
    if obj.is_null() {
        return env.new_string("").unwrap().into_inner();
    }

    let action = (*obj).bridge_rx.recv().unwrap();
    let action = serde_json::to_string(&action).unwrap();
    env.new_string(action).unwrap().into_inner()
}
