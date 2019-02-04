#[macro_use]
extern crate maplit;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate crossbeam_channel;

use std::thread;
use rumqtt::MqttOptions;

mod collector;
mod publisher;
mod error;

fn main() {
    let (tx, rx) = crossbeam_channel::bounded::<Vec<String>>(30);
    let limits = hashmap! {
        "can_raw" => 10
    };
    let c = collector::Udp::new(tx).set_channel_limits(limits);
    let mqttoptions = MqttOptions::new("uplink", "localhost", 1883);
    let mut p = publisher::Publisher::new(rx, mqttoptions).unwrap();
    
    thread::spawn(move || {
        c.start().unwrap();
    });

    p.start().unwrap();
}
