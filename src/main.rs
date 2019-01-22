#[macro_use]
extern crate maplit;
#[macro_use]
extern crate derive_more;

use crossbeam_channel;
use std::thread;

mod collector;

fn main() {
    let (tx, rx) = crossbeam_channel::bounded::<Vec<String>>(30);
    let limits = hashmap! {
        "can_raw" => 10
    };
    let c = collector::Udp::new(tx).set_channel_limits(limits);

    thread::spawn(move || {
        c.start();
    });

    for data in rx {
        println!("{:?}", data);
    }
}
