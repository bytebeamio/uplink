use std::thread;

use crossbeam_channel as channel;

mod collector;
mod serializer;


fn main() {
    pretty_env_logger::init();
    
    let (collector_tx, collector_rx) = channel::bounded(10);
    let mut collector = collector::simulator::Simulator::new(collector_tx).unwrap();
    let mut serializer = serializer::Serializer::new(collector_rx);

    thread::spawn(move || {
        serializer.start();
    });

    collector.start();
}
