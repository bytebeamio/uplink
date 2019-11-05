

mod collector;
mod serializer;


fn main() {
    let mut collector = collector::simulator::Simulator::new().unwrap();
    collector.start();
}
