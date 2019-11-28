pub mod simulator;
pub mod can;

use std::mem;
use std::fmt::Debug;
use std::collections::HashMap;

use serde::Serialize;
use crossbeam_channel::Sender;


pub trait Reader {
    type Item: Debug + Serialize;
    type Error: Debug;

    // Option relaxes the rule that next implementors should always
    // return an item. This is very useful for filtering and
    // conditional rollup (see simulator) without using loops.
    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error>;
}


#[derive(Debug)]
pub struct Buffer<T> {
    pub channel: String,
    pub buffer: Vec<T>,
    pub max_buffer_size: usize,
}

impl<T: Serialize> Buffer<T> {
    pub fn new(channel: &str, max_buffer_size: usize) -> Buffer<T> {
        Buffer {
            channel: channel.to_owned(),
            buffer: Vec::new(),
            max_buffer_size
        }
    }

    pub fn fill(&mut self, data: T) -> Option<Buffer<T>> {
        self.buffer.push(data);

        if self.buffer.len() >= self.max_buffer_size {
            let buffer = mem::replace(&mut self.buffer, Vec::new());
            let channel = self.channel.clone();
            let max_buffer_size = self.max_buffer_size;
            let buffer = Buffer { channel, buffer, max_buffer_size };
            return Some(buffer);
        }

        None
    }
}

pub struct Collector<T, R> {
    collection: HashMap<String, Buffer<T>>,
    tx: Sender<Buffer<T>>,
    reader: R
}

// TODO: Implement a collector macro with takes channel name and size of multiple channels
// as var args

type ChannelName = String;
type ChannelSize = usize;

impl<T: Serialize, R: Reader> Collector<T, R> {
    /// Create a new collection of buffers mapped to a (configured) channel
    pub fn new(reader: R, tx: Sender<Buffer<T>>, channels: Vec<(ChannelName, ChannelSize)>) -> Collector<T, R> {
        let mut collector = Collector {
            collection: HashMap::new(),
            tx,
            reader
        };

        for channel in channels {
            let buffer = Buffer::new(&channel.0, channel.1);
            collector.collection.insert(buffer.channel.to_owned(), buffer);
        }

        collector
    }

    pub fn fill(&mut self, channel: &str, data: T) {
        let o = if let Some(buffer) = self.collection.get_mut(channel) {
            buffer.fill(data)
        } else {
            error!("Invalid channel = {:?}", channel);
            None
        };

        if let Some(buffer) = o {
            self.tx.send(buffer).unwrap();
        }
    }

    pub fn start(&mut self) {
        loop {
            if let Some(item) = self.reader.next().unwrap() {
                self.fill("dummy", item);

            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::{Buffer, Collector};
    use serde::Serialize;
    use crossbeam_channel::bounded;

    #[derive(Clone, Debug, Serialize)]
    pub struct Dummy {
        a: i32,
        b: String,
        c: Vec<u8>
    }

    #[test]
    fn return_filled_buffer_after_it_is_full() {
        let mut buffer = Buffer::new("dummy", 10);
        let dummy = Dummy {a: 10, b: "hello".to_owned(), c: vec![1, 2, 3]};

        for i in 1..100 {
            let o = buffer.fill(dummy.clone());

            if i % 10 == 0 {
                assert!(o.is_some());
                assert_eq!(o.unwrap().buffer.len(), 10);
            } else {
                assert!(o.is_none())
            }
        }
    }

    #[test]
    fn invalid_channels_are_not_filled_by_the_collector() {
        let dummy = Dummy {a: 10, b: "hello".to_owned(), c: vec![1, 2, 3]};
        let (tx, rx) = bounded(1);

        let mut collector = Collector::new(
            tx, 
            vec![("can".to_owned(), 10), ("gps".to_owned(), 10)]
        );

        for _ in 1..100 {
            collector.fill("dummy", dummy.clone());
            assert!(rx.try_recv().is_err())
        }
    }

    #[test]
    fn collector_places_the_buffer_in_the_channel_after_the_buffer_is_full() {
        let (tx, rx) = bounded(1);

        let mut collector = Collector::new(
            tx, 
            vec![("can".to_owned(), 10)]
        );

        let dummy = Dummy {a: 10, b: "hello".to_owned(), c: vec![1, 2, 3]};
        for i in 1..=100 {
            collector.fill("can", dummy.clone());

            if i % 10 == 0 {
                assert_eq!(rx.try_recv().unwrap().buffer.len(), 10);
            } else {
                assert!(rx.try_recv().is_err())
            }
        }
    }
}

