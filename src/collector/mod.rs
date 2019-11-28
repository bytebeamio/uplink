pub mod simulator;
pub mod can;

use std::mem;
use std::fmt::Debug;
use std::collections::HashMap;

use serde::Serialize;
use crossbeam_channel::Sender;

type Channel = String;

/// Defines methods required for a type to be compatible with collector.
/// Collector uses this trait definition to pull data from respective data sources
/// E.g can bus, journal, grpc
pub trait Reader {
    type Item: Debug + Serialize;
    type Error: Debug;
    
    /// List of valid channels for this reader
    fn channels(&self) -> Vec<String>;

    // Option relaxes the rule that next implementors should always
    // return an item. This is very useful for filtering and
    // conditional rollup (see simulator) without using loops.
    fn next(&mut self) -> Result<Option<(Channel, Self::Item)>, Self::Error>;
}


pub trait Batch {
    fn channel(&self) -> String;
    fn serialize(&self) -> Vec<u8>;
}

/// Buffer is an abstraction of a collection that serializer receives.
/// It also contains meta data to understand the type of data
/// e.g channel to mqtt topic mapping
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

/// Collector uses its `Reader` to read data from source. This data
/// is `Serialize`ble data which is collected by the serializer.
pub struct Collector<T, R> {
    collection: HashMap<String, Buffer<T>>,
    tx: Sender<Buffer<T>>,
    reader: R
}

impl<T, R> Collector<T, R> where T: Serialize + Debug, R: Reader<Item = T> {
    /// Create a new collection of buffers mapped to a (configured) channel
    pub fn new(reader: R, tx: Sender<Buffer<T>>) -> Collector<T, R> {
        let mut collector = Collector {
            collection: HashMap::new(),
            tx,
            reader
        };

        for channel in collector.reader.channels().iter() {
            let buffer = Buffer::new(channel, 10);
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
            if let Some((channel, data)) = self.reader.next().unwrap() {
                self.fill(&channel, data);
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::{Buffer, Collector, Reader};
    use serde::Serialize;
    use crossbeam_channel::bounded;

    #[derive(Clone, Debug, Serialize)]
    pub struct Dummy {
        a: i32,
        b: String,
        c: Vec<u8>
    }

    struct Generator;
    impl Reader for Generator {
        type Item = Dummy;
        type Error = ();

        fn channels(&self) -> Vec<String> { vec!["can".to_owned()] }
        fn next(&mut self) -> Result<Option<(String, Self::Item)>, Self::Error> { 
            let channel = "can".to_owned();
            let data = Dummy {a: 10, b: "hello".to_owned(), c: vec![1, 2, 3]};
            let out = (channel, data);
            
            Ok(Some(out))
        }
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
        let (tx, rx) = bounded(1);
        let generator = Generator;

        let mut collector = Collector::new(generator, tx);

        for _ in 1..100 {
            let (_channel, data) = collector.reader.next().unwrap().unwrap();
            collector.fill("dummy", data);
            assert!(rx.try_recv().is_err())
        }
    }

    #[test]
    fn collector_places_the_buffer_in_the_channel_after_the_buffer_is_full() {
        let (tx, rx) = bounded(1);
        let generator = Generator;
        let mut collector = Collector::new(generator, tx);

        for i in 1..=100 {
            let (channel, data) = collector.reader.next().unwrap().unwrap();
            collector.fill(&channel, data);
            if i % 10 == 0 {
                assert_eq!(rx.try_recv().unwrap().buffer.len(), 10);
            } else {
                assert!(rx.try_recv().is_err())
            }
        }
    }
}