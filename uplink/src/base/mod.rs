use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::path::PathBuf;

use serde::Deserialize;
use derive_more::From;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::SendError;

pub mod actions;
pub mod serializer;
pub mod mqtt;

#[derive(Debug, From)]
pub enum Error {
    Send(SendError<Box<dyn Package>>)
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelConfig {
    pub topic: String,
    pub buf_size: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub channels: HashMap<String, ChannelConfig>,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
}

pub trait Package: Send + Debug {
    fn channel(&self) -> String;
    fn serialize(&self) -> Vec<u8>;
}

/// Signal to modify the behaviour of collector
#[derive(Debug)]
pub enum Control {
    Shutdown,
    StopChannel(String),
    StartChannel(String),
}

/// Buffer is an abstraction of a collection that serializer receives.
/// It also contains meta data to understand the type of data
/// e.g channel to mqtt topic mapping
/// Buffer doesn't put any restriction on type of `T`
#[derive(Debug)]
pub struct Buffer<T> {
    pub channel: String,
    pub buffer: Vec<T>,
    max_buffer_size: usize,
}

impl<T> Buffer<T> {
    pub fn new<S: Into<String>>(channel: S, max_buffer_size: usize) -> Buffer<T> {
        Buffer { channel: channel.into(), buffer: Vec::new(), max_buffer_size }
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

/// Partitions has handles to fill data segregated by channel, send
/// filled data to the serializer when a channel is full and handle to
/// receive controller notifications like shutdown, ignore a channel or
/// throttle collection
pub struct Partitions<T> {
    collection: HashMap<String, Buffer<T>>,
    tx: Sender<Box<dyn Package>>,
}

impl<T> Partitions<T>
where
    T: Debug + Send + 'static,
    Buffer<T>: Package,
{
    /// Create a new collection of buffers mapped to a (configured) channel
    pub fn new<S: Into<String>>(tx: Sender<Box<dyn Package>>, channels: Vec<(S, usize)>) -> Self {
        let mut partitions = Partitions { collection: HashMap::new(), tx };

        for channel in channels.into_iter() {
            let buffer = Buffer::new(channel.0, channel.1);
            partitions.collection.insert(buffer.channel.to_owned(), buffer);
        }
        partitions
    }

    pub async fn fill(&mut self, channel: &str, data: T) -> Result<(), Error> {
        let o = if let Some(buffer) = self.collection.get_mut(channel) {
            buffer.fill(data)
        } else {
            error!("Invalid channel = {:?}", channel);
            None
        };

        if let Some(buffer) = o {
            let buffer = Box::new(buffer);
            self.tx.send(buffer).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {

use serde::Serialize;
use super::Buffer;

    #[derive(Clone, Debug, Serialize)]
    pub struct Dummy {
        a: i32,
        b: String,
        c: Vec<u8>,
    }

    #[test]
    fn return_filled_buffer_after_it_is_full() {
        let mut buffer = Buffer::new("dummy", 10);
        let dummy = Dummy { a: 10, b: "hello".to_owned(), c: vec![1, 2, 3] };

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
}

