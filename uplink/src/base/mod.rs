use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::path::PathBuf;

use async_channel::{SendError, Sender};
use serde::Deserialize;

pub mod actions;
pub mod mqtt;
pub mod serializer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub topic: String,
    pub buf_size: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub bridge_port: u16,
    pub actions: Vec<String>,
    pub streams: HashMap<String, StreamConfig>,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
}

pub trait Package: Send + Debug {
    fn stream(&self) -> String;
    fn serialize(&self) -> Vec<u8>;
}

/// Signal to modify the behaviour of collector
#[derive(Debug)]
pub enum Control {
    Shutdown,
    StopStream(String),
    StartStream(String),
}

/// Buffer is an abstraction of a collection that serializer receives.
/// It also contains meta data to understand the type of data
/// e.g stream to mqtt topic mapping
/// Buffer doesn't put any restriction on type of `T`
#[derive(Debug)]
pub struct Buffer<T> {
    pub stream: String,
    pub buffer: Vec<T>,
    max_buffer_size: usize,
}

impl<T> Buffer<T> {
    pub fn new<S: Into<String>>(stream: S, max_buffer_size: usize) -> Buffer<T> {
        Buffer { stream: stream.into(), buffer: Vec::new(), max_buffer_size }
    }

    pub fn fill(&mut self, data: T) -> Option<Buffer<T>> {
        self.buffer.push(data);

        if self.buffer.len() >= self.max_buffer_size {
            let buffer = mem::replace(&mut self.buffer, Vec::new());
            let stream = self.stream.clone();
            let max_buffer_size = self.max_buffer_size;
            let buffer = Buffer { stream, buffer, max_buffer_size };

            return Some(buffer);
        }

        None
    }
}

pub struct Bucket<T> {
    buffer: Buffer<T>,
    tx: Sender<Box<dyn Package>>,
}

impl<T> Bucket<T>
where
    T: Debug + Send + 'static,
    Buffer<T>: Package,
{
    pub fn new(tx: Sender<Box<dyn Package>>, stream: &str, max: usize) -> Self {
        let buffer = Buffer::new(stream.to_owned(), max);
        Bucket { buffer, tx }
    }

    pub async fn fill(&mut self, data: T) -> Result<(), Error> {
        if let Some(buffer) = self.buffer.fill(data) {
            info!("Flushing {} buffer", buffer.stream);

            let buffer = Box::new(buffer);
            self.tx.send(buffer).await?;
        }

        Ok(())
    }
}

impl<T> Clone for Bucket<T> {
    fn clone(&self) -> Self {
        Bucket { buffer: Buffer::new(self.buffer.stream.clone(), self.buffer.max_buffer_size), tx: self.tx.clone() }
    }
}

/// Partitions has handles to fill data segregated by stream, send
/// filled data to the serializer when a stream is full
pub struct Partitions<T> {
    collection: HashMap<String, Buffer<T>>,
    tx: Sender<Box<dyn Package>>,
}

impl<T> Partitions<T>
where
    T: Debug + Send + 'static,
    Buffer<T>: Package,
{
    /// Create a new collection of buffers mapped to a (configured) stream
    pub fn new<S: Into<String>>(tx: Sender<Box<dyn Package>>, streams: Vec<(S, usize)>) -> Self {
        let mut partitions = Partitions { collection: HashMap::new(), tx };

        for stream in streams.into_iter() {
            let buffer = Buffer::new(stream.0, stream.1);
            partitions.collection.insert(buffer.stream.to_owned(), buffer);
        }

        partitions
    }

    pub async fn fill(&mut self, stream: &str, data: T) -> Result<(), Error> {
        let o = match self.collection.get_mut(stream) {
            Some(buffer) => {
                debug!("Filling {} buffer. Count = {}", stream, buffer.buffer.len());
                buffer.fill(data)
            }
            None => {
                error!("Invalid stream = {:?}", stream);
                None
            }
        };

        if let Some(buffer) = o {
            info!("Flushing {} buffer", stream);
            let buffer = Box::new(buffer);
            self.tx.send(buffer).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::Buffer;
    use serde::Serialize;

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
