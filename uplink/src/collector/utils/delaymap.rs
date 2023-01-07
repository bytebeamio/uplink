use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use flume::Sender;
use log::{error, info, warn};
use tokio_stream::StreamExt;
use tokio_util::time::{delay_queue::Key, DelayQueue};

use crate::base::bridge::{self, StreamStatus};
use crate::{Config, Package, Payload, Stream};

/// A map to store and retrieve delays from a DelayQueue.
pub struct DelayMap<T> {
    queue: DelayQueue<T>,
    map: HashMap<T, Key>,
}

impl<T: Eq + Hash + Clone + Display> DelayMap<T> {
    pub fn new() -> Self {
        Self { queue: DelayQueue::new(), map: HashMap::new() }
    }

    // Removes timeout if it exists, else returns false.
    pub fn remove(&mut self, item: &T) {
        match self.map.remove(item) {
            Some(key) => {
                self.queue.remove(&key);
            }
            None => warn!("Timeout couldn't be removed from DelayMap: {}", item),
        }
    }

    // Insert new timeout.
    pub fn insert(&mut self, item: &T, period: Duration) {
        let key = self.queue.insert(item.clone(), period);
        if self.map.insert(item.to_owned(), key).is_some() {
            warn!("Timeout might have already been in DelayMap: {}", item);
        }
    }

    // Remove a key from map if it has timedout.
    pub async fn next(&mut self) -> Option<T> {
        if let Some(item) = self.queue.next().await {
            self.map.remove(item.get_ref());
            return Some(item.into_inner());
        }

        None
    }

    // Check if queue is empty.
    pub fn has_pending(&self) -> bool {
        !self.queue.is_empty()
    }
}
