use std::hash::Hash;
use std::{collections::HashMap, time::Duration};

use tokio_stream::StreamExt;
use tokio_util::time::{delay_queue::Key, DelayQueue};

/// A map to store and retrieve delays from a DelayQueue.
pub struct DelayMap<T> {
    queue: DelayQueue<T>,
    map: HashMap<T, Key>,
}

impl<T: Eq + Hash + Clone> DelayMap<T> {
    pub fn new() -> Self {
        Self { queue: DelayQueue::new(), map: HashMap::new() }
    }

    // Removes timeout if it exists, else returns false.
    pub fn remove(&mut self, item: &T) -> bool {
        let key = match self.map.remove(item) {
            Some(k) => k,
            None => return false,
        };
        self.queue.remove(&key);

        true
    }

    // Resets timeout if it exists, else returns false.
    pub fn reset(&mut self, item: &T, period: Duration) -> bool {
        let key = match self.map.get(item) {
            Some(k) => k,
            None => return false,
        };
        self.queue.reset(key, period);

        true
    }

    // Insert new timeout.
    pub fn insert(&mut self, item: T, period: Duration) {
        let key = self.queue.insert(item.clone(), period);
        self.map.insert(item, key);
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
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
