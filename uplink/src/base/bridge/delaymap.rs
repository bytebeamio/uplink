use std::fmt::Display;
use std::hash::Hash;
use std::{collections::HashMap, time::Duration};

use log::warn;
use tokio_stream::StreamExt;
use tokio_util::time::{delay_queue::Key, DelayQueue};

/// A structure to manage delayed items, allowing items to be added with a timeout period,
/// removed when expired, or queried to check if any items remain pending.
#[derive(Debug, Default)]
pub struct DelayMap<T> {
    /// A delay queue for managing timed entries
    queue: DelayQueue<T>,
    /// A map to track item keys for efficient removal from the queue
    map: HashMap<T, Key>,
}

impl<T: Eq + Hash + Clone + Display> DelayMap<T> {
    /// Removes a timeout from the `DelayMap` for the specified `item`.
    /// Logs a warning if the item was not found in the map.
    pub fn remove(&mut self, item: &T) {
        if let Some(key) = self.map.remove(item) {
            self.queue.remove(&key);
        } else {
            warn!("Attempted to remove non-existent timeout from DelayMap: {item}");
        }
    }

    /// Inserts a new timeout for the specified `item` with a given `period`.
    /// If the item already exists in the map, logs a warning but replaces the existing timeout.
    pub fn insert(&mut self, item: &T, period: Duration) {
        let key = self.queue.insert(item.clone(), period);
        if self.map.insert(item.clone(), key).is_some() {
            warn!("Timeout for {item} was already present in DelayMap and has been replaced.");
        }
    }

    /// Waits for the next item to time out, removes it from the map, and returns it.
    /// Returns `None` if the queue is empty or no items have timed out.
    pub async fn next(&mut self) -> Option<T> {
        match self.queue.next().await {
            Some(item) => {
                self.map.remove(item.get_ref());
                Some(item.into_inner())
            }
            None => None,
        }
    }

    /// Returns `true` if there are any pending timeouts in the queue.
    pub fn has_pending(&self) -> bool {
        !self.queue.is_empty()
    }
}
