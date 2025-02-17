use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use std::collections::HashSet;
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;

/// Like a `DelayQueue`, with an additional `contains` method
pub struct DelayMap<T> {
    timeouts: DelayQueue<T>,
    key_set: HashSet<T>,
}

impl<T: Eq + Hash + Clone + Display> DelayMap<T> {
    pub fn new() -> Self {
        Self { timeouts: DelayQueue::new(), key_set: HashSet::new() }
    }

    // Insert new timeout.
    pub fn insert(&mut self, item: T, period: Duration) {
        self.timeouts.insert(item.clone(), period);
        self.key_set.insert(item);
    }

    pub async fn next(&mut self) -> Option<T> {
        if let Some(item) = self.timeouts.next().await {
            self.key_set.remove(item.get_ref());
            return Some(item.into_inner());
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.timeouts.is_empty()
    }

    pub fn contains(&self, k: &T) -> bool {
        self.key_set.contains(k)
    }
}
