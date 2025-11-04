use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub struct AC<T> {
    inner: RwLock<Arc<T>>
}

impl<T> AC<T> {
    pub fn new(inner: T) -> Self {
        let inner = RwLock::new(Arc::new(inner));
        Self { inner }
    }

    pub fn get(&self) -> Arc<T> {
        let lock = self.inner.read().unwrap();
        lock.clone()
    }

    pub fn put(&self, new: T) {
        *self.inner.write().unwrap() = Arc::new(new);
    }
}
