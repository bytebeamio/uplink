use std::collections::VecDeque;
use std::fmt::Debug;

/// Map with a maximum size
///
/// If too many items are inserted, the oldest entry will be deleted
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LimitedArrayMap<K, V> {
    pub(crate) map: VecDeque<(K, V)>,
}

impl<K: Eq + Clone + Debug, V> LimitedArrayMap<K, V> {
    pub fn new(max_size: usize) -> Self {
        Self {
            map: VecDeque::with_capacity(max_size),
        }
    }

    pub fn set(&mut self, key: K, value: V) -> Option<(K, V)> {
        let mut result = None;
        match self.map.iter_mut().rev().find(|(k, _)| k == &key) {
            Some((k, v)) => {
                result = Some((k.clone(), std::mem::replace(v, value)));
            }
            None => {
                if self.map.len() == self.map.capacity() {
                    if let Some(oldest_entry) = self.map.pop_front() {
                        result = Some(oldest_entry);
                    }
                }
                self.map.push_back((key, value));
            }
        }
        result
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.iter().rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }
}

#[test]
fn t1() {
    let mut m = LimitedArrayMap::new(64);
    dbg!(m.set("a".to_owned(), "A".to_owned()));
    dbg!(m.set("b".to_owned(), "B".to_owned()));
    dbg!(m);
}

#[test]
fn t2() {
    let mut m = LimitedArrayMap::new(64);
    dbg!(m.set("a".to_owned(), "A".to_owned()));
    dbg!(m.set("b".to_owned(), "B".to_owned()));
    dbg!(m.get(&"a".to_owned()));
}