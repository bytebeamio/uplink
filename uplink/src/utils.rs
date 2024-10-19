use std::collections::VecDeque;
use std::fmt::Debug;
use bytes::buf::Limit;

/// Map with a maximum size
///
/// If too many rows are inserted, the oldest entry will be deleted
#[derive(Debug)]
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
        match self.map.iter_mut().find(|(k, _)| k == &key) {
            Some((k, v)) => {
                result = Some((k.clone(), std::mem::replace(v, value)));
            }
            None => {
                if self.map.len() == self.map.capacity() {
                    if let Some(oldest_entry) = self.map.pop_front() {
                        result = Some(oldest_entry);
                    }
                }
                dbg!(self.map.len());
                self.map.push_back((key, value));
                dbg!(self.map.len());
            }
        }
        result
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }
}

pub fn lerp<T: Ord>(mn: T, val: T, mx: T) -> T {
    if val < mn {
        mn
    } else if val > mx {
        mx
    } else {
        val
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