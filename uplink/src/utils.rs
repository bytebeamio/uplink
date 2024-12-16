use std::collections::btree_map::IterMut;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::thread::JoinHandle;
use flume::{SendError, Sender};

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

/// An iterator that allows user to access the current element
/// under the cursor of a BTreeMap
pub struct BTreeCursorMut<'a, K, V> {
    iter: IterMut<'a, K, V>,
    pub current: Option<(&'a K, &'a mut V)>,
}

impl<'a, K: Ord, V> BTreeCursorMut<'a, K, V> {
    pub fn new(map: &'a mut BTreeMap<K, V>) -> Self {
        let mut iter = map.iter_mut();
        let current = iter.next();
        BTreeCursorMut { iter, current }
    }

    pub fn bump(&mut self) {
        self.current = self.iter.next();
    }
}

pub struct SendOnce<T>(pub Sender<T>);

impl<T> SendOnce<T> {
    pub async fn send_async(self, value: T) -> Result<(), SendError<T>> {
        self.0.send_async(value).await
    }
}

pub struct AsyncTaskContext {
    thread: JoinHandle<()>,
    ctrl_tx: Sender<()>,
}

impl AsyncTaskContext {
    pub fn join(self) {
        let _ = self.ctrl_tx.send(());
        let _ = self.thread.join();
    }
}

/// Spawn a task in a new thread
/// The task will be given a Receiver<()>
/// The task must perform cleanup and shutdown when data is sent on that receiver
/// Calling the closure returned by this function will send shutdown notification to the task and
/// block until the task is finished
pub fn spawn_task_with_type(task: fn(flume::Receiver<()>)) -> AsyncTaskContext {
    let (ctrl_tx, ctrl_rx) = flume::bounded::<()>(1);
    let thread = std::thread::spawn(move || task(ctrl_rx));
    AsyncTaskContext { thread, ctrl_tx }
}

#[macro_export]
macro_rules! hashmap {
    ($( $key:expr => $value:expr ),* $(,)?) => {{
        let mut map = std::collections::HashMap::new();
        $(
            map.insert($key, $value);
        )*
        map
    }};
}
