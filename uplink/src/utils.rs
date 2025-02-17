use std::collections::btree_map::IterMut;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::thread::JoinHandle;
use flume::{SendError, Sender};

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

/// Wraps a channel sink and allows only one value to be sent over it
/// The send_async task/future takes the ownership of the sink
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
