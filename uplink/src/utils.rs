use std::thread::JoinHandle;
use flume::{SendError, Sender};

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
