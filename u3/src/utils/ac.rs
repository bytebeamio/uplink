use std::sync::Arc;
use std::sync::atomic::AtomicPtr;

pub struct AC<T> {
    inner: AtomicPtr<Arc<T>>
}

impl<T> AC<T> {
    pub fn new(inner: T) -> Self {
        let arc = Box::new(Arc::new(inner));
        let ptr = arc.as_ref() as *const Arc<T> as *mut Arc<T>;
        let inner = AtomicPtr::new(ptr);
        std::mem::forget(arc);
        Self { inner }
    }

    pub fn swap(&mut self, new: T) {
        let arc = Arc::new(new);
        let ptr = self.inner.as_ptr() as *const Arc<T> as *mut Arc<T>;
        std::mem::replace(unsafe { &mut *ptr }, arc);
        todo!();
    }

}

// impl Drop for AC {
//     fn drop(&mut self) {
//     }
// }