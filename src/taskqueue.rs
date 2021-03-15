use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;

#[derive(Debug)]
pub struct GlobalQueue {
    inner: ConcurrentQueue<Runnable>,
}

impl Default for GlobalQueue {
    fn default() -> Self {
        Self {
            inner: ConcurrentQueue::unbounded(),
        }
    }
}

impl GlobalQueue {
    pub fn push(&self, task: Runnable) {
        // eprintln!("pushing global queue length {}", self.inner.len());
        self.inner.push(task).unwrap()
    }

    pub fn pop(&self) -> Option<Runnable> {
        self.inner.pop().ok()
    }
}

#[derive(Debug)]
pub struct LocalQueue {
    inner: ConcurrentQueue<Runnable>,
}

impl Default for LocalQueue {
    fn default() -> Self {
        Self {
            inner: ConcurrentQueue::bounded(512),
        }
    }
}

impl LocalQueue {
    pub fn push(&self, task: Runnable) -> Result<(), Runnable> {
        // eprintln!("pushing local queue length {}", self.inner.len());
        self.inner.push(task).map_err(|err| err.into_inner())
    }

    pub fn pop(&self) -> Option<Runnable> {
        self.inner.pop().ok()
    }

    pub fn steal_global(&self, other: &GlobalQueue) {
        let mut count = (other.inner.len() + 1) / 2;

        if count > 0 {
            // Don't steal more than fits into the queue.
            if let Some(cap) = self.inner.capacity() {
                count = count.min(cap - self.inner.len());
            }

            // Steal tasks.
            for _ in 0..count {
                if let Some(t) = other.pop() {
                    assert!(self.inner.push(t).is_ok());
                } else {
                    break;
                }
            }
        }
    }

    pub fn steal_local(&self, other: &LocalQueue) {
        let mut count = (other.inner.len() + 1) / 2;

        if count > 0 {
            // Don't steal more than fits into the queue.
            if let Some(cap) = self.inner.capacity() {
                count = count.min(cap - self.inner.len());
            }

            // Steal tasks.
            for _ in 0..count {
                if let Some(t) = other.pop() {
                    assert!(self.inner.push(t).is_ok());
                } else {
                    break;
                }
            }
        }
    }
}
