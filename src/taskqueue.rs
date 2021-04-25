use std::sync::Arc;

use async_task::Runnable;

use concurrent_queue::ConcurrentQueue;
use parking_lot::Mutex;
use rtrb::{Consumer, Producer, PushError, RingBuffer};
use try_mutex::TryMutex;

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
        self.inner.push(task).unwrap();
    }

    pub fn pop(&self) -> Option<Runnable> {
        self.inner.pop().ok()
    }
}

#[derive(Debug, Clone)]
pub struct LocalQueueHandle {
    inner: Arc<TryMutex<Consumer<Runnable>>>,
}

#[derive(Debug)]
pub struct LocalQueue {
    pusher: Producer<Runnable>,
    popper: Arc<TryMutex<Consumer<Runnable>>>,
    next_task: Option<Runnable>,
}

unsafe impl Send for LocalQueue {}

impl Default for LocalQueue {
    fn default() -> Self {
        let (pusher, popper) = RingBuffer::new(256).split();
        Self {
            pusher,
            popper: Arc::new(TryMutex::new(popper)),
            next_task: Default::default(),
        }
    }
}

impl LocalQueue {
    pub fn push(&mut self, is_yield: bool, task: Runnable) -> Result<(), Runnable> {
        // if this is the same task as last time, we don't push to next_task
        if is_yield {
            self.pusher.push(task).map_err(|e| match e {
                PushError::Full(e) => e,
            })?;
        } else if let Some(task) = self.next_task.replace(task) {
            self.pusher.push(task).map_err(|e| match e {
                PushError::Full(e) => e,
            })?;
        }
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Runnable> {
        if let Some(next_task) = self.next_task.take() {
            Some(next_task)
        } else {
            self.popper.try_lock()?.pop().ok()
        }
    }

    pub fn steal_global(&mut self, other: &GlobalQueue) {
        if !self.pusher.is_full() {
            let steal_cnt = other.inner.len() / 2 + 1;
            let remaining_cap = self.pusher.slots();
            for _ in 0..steal_cnt.min(remaining_cap) {
                if let Some(popped) = other.pop() {
                    self.pusher.push(popped).unwrap();
                } else {
                    return;
                }
            }
        }
    }

    pub fn steal_local(&mut self, other: &LocalQueueHandle) {
        if !self.pusher.is_full() {
            if let Some(mut other) = other.inner.try_lock() {
                let steal_cnt = other.slots() / 2 + 1;
                let remaining_cap = self.pusher.slots();
                for _ in 0..steal_cnt.min(remaining_cap) {
                    if let Ok(popped) = other.pop() {
                        self.pusher.push(popped).unwrap();
                    } else {
                        return;
                    }
                }
            }
        }
    }

    pub fn handle(&self) -> LocalQueueHandle {
        LocalQueueHandle {
            inner: self.popper.clone(),
        }
    }
}
