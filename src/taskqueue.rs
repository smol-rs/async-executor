use std::cell::UnsafeCell;

use async_task::Runnable;

use crossbeam_deque::{Injector, Steal, Stealer, Worker};

#[derive(Debug)]
pub struct GlobalQueue {
    inner: Injector<Runnable>,
}

impl Default for GlobalQueue {
    fn default() -> Self {
        Self {
            inner: Injector::default(),
        }
    }
}

impl GlobalQueue {
    pub fn push(&self, task: Runnable) {
        self.inner.push(task)
    }

    pub fn pop(&self) -> Option<Runnable> {
        loop {
            match self.inner.steal() {
                Steal::Retry => continue,
                Steal::Empty => return None,
                Steal::Success(v) => return Some(v),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalQueueHandle {
    inner: Stealer<Runnable>,
}

#[derive(Debug)]
pub struct LocalQueue {
    inner: Worker<Runnable>,
    last_pushed: UnsafeCell<u64>,
    next_task: UnsafeCell<Option<Runnable>>,
}

unsafe impl Send for LocalQueue {}

impl Default for LocalQueue {
    fn default() -> Self {
        Self {
            inner: Worker::new_fifo(),
            last_pushed: Default::default(),
            next_task: Default::default(),
        }
    }
}

impl LocalQueue {
    pub fn push(&mut self, task_id: u64, task: Runnable) -> Result<(), Runnable> {
        let last_pushed = unsafe { &mut *self.last_pushed.get() };
        // if this is the same task as last time, we don't push to next_task
        if task_id == *last_pushed {
            self.inner.push(task);
        } else {
            let next_task = self.next_task();
            if let Some(task) = next_task.replace(task) {
                self.inner.push(task);
            }
        }
        *last_pushed = task_id;
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Runnable> {
        let next_task = self.next_task();
        if let Some(next_task) = next_task.take() {
            Some(next_task)
        } else {
            self.inner.pop()
        }
    }

    #[allow(clippy::clippy::mut_from_ref)]
    fn next_task(&self) -> &mut Option<Runnable> {
        unsafe { &mut *self.next_task.get() }
    }

    pub fn steal_global(&self, other: &GlobalQueue) {
        let _ = other.inner.steal_batch(&self.inner);
        // let mut count = (other.inner.len() + 1) / 2;

        // if count > 0 {
        //     // Steal tasks.
        //     for _ in 0..count {
        //         if let Some(t) = other.pop() {
        //             assert!(self.inner.push(t).is_ok());
        //         } else {
        //             break;
        //         }
        //     }
        // }
    }

    pub fn steal_local(&self, other: &LocalQueueHandle) {
        let _ = other.inner.steal_batch(&self.inner);
        // let mut count = (other.inner.len() + 1) / 2;

        // if count > 0 {
        //     // Don't steal more than fits into the queue.
        //     if let Some(cap) = self.inner.capacity() {
        //         count = count.min(cap - self.inner.len());
        //     }

        //     // Steal tasks.
        //     for _ in 0..count {
        //         if let Ok(t) = other.inner.pop() {
        //             assert!(self.inner.push(t).is_ok());
        //         } else {
        //             break;
        //         }
        //     }
        // }
    }

    pub fn handle(&self) -> LocalQueueHandle {
        LocalQueueHandle {
            inner: self.inner.stealer(),
        }
    }
}
