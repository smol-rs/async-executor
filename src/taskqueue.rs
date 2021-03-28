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
    next_task: UnsafeCell<Option<Runnable>>,
}

unsafe impl Send for LocalQueue {}

impl Default for LocalQueue {
    fn default() -> Self {
        Self {
            inner: Worker::new_fifo(),
            next_task: Default::default(),
        }
    }
}

impl LocalQueue {
    pub fn push(&mut self, is_yield: bool, task: Runnable) -> Result<(), Runnable> {
        // if this is the same task as last time, we don't push to next_task
        if is_yield {
            self.inner.push(task);
        } else {
            let next_task = self.next_task();
            if let Some(task) = next_task.replace(task) {
                self.inner.push(task);
            }
        }
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

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[allow(clippy::clippy::mut_from_ref)]
    fn next_task(&self) -> &mut Option<Runnable> {
        unsafe { &mut *self.next_task.get() }
    }

    pub fn steal_global(&self, other: &GlobalQueue) {
        std::iter::repeat_with(|| other.inner.steal_batch(&self.inner)).find(|v| !v.is_retry());
    }

    pub fn steal_local(&self, other: &LocalQueueHandle) {
        let _ = other.inner.steal_batch(&self.inner);
    }

    pub fn handle(&self) -> LocalQueueHandle {
        LocalQueueHandle {
            inner: self.inner.stealer(),
        }
    }
}
