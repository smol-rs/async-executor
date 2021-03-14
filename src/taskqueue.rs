use std::{collections::VecDeque, sync::Mutex};

use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;

// #[derive(Debug, Default)]
// pub struct TaskQueue {
//     inner: Mutex<VecDeque<Runnable>>,
// }

// impl TaskQueue {
//     pub fn push(&self, task: Runnable) {
//         self.inner.lock().unwrap().push_front(task)
//     }

//     pub fn pop(&self) -> Option<Runnable> {
//         self.inner.lock().unwrap().pop_front()
//     }
// }

#[derive(Debug)]
pub struct TaskQueue {
    inner: ConcurrentQueue<Runnable>,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self {
            inner: ConcurrentQueue::unbounded(),
        }
    }
}

impl TaskQueue {
    pub fn push(&self, task: Runnable) {
        self.inner.push(task).unwrap()
    }

    pub fn pop(&self) -> Option<Runnable> {
        self.inner.pop().ok()
    }

    pub fn steal_from(&self, other: &Self) {
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
