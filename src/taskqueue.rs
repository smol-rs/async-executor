use std::{collections::VecDeque, sync::Mutex};

use async_task::Runnable;

#[derive(Debug, Default)]
pub struct TaskQueue {
    inner: Mutex<VecDeque<Runnable>>,
}

impl TaskQueue {
    pub fn push(&self, task: Runnable) {
        self.inner.lock().unwrap().push_front(task)
    }

    pub fn pop(&self) -> Option<Runnable> {
        self.inner.lock().unwrap().pop_front()
    }
}
