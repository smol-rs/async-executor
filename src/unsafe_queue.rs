use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicU32, Ordering},
};

use async_task::Runnable;

#[derive(Debug)]
pub struct UnsafeQueue {
    /// Concurrently updated by many threads.
    head: AtomicU32,

    /// Only updated by producer thread but read by many threads.
    tail: AtomicU32,

    /// Masks the head / tail position value to obtain the index in the buffer.
    mask: usize,

    /// Stores the tasks.
    buffer: UnsafeCell<Vec<MaybeUninit<Runnable>>>,
}

unsafe impl Send for UnsafeQueue {}
unsafe impl Sync for UnsafeQueue {}

impl Default for UnsafeQueue {
    fn default() -> Self {
        let mut vec = Vec::with_capacity(512);
        for _ in 0..512 {
            vec.push(MaybeUninit::uninit());
        }
        Self {
            head: Default::default(),
            tail: Default::default(),
            mask: 0,
            buffer: UnsafeCell::new(vec),
        }
    }
}

impl UnsafeQueue {
    pub fn pushable(&self) -> bool {
        let buffer = self.buffer.get();
        let buffer = unsafe { &mut *buffer };
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        tail.wrapping_sub(head) < buffer.len() as u32
    }

    pub unsafe fn push(&self, task: Runnable) -> Result<(), Runnable> {
        let head = self.head.load(Ordering::SeqCst);

        // safety: this is the **only** thread that updates this cell.
        let tail = self.tail.load(Ordering::SeqCst);
        let buffer = self.buffer.get();
        let buffer = &mut *buffer;

        if tail.wrapping_sub(head) < buffer.len() as u32 {
            // Map the position to a slot index.
            let idx = tail as usize & self.mask;

            // Don't drop the previous value in `buffer[idx]` because
            // it is uninitialized memory.
            buffer[idx].as_mut_ptr().write(task);

            // Make the task available
            self.tail.store(tail.wrapping_add(1), Ordering::Relaxed);

            return Ok(());
        }

        Err(task)
    }

    pub unsafe fn pop(&self) -> Option<Runnable> {
        loop {
            let head = self.head.load(Ordering::SeqCst);

            // safety: this is the **only** thread that updates this cell.
            let tail = self.tail.load(Ordering::SeqCst);

            if head == tail {
                // queue is empty
                return None;
            }

            // Map the head position to a slot index.
            let idx = head as usize & self.mask;

            let buffer = self.buffer.get();
            let buffer = &mut *buffer;

            let task = buffer[idx].as_ptr().read();

            // Attempt to claim the task read above.
            let actual = self
                .head
                .compare_and_swap(head, head.wrapping_add(1), Ordering::SeqCst);

            if actual == head {
                return Some(task);
            }
        }
    }
}
