use std::{mem::MaybeUninit, sync::atomic::AtomicU32};

use async_task::Runnable;

struct Queue {
    /// Concurrently updated by many threads.
    head: AtomicU32,

    /// Only updated by producer thread but read by many threads.
    tail: AtomicU32,

    /// Masks the head / tail position value to obtain the index in the buffer.
    mask: usize,

    /// Stores the tasks.
    buffer: Box<[MaybeUninit<Runnable>]>,
}
