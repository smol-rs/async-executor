use crate::{debug_state, Executor, State};
use async_task::{Builder, Runnable, Task};
use slab::Slab;
use std::{
    fmt,
    future::Future,
    panic::{RefUnwindSafe, UnwindSafe},
};

impl Executor<'static> {
    /// Consumes the [`Executor`] and intentionally leaks it.
    ///
    /// Largely equivalent to calling `Box::leak(Box::new(executor))`, but the produced
    /// [`LeakedExecutor`]'s functions are optimized to require fewer synchronizing operations
    /// when spawning, running, and finishing tasks.
    ///
    /// # Example
    ///
    /// ```
    /// use async_executor::Executor;
    /// use futures_lite::future;
    ///
    /// let ex = Executor::new().leak();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    ///
    /// future::block_on(ex.run(task));
    /// ```
    pub fn leak(self) -> LeakedExecutor {
        let ptr = self.state_ptr();
        // SAFETY: So long as an Executor lives, it's state pointer will always be valid
        // when accessed through state_ptr. This executor will live for the full 'static
        // lifetime so this isn't an arbitrary lifetime extension.
        let state = unsafe { &*ptr };

        std::mem::forget(self);

        let mut active = state.active.lock().unwrap();
        if !active.is_empty() {
            // Reschedule all of the active tasks.
            for waker in active.drain() {
                waker.wake();
            }
            // Overwrite to ensure that the slab is deallocated.
            *active = Slab::new();
        }

        LeakedExecutor { state }
    }
}

/// A leaked async [`Executor`] created from [`Executor::leak`].
///
/// Largely equivalent to calling `Box::leak(Box::new(executor))`, but spawning, running, and
/// finishing tasks are optimized with the assumption that the executor will never be `Drop`'ed.
/// A leaked executor may require signficantly less overhead in both single-threaded and
/// mulitthreaded use cases.
///
/// As this type does not implement `Drop`, losing the handle to the executor or failing
/// to consistently drive the executor with [`tick`] or [`run`] will cause the all spawned
/// tasks to permanently leak. Any tasks at the time will not be cancelled.
///
/// Unlike [`Executor`], this type trivially implements both [`Clone`] and [`Copy`].
///
/// This type *cannot* be converted back to a `Executor`.
#[derive(Copy, Clone)]
pub struct LeakedExecutor {
    state: &'static State,
}

// SAFETY: Executor stores no thread local state that can be accessed via other thread.
unsafe impl Send for LeakedExecutor {}
// SAFETY: Executor internally synchronizes all of it's operations internally.
unsafe impl Sync for LeakedExecutor {}

impl UnwindSafe for LeakedExecutor {}
impl RefUnwindSafe for LeakedExecutor {}

impl fmt::Debug for LeakedExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_state(self.state, "LeakedExecutor", f)
    }
}

impl LeakedExecutor {
    /// Spawns a task onto the executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new().leak();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// ```
    pub fn spawn<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T> {
        let (runnable, task) = Builder::new()
            .propagate_panic(true)
            .spawn(|()| future, self.schedule());
        runnable.schedule();
        task
    }

    /// Spawns a non-`'static` task onto the executor.
    ///
    /// ## Safety
    ///
    /// The caller must ensure that the returned task terminates
    /// or is cancelled before the end of 'a.
    pub unsafe fn spawn_scoped<'a, T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'a,
    ) -> Task<T> {
        // SAFETY:
        //
        // - `future` is `Send`
        // - `future` is not `'static`, but the caller guarantees that the
        //    task, and thus its `Runnable` must not live longer than `'a`.
        // - `self.schedule()` is `Send`, `Sync` and `'static`, as checked below.
        //    Therefore we do not need to worry about what is done with the
        //    `Waker`.
        let (runnable, task) = unsafe {
            Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(|()| future, self.schedule())
        };
        runnable.schedule();
        task
    }

    /// Attempts to run a task if at least one is scheduled.
    ///
    /// Running a scheduled task means simply polling its future once.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new().leak();
    /// assert!(!ex.try_tick()); // no tasks to run
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// assert!(ex.try_tick()); // a task was found
    /// ```
    pub fn try_tick(&self) -> bool {
        self.state.try_tick()
    }

    /// Runs a single task.
    ///
    /// Running a task means simply polling its future once.
    ///
    /// If no tasks are scheduled when this method is called, it will wait until one is scheduled.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    /// use futures_lite::future;
    ///
    /// let ex = Executor::new().leak();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// future::block_on(ex.tick()); // runs the task
    /// ```
    pub async fn tick(&self) {
        self.state.tick().await;
    }

    /// Runs the executor until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    /// use futures_lite::future;
    ///
    /// let ex = Executor::new().leak();
    ///
    /// let task = ex.spawn(async { 1 + 2 });
    /// let res = future::block_on(ex.run(async { task.await * 2 }));
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        self.state.run(future).await
    }

    /// Returns a function that schedules a runnable task when it gets woken up.
    fn schedule(&self) -> impl Fn(Runnable) + Send + Sync + 'static {
        let state = self.state;
        // TODO: If possible, push into the current local queue and notify the ticker.
        move |runnable| {
            state.queue.push(runnable).unwrap();
            state.notify();
        }
    }
}
