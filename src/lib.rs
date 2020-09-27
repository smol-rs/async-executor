//! Async executors.
//!
//! # Examples
//!
//! ```
//! use async_executor::Executor;
//! use futures_lite::future;
//!
//! // Create a new executor.
//! let ex = Executor::new();
//!
//! // Spawn a task.
//! let task = ex.spawn(async {
//!     println!("Hello world");
//! });
//!
//! // Run the executor until the task complets.
//! future::block_on(ex.run(task));
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};

use async_task::Runnable;
use futures_lite::{future, pin, FutureExt};
use parking_lot::{Mutex, MutexGuard};
use vec_arena::Arena;

#[doc(no_inline)]
pub use async_task::Task;

/// An async executor.
///
/// # Examples
///
/// A multi-threaded executor:
///
/// ```
/// use async_channel::unbounded;
/// use async_executor::Executor;
/// use easy_parallel::Parallel;
/// use futures_lite::future;
///
/// let ex = Executor::new();
/// let (signal, shutdown) = unbounded::<()>();
///
/// Parallel::new()
///     // Run four executor threads.
///     .each(0..4, |_| future::block_on(ex.run(shutdown.recv())))
///     // Run the main future on the current thread.
///     .finish(|| future::block_on(async {
///         println!("Hello world!");
///         drop(signal);
///     }));
/// ```
#[derive(Debug)]
pub struct Executor<'a> {
    /// The inner scheduler.
    sched: once_cell::sync::OnceCell<Arc<Scheduler>>,

    /// Makes the `'a` lifetime invariant.
    _marker: PhantomData<std::cell::UnsafeCell<&'a ()>>,
}

unsafe impl Send for Executor<'_> {}
unsafe impl Sync for Executor<'_> {}

impl UnwindSafe for Executor<'_> {}
impl RefUnwindSafe for Executor<'_> {}

impl<'a> Executor<'a> {
    /// Creates a new executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new();
    /// ```
    pub const fn new() -> Executor<'a> {
        Executor {
            sched: once_cell::sync::OnceCell::new(),
            _marker: PhantomData,
        }
    }

    /// Spawns a task onto the executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// ```
    pub fn spawn<T: Send + 'a>(&self, future: impl Future<Output = T> + Send + 'a) -> Task<T> {
        unsafe { self.sched().spawn_unchecked(future) }
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
    /// let ex = Executor::new();
    /// assert!(!ex.try_tick()); // no tasks to run
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// assert!(ex.try_tick()); // a task was found
    /// ```
    pub fn try_tick(&self) -> bool {
        Ticker::new(self.sched()).try_tick()
    }

    /// Run a single task.
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
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// future::block_on(ex.tick()); // runs the task
    /// ```
    pub async fn tick(&self) {
        Ticker::new(self.sched()).tick().await;
    }

    /// Runs the executor until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    /// use futures_lite::future;
    ///
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async { 1 + 2 });
    /// let res = future::block_on(ex.run(async { task.await * 2 }));
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        // A future that runs tasks forever.
        let run_forever = async {
            let mut ticker = Ticker::new(self.sched());
            loop {
                for _ in 0..200 {
                    ticker.tick().await;
                }
                future::yield_now().await;
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future.or(run_forever).await
    }

    /// Returns a reference to the inner scheduler.
    fn sched(&self) -> &Arc<Scheduler> {
        self.sched.get_or_init(|| Default::default())
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        if let Some(sched) = self.sched.get() {
            sched.destroy();
        }
    }
}

impl<'a> Default for Executor<'a> {
    fn default() -> Executor<'a> {
        Executor::new()
    }
}

/// A thread-local executor.
///
/// The executor can only be run on the thread that created it.
///
/// # Examples
///
/// ```
/// use async_executor::LocalExecutor;
/// use futures_lite::future;
///
/// let local_ex = LocalExecutor::new();
///
/// future::block_on(local_ex.run(async {
///     println!("Hello world!");
/// }));
/// ```
#[derive(Debug)]
pub struct LocalExecutor<'a> {
    /// The inner executor.
    inner: once_cell::unsync::OnceCell<Executor<'a>>,

    /// Makes the type `!Send` and `!Sync`.
    _marker: PhantomData<Rc<()>>,
}

impl UnwindSafe for LocalExecutor<'_> {}
impl RefUnwindSafe for LocalExecutor<'_> {}

impl<'a> LocalExecutor<'a> {
    /// Creates a single-threaded executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::new();
    /// ```
    pub const fn new() -> LocalExecutor<'a> {
        LocalExecutor {
            inner: once_cell::unsync::OnceCell::new(),
            _marker: PhantomData,
        }
    }

    /// Spawns a task onto the executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::new();
    ///
    /// let task = local_ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// ```
    pub fn spawn<T: 'a>(&self, future: impl Future<Output = T> + 'a) -> Task<T> {
        unsafe { self.inner().sched().spawn_unchecked(future) }
    }

    /// Attempts to run a task if at least one is scheduled.
    ///
    /// Running a scheduled task means simply polling its future once.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    ///
    /// let ex = LocalExecutor::new();
    /// assert!(!ex.try_tick()); // no tasks to run
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// assert!(ex.try_tick()); // a task was found
    /// ```
    pub fn try_tick(&self) -> bool {
        self.inner().try_tick()
    }

    /// Run a single task.
    ///
    /// Running a task means simply polling its future once.
    ///
    /// If no tasks are scheduled when this method is called, it will wait until one is scheduled.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    /// use futures_lite::future;
    ///
    /// let ex = LocalExecutor::new();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// future::block_on(ex.tick()); // runs the task
    /// ```
    pub async fn tick(&self) {
        self.inner().tick().await
    }

    /// Runs the executor until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    /// use futures_lite::future;
    ///
    /// let local_ex = LocalExecutor::new();
    ///
    /// let task = local_ex.spawn(async { 1 + 2 });
    /// let res = future::block_on(local_ex.run(async { task.await * 2 }));
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        self.inner().run(future).await
    }

    /// Returns a reference to the inner executor.
    fn inner(&self) -> &Executor<'a> {
        self.inner.get_or_init(|| Executor::new())
    }
}

impl<'a> Default for LocalExecutor<'a> {
    fn default() -> LocalExecutor<'a> {
        LocalExecutor::new()
    }
}

/// Scheduler shared by the executor and wakers.
#[derive(Debug, Default)]
struct Scheduler {
    state: Mutex<State>,
}

/// The state of a scheduler.
#[derive(Debug, Default)]
struct State {
    /// The global queue.
    queue: VecDeque<Runnable>,

    /// Currently active tasks.
    active: Arena<Waker>,

    /// Number of sleeping tickers (both notified and unnotified).
    sleeping: usize,

    /// IDs and wakers of sleeping unnotified tickers.
    ///
    /// A sleeping ticker is notified when its waker is missing from this list.
    wakers: Vec<(u64, Waker)>,

    /// ID generator.
    id_gen: u64,
}

impl Scheduler {
    /// Spawns a task onto the scheduler.
    unsafe fn spawn_unchecked<T>(
        self: &Arc<Scheduler>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        // Remove the task from the set of active tasks when the future finishes.
        let future = {
            let sched = self.clone();
            async move {
                pin!(future);
                let mut index = None;
                future::poll_fn(|cx| {
                    let poll = future.as_mut().poll(cx);
                    if poll.is_pending() {
                        if index.is_none() {
                            index = Some(sched.state.lock().active.insert(cx.waker().clone()));
                        }
                    } else {
                        if let Some(index) = index {
                            sched.state.lock().active.remove(index);
                        }
                    }
                    poll
                })
                .await
            }
        };

        // Create a function that schedules runnable tasks.
        let sched = self.clone();
        let schedule = move |runnable| {
            let mut state = sched.state.lock();
            state.queue.push_back(runnable);
            notify(state);
        };

        // Create the task and register it in the set of active tasks.
        let (runnable, task) = async_task::spawn_unchecked(future, schedule);
        runnable.schedule();
        task
    }

    /// Drops all unfinished tasks.
    fn destroy(self: &Arc<Scheduler>) {
        // Collect wakers of unfinished tasks.
        let mut wakers = Vec::new();
        {
            let mut state = self.state.lock();
            for i in 0..state.active.capacity() {
                if let Some(w) = state.active.remove(i) {
                    wakers.push(w);
                }
            }
        }

        // Wake unfinished tasks.
        for w in wakers {
            w.wake();
        }

        // Drop unfinished tasks.
        mem::take(&mut self.state.lock().queue);
    }
}

/// Notifies a sleeping ticker.
#[inline]
fn notify(mut state: MutexGuard<'_, State>) {
    let waker = if state.wakers.len() == state.sleeping {
        state.wakers.pop().map(|item| item.1)
    } else {
        None
    };

    drop(state);

    if let Some(w) = waker {
        w.wake();
    }
}

/// Runs tasks one by one.
#[derive(Debug)]
struct Ticker<'a> {
    /// The scheduler.
    sched: &'a Scheduler,

    /// Set to a non-zero sleeper ID when in sleeping state.
    ///
    /// States a ticker can be in:
    /// 1) Woken.
    /// 2a) Sleeping and unnotified.
    /// 2b) Sleeping and notified.
    sleeping: Option<u64>,
}

impl Ticker<'_> {
    /// Creates a ticker.
    fn new(sched: &Scheduler) -> Ticker<'_> {
        Ticker {
            sched,
            sleeping: None,
        }
    }

    /// Attempts to run a task if at least one is scheduled.
    fn try_tick(&self) -> bool {
        let mut state = self.sched.state.lock();

        match state.queue.pop_front() {
            None => false,
            Some(runnable) => {
                // Notify another ticker now to pick up where this ticker left off, just in case
                // running the task takes a long time.
                notify(state);

                // Run the task.
                runnable.run();
                true
            }
        }
    }

    /// Waits for the next runnable task to run.
    async fn tick(&mut self) {
        let runnable = future::poll_fn(|cx| {
            let mut state = self.sched.state.lock();

            match state.queue.pop_front() {
                None => {
                    // Move to sleeping and unnotified state.
                    match self.sleeping {
                        None => {
                            let id = state.id_gen;
                            state.id_gen += 1;

                            self.sleeping = Some(id);
                            state.sleeping += 1;

                            if state.sleeping > state.wakers.len() {
                                state.wakers.push((id, cx.waker().clone()));
                            } else {
                                cx.waker().wake_by_ref();
                            }
                        }
                        Some(id) => {
                            for item in &mut state.wakers {
                                if item.0 == id {
                                    if !item.1.will_wake(cx.waker()) {
                                        item.1 = cx.waker().clone();
                                    }
                                    return Poll::Pending;
                                }
                            }

                            state.wakers.push((id, cx.waker().clone()));
                        }
                    }

                    Poll::Pending
                }
                Some(r) => {
                    // Wake up.
                    if let Some(id) = self.sleeping.take() {
                        state.sleeping -= 1;

                        for i in (0..state.wakers.len()).rev() {
                            if state.wakers[i].0 == id {
                                state.wakers.remove(i);
                                break;
                            }
                        }
                    }

                    // Notify another ticker now to pick up where this ticker left off, just in case
                    // running the task takes a long time.
                    notify(state);

                    Poll::Ready(r)
                }
            }
        })
        .await;

        // Run the task.
        runnable.run();
    }
}

impl Drop for Ticker<'_> {
    fn drop(&mut self) {
        // If this ticker is in sleeping state, it must be removed from the sleepers list.
        if let Some(id) = self.sleeping {
            let mut state = self.sched.state.lock();
            state.sleeping -= 1;

            for i in (0..state.wakers.len()).rev() {
                if state.wakers[i].0 == id {
                    state.wakers.remove(i);
                    return;
                }
            }

            // If this ticker was notified, then notify another ticker.
            notify(state);
        }
    }
}
