//! Async executors.
//!
//! This crate provides two reference executors that trade performance for
//! functionality. They should be considered reference executors that are "good
//! enough" for most use cases. For more specialized use cases, consider writing
//! your own executor on top of [`async-task`].
//!
//! [`async-task`]: https://crates.io/crates/async-task
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
//! // Run the executor until the task completes.
//! future::block_on(ex.run(task));
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]

use std::fmt;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::task::Waker;

use async_lock::OnceCell;
use async_task::{Builder, Runnable};
use concurrent_queue::ConcurrentQueue;
use event_listener::{listener, Event};
use futures_lite::{future, prelude::*};
use slab::Slab;
use thread_local::ThreadLocal;

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
pub struct Executor<'a> {
    /// The executor state.
    state: OnceCell<Arc<State>>,

    /// Makes the `'a` lifetime invariant.
    _marker: PhantomData<std::cell::UnsafeCell<&'a ()>>,
}

unsafe impl Send for Executor<'_> {}
unsafe impl Sync for Executor<'_> {}

impl UnwindSafe for Executor<'_> {}
impl RefUnwindSafe for Executor<'_> {}

impl fmt::Debug for Executor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_executor(self, "Executor", f)
    }
}

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
            state: OnceCell::new(),
            _marker: PhantomData,
        }
    }

    /// Returns `true` if there are no unfinished tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new();
    /// assert!(ex.is_empty());
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// assert!(!ex.is_empty());
    ///
    /// assert!(ex.try_tick());
    /// assert!(ex.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.state().active.lock().unwrap().is_empty()
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
        let mut active = self.state().active.lock().unwrap();

        // Remove the task from the set of active tasks when the future finishes.
        let entry = active.vacant_entry();
        let index = entry.key();
        let state = self.state().clone();
        let future = async move {
            let _guard = CallOnDrop(move || drop(state.active.lock().unwrap().try_remove(index)));
            future.await
        };

        // Create the task and register it in the set of active tasks.
        let (runnable, task) = unsafe {
            Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(|()| future, self.schedule())
        };
        entry.insert(runnable.waker());

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
    /// let ex = Executor::new();
    /// assert!(!ex.try_tick()); // no tasks to run
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// assert!(ex.try_tick()); // a task was found
    /// ```
    pub fn try_tick(&self) -> bool {
        match self.state().queue.pop() {
            Err(_) => false,
            Ok(runnable) => {
                // Notify another ticker now to pick up where this ticker left off, just in case
                // running the task takes a long time.
                self.state().notify();

                // Run the task.
                runnable.run();
                true
            }
        }
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
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// future::block_on(ex.tick()); // runs the task
    /// ```
    pub async fn tick(&self) {
        let state = self.state();
        let runnable = state
            .tick_with(|local, steal| {
                local
                    .queue
                    .pop()
                    .ok()
                    .or_else(|| if steal { state.queue.pop().ok() } else { None })
            })
            .await;
        runnable.run();
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
        let mut runner = Runner::new(self.state());
        let mut rng = fastrand::Rng::new();

        // A future that runs tasks forever.
        let run_forever = async {
            loop {
                for _ in 0..200 {
                    let runnable = runner.runnable(&mut rng).await;
                    runnable.run();
                }
                future::yield_now().await;
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future.or(run_forever).await
    }

    /// Returns a function that schedules a runnable task when it gets woken up.
    fn schedule(&self) -> impl Fn(Runnable) + Send + Sync + 'static {
        let state = self.state().clone();

        move |mut runnable| {
            // If possible, push into the current local queue and notify the ticker.
            if let Some(local) = state.local_queue.get() {
                // Don't push into the local queue if no one is ticking it.
                if local.tickers.load(Ordering::Acquire) > 0 {
                    runnable = if let Err(err) = local.queue.push(runnable) {
                        err.into_inner()
                    } else {
                        // Try to notify threads waiting on this queue. If there are
                        // none, notify another thread.
                        if local.waiters.notify_additional(1) == 0 {
                            state.new_tasks.notify_additional(1);
                        }
                        return;
                    }
                }
            }
            // If the local queue is full, fallback to pushing onto the global injector queue.
            state.queue.push(runnable).unwrap();
            state.new_tasks.notify_additional(1);
        }
    }

    /// Returns a reference to the inner state.
    fn state(&self) -> &Arc<State> {
        #[cfg(not(target_family = "wasm"))]
        {
            return self.state.get_or_init_blocking(|| Arc::new(State::new()));
        }

        // Some projects use this on WASM for some reason. In this case get_or_init_blocking
        // doesn't work. Just poll the future once and panic if there is contention.
        #[cfg(target_family = "wasm")]
        future::block_on(future::poll_once(
            self.state.get_or_init(|| async { Arc::new(State::new()) }),
        ))
        .expect("encountered contention on WASM")
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.state.get() {
            let mut active = state.active.lock().unwrap();
            for w in active.drain() {
                w.wake();
            }
            drop(active);

            for local_queue in state.local_queue.iter() {
                while local_queue.queue.pop().is_ok() {}
            }
            while state.queue.pop().is_ok() {}
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
pub struct LocalExecutor<'a> {
    /// The inner executor.
    inner: Executor<'a>,

    /// Makes the type `!Send` and `!Sync`.
    _marker: PhantomData<Rc<()>>,
}

impl UnwindSafe for LocalExecutor<'_> {}
impl RefUnwindSafe for LocalExecutor<'_> {}

impl fmt::Debug for LocalExecutor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_executor(&self.inner, "LocalExecutor", f)
    }
}

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
            inner: Executor::new(),
            _marker: PhantomData,
        }
    }

    /// Returns `true` if there are no unfinished tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::new();
    /// assert!(local_ex.is_empty());
    ///
    /// let task = local_ex.spawn(async {
    ///     println!("Hello world");
    /// });
    /// assert!(!local_ex.is_empty());
    ///
    /// assert!(local_ex.try_tick());
    /// assert!(local_ex.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner().is_empty()
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
        let mut active = self.inner().state().active.lock().unwrap();

        // Remove the task from the set of active tasks when the future finishes.
        let entry = active.vacant_entry();
        let index = entry.key();
        let state = self.inner().state().clone();
        let future = async move {
            let _guard = CallOnDrop(move || drop(state.active.lock().unwrap().try_remove(index)));
            future.await
        };

        // Create the task and register it in the set of active tasks.
        let (runnable, task) = unsafe {
            Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(|()| future, self.schedule())
        };
        entry.insert(runnable.waker());

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

    /// Runs a single task.
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

    /// Returns a function that schedules a runnable task when it gets woken up.
    fn schedule(&self) -> impl Fn(Runnable) + Send + Sync + 'static {
        let state = self.inner().state().clone();

        move |runnable| {
            state.queue.push(runnable).unwrap();
            state.notify();
        }
    }

    /// Returns a reference to the inner executor.
    fn inner(&self) -> &Executor<'a> {
        &self.inner
    }
}

impl<'a> Default for LocalExecutor<'a> {
    fn default() -> LocalExecutor<'a> {
        LocalExecutor::new()
    }
}

/// The state of a executor.
struct State {
    /// The global queue.
    queue: ConcurrentQueue<Runnable>,

    /// Local queues created by runners.
    ///
    /// If possible, tasks are scheduled onto the local queue, and will only defer
    /// to other global queue when they're full, or the task is being scheduled from
    /// a thread without a runner.
    ///
    /// Note: if a runner terminates and drains its local queue, any subsequent
    /// spawn calls from the same thread will be added to the same queue, but won't
    /// be executed until `Executor::run` is run on the thread again, or another
    /// thread steals the task.
    local_queue: ThreadLocal<LocalQueue>,

    /// Tickers waiting on new tasks from the global queue.
    new_tasks: Event,

    /// Currently active tasks.
    active: Mutex<Slab<Waker>>,
}

impl State {
    /// Creates state for a new executor.
    fn new() -> State {
        State {
            queue: ConcurrentQueue::unbounded(),
            local_queue: ThreadLocal::new(),
            new_tasks: Event::new(),
            active: Mutex::new(Slab::new()),
        }
    }

    /// Notifies a sleeping ticker.
    #[inline]
    fn notify(&self) {
        self.new_tasks.notify(1);
    }

    /// Run a tick using the provided function to get the next task.
    async fn tick_with(
        &self,
        mut local_ticker: impl FnMut(&LocalQueue, bool) -> Option<Runnable>,
    ) -> Runnable {
        let local = self.local_queue.get_or_default();

        loop {
            // Try to get a runnable from the local queue.
            if let Some(runnable) = local_ticker(local, false) {
                return runnable;
            }

            // Register a local waiter.
            listener!(local.waiters => local_listener);

            // Try for a global runner.
            if let Ok(runnable) = self.queue.pop() {
                return runnable;
            }

            // Register a global waiter.
            listener!(self.new_tasks => global_listener);

            // Try for both again.
            if let Some(runnable) = local_ticker(local, true) {
                return runnable;
            }

            // Wait on both listeners in parallel.
            local_listener.or(global_listener).await;
        }
    }
}

/// A worker in a work-stealing executor.
///
/// This is just a ticker that also has an associated local queue for improved cache locality.
struct Runner<'a> {
    /// The executor state.
    state: &'a State,

    /// Bumped every time a runnable task is found.
    ticks: usize,
}

impl Runner<'_> {
    /// Creates a runner and registers it in the executor state.
    fn new(state: &State) -> Runner<'_> {
        state.local_queue.get_or_default().start_ticking();

        Runner { state, ticks: 0 }
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&mut self, rng: &mut fastrand::Rng) -> Runnable {
        let local = self.state.local_queue.get_or_default();

        let runnable = self
            .state
            .tick_with(|_, try_stealing| {
                // Try the local queue.
                if let Ok(r) = local.queue.pop() {
                    return Some(r);
                }

                // Remaining work involves stealing.
                if !try_stealing {
                    return None;
                }

                // Try stealing from the global queue.
                if let Ok(r) = self.state.queue.pop() {
                    steal(&self.state.queue, &local.queue);
                    return Some(r);
                }

                // Try stealing from other runners.
                let local_queues = &self.state.local_queue;

                // Pick a random starting point in the iterator list and rotate the list.
                let n = local_queues.iter().count();
                let start = rng.usize(..n);
                let iter = local_queues
                    .iter()
                    .chain(local_queues.iter())
                    .skip(start)
                    .take(n);

                // Remove this runner's local queue.
                let iter = iter.filter(|other| !core::ptr::eq(*other, local));

                // Try stealing from each local queue in the list.
                for other in iter {
                    steal(&other.queue, &local.queue);
                    if let Ok(r) = local.queue.pop() {
                        return Some(r);
                    }
                }

                None
            })
            .await;

        // Bump the tick counter.
        self.ticks = self.ticks.wrapping_add(1);

        if self.ticks % 64 == 0 {
            // Steal tasks from the global queue to ensure fair task scheduling.
            steal(&self.state.queue, &local.queue);
        }

        runnable
    }
}

impl Drop for Runner<'_> {
    fn drop(&mut self) {
        // Remove the local queue.
        if let Some(local) = self.state.local_queue.get() {
            local.stop_ticking(self.state);
        }
    }
}

/// Steals some items from one queue into another.
fn steal<T>(src: &ConcurrentQueue<T>, dest: &ConcurrentQueue<T>) {
    // Half of `src`'s length rounded up.
    let mut count = (src.len() + 1) / 2;

    if count > 0 {
        // Don't steal more than fits into the queue.
        if let Some(cap) = dest.capacity() {
            count = count.min(cap - dest.len());
        }

        // Steal tasks.
        for _ in 0..count {
            if let Ok(t) = src.pop() {
                assert!(dest.push(t).is_ok());
            } else {
                break;
            }
        }
    }
}

/// Debug implementation for `Executor` and `LocalExecutor`.
fn debug_executor(executor: &Executor<'_>, name: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Get a reference to the state.
    let state = match executor.state.get() {
        Some(state) => state,
        None => {
            // The executor has not been initialized.
            struct Uninitialized;

            impl fmt::Debug for Uninitialized {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("<uninitialized>")
                }
            }

            return f.debug_tuple(name).field(&Uninitialized).finish();
        }
    };

    /// Debug wrapper for the number of active tasks.
    struct ActiveTasks<'a>(&'a Mutex<Slab<Waker>>);

    impl fmt::Debug for ActiveTasks<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0.try_lock() {
                Ok(lock) => fmt::Debug::fmt(&lock.len(), f),
                Err(TryLockError::WouldBlock) => f.write_str("<locked>"),
                Err(TryLockError::Poisoned(_)) => f.write_str("<poisoned>"),
            }
        }
    }

    /// Debug wrapper for the local runners.
    struct LocalRunners<'a>(&'a ThreadLocal<LocalQueue>);

    impl fmt::Debug for LocalRunners<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_list()
                .entries(self.0.iter().map(|local| local.queue.len()))
                .finish()
        }
    }

    f.debug_struct(name)
        .field("active", &ActiveTasks(&state.active))
        .field("global_tasks", &state.queue.len())
        .field("local_runners", &LocalRunners(&state.local_queue))
        .finish()
}

/// A queue local to each thread.
///
/// It's Default implementation is used for initializing each
/// thread's queue via `ThreadLocal::get_or_default`.
///
/// The local queue *must* be flushed, and all pending runnables
/// rescheduled onto the global queue when a runner is dropped.
struct LocalQueue {
    /// Queue of concurrent tasks.
    queue: ConcurrentQueue<Runnable>,

    /// Tickers waiting on an event from this queue.
    waiters: Event,

    /// Number of tickers waiting on this queue.
    tickers: AtomicUsize,
}

impl Default for LocalQueue {
    fn default() -> Self {
        Self {
            queue: ConcurrentQueue::bounded(512),
            waiters: Event::new(),
            tickers: AtomicUsize::new(0),
        }
    }
}

impl LocalQueue {
    /// Indicate that we are now waiting on this queue.
    fn start_ticking(&self) {
        // Relaxed ordering is fine here.
        let old_tickers = self.tickers.fetch_add(1, Ordering::Relaxed);
        if old_tickers > isize::MAX as usize {
            panic!("too many tickers waiting on one thread");
        }
    }

    /// Indicate that we are no longer waiting on this queue.
    #[inline]
    fn stop_ticking(&self, state: &State) {
        if self.tickers.fetch_sub(1, Ordering::Release) == 1 {
            // Make sure everyone knows we're about to release tasks.
            std::sync::atomic::fence(Ordering::Acquire);

            // Drain any tasks.
            self.drain_tasks(state);
        }
    }

    /// Drain all tasks from this queue.
    #[cold]
    fn drain_tasks(&self, state: &State) {
        while let Ok(task) = self.queue.pop() {
            state.queue.push(task).ok();
            state.notify();
        }
    }
}

/// Runs a closure when dropped.
struct CallOnDrop<F: FnMut()>(F);

impl<F: FnMut()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

fn _ensure_send_and_sync() {
    use futures_lite::future::pending;

    fn is_send<T: Send>(_: T) {}
    fn is_sync<T: Sync>(_: T) {}

    is_send::<Executor<'_>>(Executor::new());
    is_sync::<Executor<'_>>(Executor::new());

    let ex = Executor::new();
    is_send(ex.run(pending::<()>()));
    is_sync(ex.run(pending::<()>()));
    is_send(ex.tick());
    is_sync(ex.tick());

    /// ```compile_fail
    /// use async_executor::LocalExecutor;
    /// use futures_lite::future::pending;
    ///
    /// fn is_send<T: Send>(_: T) {}
    /// fn is_sync<T: Sync>(_: T) {}
    ///
    /// is_send::<LocalExecutor<'_>>(LocalExecutor::new());
    /// is_sync::<LocalExecutor<'_>>(LocalExecutor::new());
    ///
    /// let ex = LocalExecutor::new();
    /// is_send(ex.run(pending::<()>()));
    /// is_sync(ex.run(pending::<()>()));
    /// is_send(ex.tick());
    /// is_sync(ex.tick());
    /// ```
    fn _negative_test() {}
}
