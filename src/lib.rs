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
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, TryLockError};
use std::task::{Poll, Waker};

use async_lock::OnceCell;
use async_task::{Builder, Runnable};
use concurrent_queue::ConcurrentQueue;
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
        let runnable = Ticker::new(state).runnable().await;
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
                runnable = if let Err(err) = local.queue.push(runnable) {
                    err.into_inner()
                } else {
                    state.notify();
                    return;
                }
            }
            // If the local queue is full, fallback to pushing onto the global injector queue.
            state.queue.push(runnable).unwrap();
            state.notify();
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
    /// to othe global queue when they're full, or the task is being scheduled from
    /// a thread without a runner.
    local_queue: ThreadLocal<LocalQueue>,

    /// Set to `true` when a sleeping ticker is notified or no tickers are sleeping.
    notified: AtomicBool,

    /// A list of sleeping tickers.
    sleepers: Mutex<Sleepers>,

    /// Currently active tasks.
    active: Mutex<Slab<Waker>>,
}

impl State {
    /// Creates state for a new executor.
    fn new() -> State {
        State {
            queue: ConcurrentQueue::unbounded(),
            local_queue: ThreadLocal::new(),
            notified: AtomicBool::new(true),
            sleepers: Mutex::new(Sleepers {
                count: 0,
                wakers: Vec::new(),
                free_ids: Vec::new(),
            }),
            active: Mutex::new(Slab::new()),
        }
    }

    /// Notifies a sleeping ticker.
    #[inline]
    fn notify(&self) {
        if self
            .notified
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let waker = self.sleepers.lock().unwrap().notify();
            if let Some(w) = waker {
                w.wake();
            }
        }
    }
}

/// A list of sleeping tickers.
struct Sleepers {
    /// Number of sleeping tickers (both notified and unnotified).
    count: usize,

    /// IDs and wakers of sleeping unnotified tickers.
    ///
    /// A sleeping ticker is notified when its waker is missing from this list.
    wakers: Vec<(usize, Waker)>,

    /// Reclaimed IDs.
    free_ids: Vec<usize>,
}

impl Sleepers {
    /// Inserts a new sleeping ticker.
    fn insert(&mut self, waker: &Waker) -> usize {
        let id = match self.free_ids.pop() {
            Some(id) => id,
            None => self.count + 1,
        };
        self.count += 1;
        self.wakers.push((id, waker.clone()));
        id
    }

    /// Re-inserts a sleeping ticker's waker if it was notified.
    ///
    /// Returns `true` if the ticker was notified.
    fn update(&mut self, id: usize, waker: &Waker) -> bool {
        for item in &mut self.wakers {
            if item.0 == id {
                if !item.1.will_wake(waker) {
                    item.1 = waker.clone();
                }
                return false;
            }
        }

        self.wakers.push((id, waker.clone()));
        true
    }

    /// Removes a previously inserted sleeping ticker.
    ///
    /// Returns `true` if the ticker was notified.
    fn remove(&mut self, id: usize) -> bool {
        self.count -= 1;
        self.free_ids.push(id);

        for i in (0..self.wakers.len()).rev() {
            if self.wakers[i].0 == id {
                self.wakers.remove(i);
                return false;
            }
        }
        true
    }

    /// Returns `true` if a sleeping ticker is notified or no tickers are sleeping.
    fn is_notified(&self) -> bool {
        self.count == 0 || self.count > self.wakers.len()
    }

    /// Returns notification waker for a sleeping ticker.
    ///
    /// If a ticker was notified already or there are no tickers, `None` will be returned.
    fn notify(&mut self) -> Option<Waker> {
        if self.wakers.len() == self.count {
            self.wakers.pop().map(|item| item.1)
        } else {
            None
        }
    }
}

/// Runs task one by one.
struct Ticker<'a> {
    /// The executor state.
    state: &'a State,

    /// Set to a non-zero sleeper ID when in sleeping state.
    ///
    /// States a ticker can be in:
    /// 1) Woken.
    /// 2a) Sleeping and unnotified.
    /// 2b) Sleeping and notified.
    sleeping: usize,
}

impl Ticker<'_> {
    /// Creates a ticker.
    fn new(state: &State) -> Ticker<'_> {
        Ticker { state, sleeping: 0 }
    }

    /// Moves the ticker into sleeping and unnotified state.
    ///
    /// Returns `false` if the ticker was already sleeping and unnotified.
    fn sleep(&mut self, waker: &Waker) -> bool {
        let mut sleepers = self.state.sleepers.lock().unwrap();

        match self.sleeping {
            // Move to sleeping state.
            0 => {
                self.sleeping = sleepers.insert(waker);
            }

            // Already sleeping, check if notified.
            id => {
                if !sleepers.update(id, waker) {
                    return false;
                }
            }
        }

        self.state
            .notified
            .store(sleepers.is_notified(), Ordering::Release);

        true
    }

    /// Moves the ticker into woken state.
    fn wake(&mut self) {
        if self.sleeping != 0 {
            let mut sleepers = self.state.sleepers.lock().unwrap();
            sleepers.remove(self.sleeping);

            self.state
                .notified
                .store(sleepers.is_notified(), Ordering::Release);
        }
        self.sleeping = 0;
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&mut self) -> Runnable {
        self.runnable_with(|| {
            self.state
                .local_queue
                .get()
                .and_then(|local| local.queue.pop().ok())
                .or_else(|| self.state.queue.pop().ok())
        })
        .await
    }

    /// Waits for the next runnable task to run, given a function that searches for a task.
    async fn runnable_with(&mut self, mut search: impl FnMut() -> Option<Runnable>) -> Runnable {
        future::poll_fn(|cx| {
            loop {
                match search() {
                    None => {
                        // Move to sleeping and unnotified state.
                        if !self.sleep(cx.waker()) {
                            // If already sleeping and unnotified, return.
                            return Poll::Pending;
                        }
                    }
                    Some(r) => {
                        // Wake up.
                        self.wake();

                        // Notify another ticker now to pick up where this ticker left off, just in
                        // case running the task takes a long time.
                        self.state.notify();

                        return Poll::Ready(r);
                    }
                }
            }
        })
        .await
    }
}

impl Drop for Ticker<'_> {
    fn drop(&mut self) {
        // If this ticker is in sleeping state, it must be removed from the sleepers list.
        if self.sleeping != 0 {
            let mut sleepers = self.state.sleepers.lock().unwrap();
            let notified = sleepers.remove(self.sleeping);

            self.state
                .notified
                .store(sleepers.is_notified(), Ordering::Release);

            // If this ticker was notified, then notify another ticker.
            if notified {
                drop(sleepers);
                self.state.notify();
            }
        }
    }
}

/// A worker in a work-stealing executor.
///
/// This is just a ticker that also has an associated local queue for improved cache locality.
struct Runner<'a> {
    /// The executor state.
    state: &'a State,

    /// Inner ticker.
    ticker: Ticker<'a>,

    /// Bumped every time a runnable task is found.
    ticks: usize,
}

impl Runner<'_> {
    /// Creates a runner and registers it in the executor state.
    fn new(state: &State) -> Runner<'_> {
        let runner = Runner {
            state,
            ticker: Ticker::new(state),
            ticks: 0,
        };
        runner
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&mut self, rng: &mut fastrand::Rng) -> Runnable {
        let local = self.state.local_queue.get_or_default();

        let runnable = self
            .ticker
            .runnable_with(|| {
                // Try the local queue.
                if let Ok(r) = local.queue.pop() {
                    return Some(r);
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
        self.ticks += 1;

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
            // Re-schedule remaining tasks in the local queue.
            for r in local.queue.try_iter() {
                // Explicitly reschedule the runnable back onto the global
                // queue to avoid rescheduling onto the local one.
                self.state.queue.push(r).unwrap();
            }
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
    struct LocalRunners<'a>(&'a RwLock<Vec<Arc<ConcurrentQueue<Runnable>>>>);

    impl fmt::Debug for LocalRunners<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0.try_read() {
                Ok(lock) => f
                    .debug_list()
                    .entries(lock.iter().map(|queue| queue.len()))
                    .finish(),
                Err(TryLockError::WouldBlock) => f.write_str("<locked>"),
                Err(TryLockError::Poisoned(_)) => f.write_str("<poisoned>"),
            }
        }
    }

    /// Debug wrapper for the sleepers.
    struct SleepCount<'a>(&'a Mutex<Sleepers>);

    impl fmt::Debug for SleepCount<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0.try_lock() {
                Ok(lock) => fmt::Debug::fmt(&lock.count, f),
                Err(TryLockError::WouldBlock) => f.write_str("<locked>"),
                Err(TryLockError::Poisoned(_)) => f.write_str("<poisoned>"),
            }
        }
    }

    f.debug_struct(name)
        .field("active", &ActiveTasks(&state.active))
        .field("global_tasks", &state.queue.len())
        .field("sleepers", &SleepCount(&state.sleepers))
        .finish()
}

/// A queue local to each thread.
///
/// It's Default implementation is used for initializing
///
/// The local queue *must* be flushed, and all pending runnables
/// rescheduled onto the global queue when a runner is dropped.
struct LocalQueue {
    queue: ConcurrentQueue<Runnable>,
}

impl Default for LocalQueue {
    fn default() -> Self {
        Self {
            queue: ConcurrentQueue::bounded(512),
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
