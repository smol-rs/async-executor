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
//! // Run the executor until the task completes.
//! future::block_on(ex.run(task));
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

use std::future::Future;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Poll, Waker};

use async_lock::OnceCell;
use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;
use futures_lite::{future, prelude::*};
use slab::Slab;

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
    /// The executor state.
    state: OnceCell<Arc<State>>,

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
        let index = active.vacant_entry().key();
        let state = self.state().clone();
        let future = async move {
            let _guard = CallOnDrop(move || drop(state.active.lock().unwrap().try_remove(index)));
            future.await
        };

        // Create the task and register it in the set of active tasks.
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, self.schedule()) };
        active.insert(runnable.waker());

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
        let runner = Runner::new(self.state());

        // A future that runs tasks forever.
        let run_forever = async {
            loop {
                for _ in 0..200 {
                    let runnable = runner.runnable().await;
                    runnable.run();
                }
                future::yield_now().await;
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future.or(run_forever).await
    }

    /// Runs the provided function without blocking the executor.
    ///
    /// In general, you shouldn't run blocking code inside of a future. This code will block the
    /// executor and prevent it from running other tasks. However, sometimes you need to run
    /// blocking code, for example, to interact with files. In this case, you can use this method
    /// to run the blocking code without blocking the executor.
    ///
    /// ## Usage
    ///
    /// This method works by emptying the local queues held by each instance of the [`run()`] method
    /// working on this thread and moving it back to the global queue. Since the runners don't have a
    /// chance to steal more threads from the global queue (as the thread is running this task instead),
    /// other available runners will be able to pick up the tasks and run them.
    ///
    /// This assumes that the current `Executor` is calling [`run()`] from the top level of the thread (i.e.
    /// the executor and nothing else is driving the thread). This assumption may be violated if, for
    /// example, the executor is running inside of another `Executor`. As that `Executor`'s `run()`
    /// method will be unaware of this call, the blocking task will affect that executor as well.
    /// Therefore, care should be taken in order to ensure that this method is only called from the
    /// top level of the thread.
    ///
    /// [`unblock()`] does not have this caveat and instead uses a different mechanism to run the
    /// blocking task. In general, [`unblock()`] should be preferred over this method, except when
    /// it is not possible to use it.
    ///
    /// [`unblock()`]: https://docs.rs/blocking/latest/blocking/fn.unblock.html
    ///
    /// ## Example
    ///
    /// ```
    /// use async_executor::Executor;
    /// use futures_lite::future;
    /// use std::time::Duration;
    ///
    /// let ex = Executor::new();
    ///
    /// // Spawn a task.
    /// let task = ex.spawn(async {
    ///     println!("Hello world");
    /// });
    ///
    /// // Run the executor.
    /// future::block_on(ex.run(async {
    ///     // Run a blocking task.
    ///     ex.block_in_place(|| {
    ///        std::thread::sleep(Duration::from_secs(1));
    ///     });
    ///
    ///     // Finish the task.
    ///     task.await;
    /// }));
    /// ```
    pub fn block_in_place<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let state = self.state();

        // Issue a compiler fence to allow the Relaxed atomic operations for setting
        // the thread ID per runner to catch up.
        fence(Ordering::SeqCst);

        // Iterate over each runner and, if it's running on our thread, empty its local
        // queue.
        let thread_id = thread_id();
        let runners = state.local_queues.read().unwrap();
        let mut new_runners = false;

        runners.iter().for_each(|runner| {
            if runner.thread.load(Ordering::Acquire) == thread_id {
                // Empty the local queue.
                while let Ok(runnable) = runner.tasks.pop() {
                    state.queue.push(runnable).ok();
                    new_runners = true;
                }
            }
        });

        // Stop holding a lock on the local queues.
        drop(runners);

        // Notify any sleeping runners that there are new tasks.
        if new_runners {
            state.notify();
        }

        // There are no more pending tasks on our thread, so we can run the function without fear
        // of blocking the executor.
        f()
    }

    /// Returns a function that schedules a runnable task when it gets woken up.
    fn schedule(&self) -> impl Fn(Runnable) + Send + Sync + 'static {
        let state = self.state().clone();

        // TODO(stjepang): If possible, push into the current local queue and notify the ticker.
        move |runnable| {
            state.queue.push(runnable).unwrap();
            state.notify();
        }
    }

    /// Returns a reference to the inner state.
    fn state(&self) -> &Arc<State> {
        self.state.get_or_init_blocking(|| Arc::new(State::new()))
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
#[derive(Debug)]
pub struct LocalExecutor<'a> {
    /// The inner executor.
    inner: Executor<'a>,

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
        let index = active.vacant_entry().key();
        let state = self.inner().state().clone();
        let future = async move {
            let _guard = CallOnDrop(move || drop(state.active.lock().unwrap().try_remove(index)));
            future.await
        };

        // Create the task and register it in the set of active tasks.
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, self.schedule()) };
        active.insert(runnable.waker());

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
#[derive(Debug)]
struct State {
    /// The global queue.
    queue: ConcurrentQueue<Runnable>,

    /// Local queues created by runners.
    local_queues: RwLock<Vec<Arc<LocalQueue>>>,

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
            local_queues: RwLock::new(Vec::new()),
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
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
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
#[derive(Debug)]
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
#[derive(Debug)]
struct Ticker<'a> {
    /// The executor state.
    state: &'a State,

    /// Set to a non-zero sleeper ID when in sleeping state.
    ///
    /// States a ticker can be in:
    /// 1) Woken.
    /// 2a) Sleeping and unnotified.
    /// 2b) Sleeping and notified.
    sleeping: AtomicUsize,
}

impl Ticker<'_> {
    /// Creates a ticker.
    fn new(state: &State) -> Ticker<'_> {
        Ticker {
            state,
            sleeping: AtomicUsize::new(0),
        }
    }

    /// Moves the ticker into sleeping and unnotified state.
    ///
    /// Returns `false` if the ticker was already sleeping and unnotified.
    fn sleep(&self, waker: &Waker) -> bool {
        let mut sleepers = self.state.sleepers.lock().unwrap();

        match self.sleeping.load(Ordering::SeqCst) {
            // Move to sleeping state.
            0 => self
                .sleeping
                .store(sleepers.insert(waker), Ordering::SeqCst),

            // Already sleeping, check if notified.
            id => {
                if !sleepers.update(id, waker) {
                    return false;
                }
            }
        }

        self.state
            .notified
            .swap(sleepers.is_notified(), Ordering::SeqCst);

        true
    }

    /// Moves the ticker into woken state.
    fn wake(&self) {
        let id = self.sleeping.swap(0, Ordering::SeqCst);
        if id != 0 {
            let mut sleepers = self.state.sleepers.lock().unwrap();
            sleepers.remove(id);

            self.state
                .notified
                .swap(sleepers.is_notified(), Ordering::SeqCst);
        }
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&self) -> Runnable {
        self.runnable_with(|| self.state.queue.pop().ok()).await
    }

    /// Waits for the next runnable task to run, given a function that searches for a task.
    async fn runnable_with(&self, mut search: impl FnMut() -> Option<Runnable>) -> Runnable {
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
        let id = self.sleeping.swap(0, Ordering::SeqCst);
        if id != 0 {
            let mut sleepers = self.state.sleepers.lock().unwrap();
            let notified = sleepers.remove(id);

            self.state
                .notified
                .swap(sleepers.is_notified(), Ordering::SeqCst);

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
#[derive(Debug)]
struct Runner<'a> {
    /// The executor state.
    state: &'a State,

    /// Inner ticker.
    ticker: Ticker<'a>,

    /// The local queue.
    local: Arc<LocalQueue>,

    /// Bumped every time a runnable task is found.
    ticks: AtomicUsize,
}

impl Runner<'_> {
    /// Creates a runner and registers it in the executor state.
    fn new(state: &State) -> Runner<'_> {
        let runner = Runner {
            state,
            ticker: Ticker::new(state),
            local: Arc::new(LocalQueue {
                tasks: ConcurrentQueue::bounded(512),
                thread: AtomicUsize::new(thread_id()),
            }),
            ticks: AtomicUsize::new(0),
        };
        state
            .local_queues
            .write()
            .unwrap()
            .push(runner.local.clone());
        runner
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&self) -> Runnable {
        // Store the current thread that is running this runner.
        //
        // Relaxed ordering is fine here since a SeqCst fence is issued before the value is
        // actually loaded.
        self.local.thread.store(thread_id(), Ordering::Relaxed);

        let runnable = self
            .ticker
            .runnable_with(|| {
                // Try the local queue.
                if let Ok(r) = self.local.tasks.pop() {
                    return Some(r);
                }

                // Try stealing from the global queue.
                if let Ok(r) = self.state.queue.pop() {
                    steal(&self.state.queue, &self.local.tasks);
                    return Some(r);
                }

                // Try stealing from other runners.
                let local_queues = self.state.local_queues.read().unwrap();

                // Pick a random starting point in the iterator list and rotate the list.
                let n = local_queues.len();
                let start = fastrand::usize(..n);
                let iter = local_queues
                    .iter()
                    .chain(local_queues.iter())
                    .skip(start)
                    .take(n);

                // Remove this runner's local queue.
                let iter = iter.filter(|local| !Arc::ptr_eq(local, &self.local));

                // Try stealing from each local queue in the list.
                for local in iter {
                    steal(&local.tasks, &self.local.tasks);
                    if let Ok(r) = self.local.tasks.pop() {
                        return Some(r);
                    }
                }

                None
            })
            .await;

        // Bump the tick counter.
        let ticks = self.ticks.fetch_add(1, Ordering::SeqCst);

        if ticks % 64 == 0 {
            // Steal tasks from the global queue to ensure fair task scheduling.
            steal(&self.state.queue, &self.local.tasks);
        }

        runnable
    }
}

impl Drop for Runner<'_> {
    fn drop(&mut self) {
        // Remove the local queue.
        self.state
            .local_queues
            .write()
            .unwrap()
            .retain(|local| !Arc::ptr_eq(local, &self.local));

        // Re-schedule remaining tasks in the local queue.
        while let Ok(r) = self.local.tasks.pop() {
            r.schedule();
        }
    }
}

/// A local queue contains a list of tasks that belong to a specific runner.
#[derive(Debug)]
struct LocalQueue {
    /// The tasks that this runner owns.
    tasks: ConcurrentQueue<Runnable>,

    /// The thread that this task is running on.
    thread: AtomicUsize,
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

/// Get a value that will be unique for every thread.
///
/// The lowest bit is guaranteed to be clear.
fn thread_id() -> usize {
    // ThreadId won't work in this case, because it can't be represented as an
    // atomic variable that we can associate with each runner.
    //
    // Instead, return the address of a thread local variable. We use a word so
    // that the lowest bit is always clear.
    thread_local! {
        static THREAD_VARIABLE: u32 = 0xABCD;
    }

    // Note: Don't call this in destructors; otherwise, we may cause a panic.
    THREAD_VARIABLE.with(|th| th as *const u32 as usize)
}

/// Runs a closure when dropped.
struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
