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
use futures_lite::{future, FutureExt};
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
    /// The executor state.
    state: once_cell::sync::OnceCell<Arc<Mutex<State>>>,

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
            state: once_cell::sync::OnceCell::new(),
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
        unsafe { self.spawn_unchecked(future) }
    }

    unsafe fn spawn_unchecked<T>(&self, future: impl Future<Output = T>) -> Task<T> {
        // Remove the task from the set of active tasks when the future finishes.
        let future = {
            let state = self.state().clone();
            async move {
                futures_lite::pin!(future);
                let mut index = None;
                future::poll_fn(|cx| {
                    let poll = future.as_mut().poll(cx);
                    if poll.is_pending() {
                        if index.is_none() {
                            index = Some(state.lock().active.insert(cx.waker().clone()));
                        }
                    } else {
                        if let Some(index) = index {
                            state.lock().active.remove(index);
                        }
                    }
                    poll
                })
                .await
            }
        };

        // Create the task and register it in the set of active tasks.
        let (runnable, task) = async_task::spawn_unchecked(future, self.schedule());
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
        let mut state = self.state().lock();

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
        let runnable = Ticker::new(self.state()).runnable().await;
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
        let mut ticker = Ticker::new(self.state());

        // A future that runs tasks forever.
        let run_forever = async {
            loop {
                for _ in 0..200 {
                    let runnable = ticker.runnable().await;
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

        // TODO(stjepang): If possible, push into the current local queue and notify the ticker.
        move |runnable| {
            let mut state = state.lock();
            state.queue.push_back(runnable);
            notify(state);
        }
    }

    /// Returns a reference to the inner state.
    fn state(&self) -> &Arc<Mutex<State>> {
        self.state.get_or_init(|| Default::default())
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.state.get() {
            let mut state = state.lock();
            let mut wakers = Vec::new();
            for i in 0..state.active.capacity() {
                if let Some(w) = state.active.remove(i) {
                    wakers.push(w);
                }
            }
            drop(state);
            for w in wakers {
                w.wake();
            }
            let runnables = mem::replace(&mut self.state().lock().queue, VecDeque::new());
            drop(runnables);
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
        unsafe { self.inner().spawn_unchecked(future) }
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

/// The state of a executor.
#[derive(Debug, Default)]
struct State {
    /// The global queue.
    queue: VecDeque<Runnable>,

    /// Set to `true` when a sleeping ticker is notified or no tickers are sleeping.
    notified: bool,

    /// A list of sleeping tickers.
    sleepers: Sleepers,

    /// Currently active tasks.
    active: Arena<Waker>,
}

/// Notifies a sleeping ticker.
#[inline]
fn notify(mut state: MutexGuard<'_, State>) {
    if !state.notified {
        state.notified = true;
        let waker = state.sleepers.notify();
        drop(state);

        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// A list of sleeping tickers.
#[derive(Debug, Default)]
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
    #[inline]
    fn insert(&mut self, waker: &Waker) -> usize {
        let id = match self.free_ids.pop() {
            Some(id) => id,
            None => self.count + 1,
        };
        if self.count > self.wakers.len() {
            self.wakers.push((id, waker.clone()));
        } else {
            waker.wake_by_ref();
        }
        self.count += 1;
        id
    }

    /// Re-inserts a sleeping ticker's waker if it was notified.
    #[inline]
    fn update(&mut self, id: usize, waker: &Waker) {
        for item in &mut self.wakers {
            if item.0 == id {
                if !item.1.will_wake(waker) {
                    item.1 = waker.clone();
                }
                return;
            }
        }

        self.wakers.push((id, waker.clone()));
    }

    /// Removes a previously inserted sleeping ticker.
    ///
    /// Returns `true` if the ticker was notified.
    #[inline]
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
    #[inline]
    fn is_notified(&self) -> bool {
        self.count == 0 || self.count > self.wakers.len()
    }

    /// Returns notification waker for a sleeping ticker.
    ///
    /// If a ticker was notified already or there are no tickers, `None` will be returned.
    #[inline]
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
    state: &'a Mutex<State>,

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
    fn new(state: &Mutex<State>) -> Ticker<'_> {
        Ticker { state, sleeping: 0 }
    }

    /// Waits for the next runnable task to run.
    async fn runnable(&mut self) -> Runnable {
        future::poll_fn(|cx| {
            let mut state = self.state.lock();

            match state.queue.pop_front() {
                None => {
                    // Move to sleeping and unnotified state.
                    match self.sleeping {
                        0 => self.sleeping = state.sleepers.insert(cx.waker()),
                        id => state.sleepers.update(id, cx.waker()),
                    }
                    state.notified = state.sleepers.is_notified();

                    Poll::Pending
                }
                Some(r) => {
                    // Wake up.
                    let id = mem::replace(&mut self.sleeping, 0);
                    if id != 0 {
                        state.sleepers.remove(id);
                        state.notified = state.sleepers.is_notified();
                    }

                    // Notify another ticker now to pick up where this ticker left off, just in
                    // case running the task takes a long time.
                    notify(state);

                    Poll::Ready(r)
                }
            }
        })
        .await
    }
}

impl Drop for Ticker<'_> {
    fn drop(&mut self) {
        // If this ticker is in sleeping state, it must be removed from the sleepers list.
        let id = self.sleeping;
        if id != 0 {
            let mut state = self.state.lock();
            let notified = state.sleepers.remove(id);
            state.notified = state.sleepers.is_notified();

            // If this ticker was notified, then notify another ticker.
            if notified {
                notify(state);
            }
        }
    }
}
