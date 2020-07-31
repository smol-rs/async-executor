//! Async executor.
//!
//! This crate offers two kinds of executors: single-threaded and multi-threaded.
//!
//! # Examples
//!
//! Run a single-threaded and a multi-threaded executor at the same time:
//!
//! ```
//! use async_channel::unbounded;
//! use async_executor::{Executor, LocalExecutor};
//! use easy_parallel::Parallel;
//!
//! let ex = Executor::new();
//! let local_ex = LocalExecutor::new();
//! let (trigger, shutdown) = unbounded::<()>();
//!
//! Parallel::new()
//!     // Run four executor threads.
//!     .each(0..4, |_| ex.run(shutdown.recv()))
//!     // Run local executor on the current thread.
//!     .finish(|| local_ex.run(async {
//!         println!("Hello world!");
//!         drop(trigger);
//!     }));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_lite::pin;
use scoped_tls::scoped_thread_local;
use waker_fn::waker_fn;

#[cfg(feature = "async-io")]
use async_io::parking;
#[cfg(not(feature = "async-io"))]
use parking;

scoped_thread_local!(static EX: Executor);
scoped_thread_local!(static LOCAL_EX: LocalExecutor);

/// Multi-threaded executor.
///
/// The executor does not spawn threads on its own. Instead, you need to call [`Executor::run()`]
/// on manually spawned executor threads.
///
/// # Examples
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
///     .each(0..4, |_| ex.run(shutdown.recv()))
///     // Run the main future on the current thread.
///     .finish(|| future::block_on(async {
///         println!("Hello world!");
///         drop(signal);
///     }));
/// ```
#[derive(Debug)]
pub struct Executor {
    ex: Arc<multitask::Executor>,
}

impl Executor {
    /// Creates a multi-threaded executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new();
    /// ```
    pub fn new() -> Executor {
        Executor {
            ex: Arc::new(multitask::Executor::new()),
        }
    }

    /// Creates a spawner for this executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new();
    /// let spawner = ex.spawner();
    /// ```
    pub fn spawner(&self) -> Spawner {
        Spawner {
            ex: self.ex.clone(),
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
    pub fn spawn<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T> {
        Task(self.ex.spawn(future))
    }

    /// Enters the context of an executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{Executor, Task};
    ///
    /// let ex = Executor::new();
    ///
    /// ex.enter(|| {
    ///     // `Task::spawn()` now knows which executor to spawn onto.
    ///     let task = Task::spawn(async {
    ///         println!("Hello world");
    ///     });
    /// });
    /// ```
    pub fn enter<T>(&self, f: impl FnOnce() -> T) -> T {
        if EX.is_set() {
            panic!("cannot call `Executor::enter()` if already inside an `Executor`");
        }
        EX.set(self, f)
    }

    /// Runs the executor until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::Executor;
    ///
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async { 1 + 2 });
    /// let res = ex.run(async { task.await * 2 });
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub fn run<T>(&self, future: impl Future<Output = T>) -> T {
        self.enter(|| {
            let (p, u) = parking::pair();

            let ticker = self.ex.ticker({
                let u = u.clone();
                move || u.unpark()
            });

            pin!(future);
            let waker = waker_fn(move || u.unpark());
            let cx = &mut Context::from_waker(&waker);

            'start: loop {
                if let Poll::Ready(t) = future.as_mut().poll(cx) {
                    break t;
                }

                for _ in 0..200 {
                    if !ticker.tick() {
                        p.park();
                        continue 'start;
                    }
                }
                p.park_timeout(Duration::from_secs(0));
            }
        })
    }

    /// Runs a local executor and the multi-threaded one until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{Executor, Task};
    ///
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async { 1 + 2 });
    /// let res = ex.run_with_local(async {
    ///     let local = Task::local(async { 1 + 1 });
    ///     task.await * local.await
    /// });
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub fn run_with_local<T>(&self, future: impl Future<Output = T>) -> T {
        let local_executor = LocalExecutor::new();
        LOCAL_EX.set(&local_executor, || {
            self.enter(|| {
                let (p, u) = parking::pair();

                let ticker = self.ex.ticker({
                    let u = u.clone();
                    move || u.unpark()
                });

                pin!(future);
                let waker = waker_fn(move || u.unpark());
                let cx = &mut Context::from_waker(&waker);

                'start: loop {
                    if let Poll::Ready(t) = future.as_mut().poll(cx) {
                        break t;
                    }

                    for _ in 0..200 {
                        if !local_executor.ex.tick() && !ticker.tick() {
                            p.park();
                            continue 'start;
                        }
                    }
                    p.park_timeout(Duration::from_secs(0));
                }
            })
        })
    }
}

impl Default for Executor {
    fn default() -> Executor {
        Executor::new()
    }
}

/// A spawner for a multi-threaded executor.
#[derive(Debug)]
pub struct Spawner {
    ex: Arc<multitask::Executor>,
}

impl Spawner {
    /// Gets a spawner for the current multi-threaded executor.
    ///
    /// If called from an [`Executor`], returns its [`Spawner`].
    ///
    /// Otherwise, this method panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{Executor, Spawner};
    ///
    /// let ex = Executor::new();
    ///
    /// ex.run(async {
    ///     let spawner = Spawner::current();
    ///     let task = spawner.spawn(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    pub fn current() -> Spawner {
        if EX.is_set() {
            EX.with(|ex| ex.spawner())
        } else {
            panic!("`Spawner::current()` must be called from an `Executor`")
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
    /// let spawner = ex.spawner();
    ///
    /// let task = spawner.spawn(async {
    ///     println!("Hello world");
    /// });
    /// ```
    pub fn spawn<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T> {
        Task(self.ex.spawn(future))
    }
}

/// Single-threaded executor.
///
/// The executor can only be run on the thread that created it.
///
/// # Examples
///
/// ```
/// use async_executor::LocalExecutor;
///
/// let local_ex = LocalExecutor::new();
///
/// local_ex.run(async {
///     println!("Hello world!");
/// });
/// ```
#[derive(Debug)]
pub struct LocalExecutor {
    ex: multitask::LocalExecutor,
    parker: parking::Parker,
}

impl LocalExecutor {
    /// Creates a single-threaded executor.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::new();
    /// ```
    pub fn new() -> LocalExecutor {
        let (p, u) = parking::pair();
        LocalExecutor {
            ex: multitask::LocalExecutor::new(move || u.unpark()),
            parker: p,
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
    pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
        Task(self.ex.spawn(future))
    }

    /// Runs the executor until the given future completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    ///
    /// let local_ex = LocalExecutor::new();
    ///
    /// let task = local_ex.spawn(async { 1 + 2 });
    /// let res = local_ex.run(async { task.await * 2 });
    ///
    /// assert_eq!(res, 6);
    /// ```
    pub fn run<T>(&self, future: impl Future<Output = T>) -> T {
        pin!(future);

        let u = self.parker.unparker();
        let waker = waker_fn(move || u.unpark());
        let cx = &mut Context::from_waker(&waker);

        LOCAL_EX.set(self, || {
            'start: loop {
                if let Poll::Ready(t) = future.as_mut().poll(cx) {
                    break t;
                }

                for _ in 0..200 {
                    if !self.ex.tick() {
                        self.parker.park();
                        continue 'start;
                    }
                }
                self.parker.park_timeout(Duration::from_secs(0));
            }
        })
    }
}

impl Default for LocalExecutor {
    fn default() -> LocalExecutor {
        LocalExecutor::new()
    }
}

/// A spawned future.
///
/// Tasks are also futures themselves and yield the output of the spawned future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To cancel a task a bit
/// more gracefully and wait until it stops running, use the [`cancel()`][`Task::cancel()`] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also causes a panic.
///
/// # Examples
///
/// ```
/// use async_executor::{Executor, Task};
///
/// let ex = Executor::new();
///
/// ex.run(async {
///     let task = Task::spawn(async {
///         println!("Hello from a task!");
///         1 + 2
///     });
///
///     assert_eq!(task.await, 3);
/// });
/// ```
#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
#[derive(Debug)]
pub struct Task<T>(multitask::Task<T>);

impl<T> Task<T> {
    /// Spawns a task onto the current multi-threaded or single-threaded executor.
    ///
    /// If called from an [`Executor`] (preferred) or from a [`LocalExecutor`], the task is spawned
    /// on it.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{Executor, Task};
    ///
    /// let ex = Executor::new();
    ///
    /// ex.run(async {
    ///     let task = Task::spawn(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    ///
    /// ```
    /// use async_executor::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::new();
    ///
    /// local_ex.run(async {
    ///     let task = Task::spawn(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    pub fn spawn(future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        if EX.is_set() {
            EX.with(|ex| ex.spawn(future))
        } else if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.spawn(future))
        } else {
            panic!("`Task::spawn()` must be called from an `Executor` or `LocalExecutor`")
        }
    }

    /// Spawns a task onto the current single-threaded executor.
    ///
    /// If called from a [`LocalExecutor`], the task is spawned on it.
    ///
    /// Otherwise, this method panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{LocalExecutor, Task};
    ///
    /// let local_ex = LocalExecutor::new();
    ///
    /// local_ex.run(async {
    ///     let task = Task::local(async { 1 + 2 });
    ///     assert_eq!(task.await, 3);
    /// });
    /// ```
    pub fn local(future: impl Future<Output = T> + 'static) -> Task<T>
    where
        T: 'static,
    {
        if LOCAL_EX.is_set() {
            LOCAL_EX.with(|local_ex| local_ex.spawn(future))
        } else {
            panic!("`Task::local()` must be called from a `LocalExecutor`")
        }
    }

    /// Detaches the task to let it keep running in the background.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{Executor, Task};
    /// use futures_lite::future;
    ///
    /// let ex = Executor::new();
    ///
    /// ex.spawn(async {
    ///     loop {
    ///         println!("I'm a background task looping forever.");
    ///         future::yield_now().await;
    ///     }
    /// })
    /// .detach();
    ///
    /// ex.run(future::yield_now());
    /// ```
    pub fn detach(self) {
        self.0.detach();
    }

    /// Cancels the task and waits for it to stop running.
    ///
    /// Returns the task's output if it was completed just before it got canceled, or [`None`] if
    /// it didn't complete.
    ///
    /// While it's possible to simply drop the [`Task`] to cancel it, this is a cleaner way of
    /// canceling because it also waits for the task to stop running.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_executor::{Executor, Task};
    /// use futures_lite::future;
    ///
    /// let ex = Executor::new();
    ///
    /// let task = ex.spawn(async {
    ///     loop {
    ///         println!("Even though I'm in an infinite loop, you can still cancel me!");
    ///         future::yield_now().await;
    ///     }
    /// });
    ///
    /// ex.run(async {
    ///     task.cancel().await;
    /// });
    /// ```
    pub async fn cancel(self) -> Option<T> {
        self.0.cancel().await
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
