use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::rc::Rc;
use std::task::{Poll, Waker};

use async_task::{Builder, Runnable, Schedule};
use futures_lite::{future, prelude::*};
use slab::Slab;

use crate::{AsyncCallOnDrop, Sleepers};
#[doc(no_inline)]
pub use async_task::Task;

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
    /// The executor state.
    state: Cell<*mut State>,

    /// Makes the `'a` lifetime invariant.
    _marker: PhantomData<UnsafeCell<&'a ()>>,
}

impl UnwindSafe for LocalExecutor<'_> {}
impl RefUnwindSafe for LocalExecutor<'_> {}

impl fmt::Debug for LocalExecutor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_executor(self, "LocalExecutor", f)
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
            state: Cell::new(std::ptr::null_mut()),
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
        // SAFETY: All UnsafeCell accesses to active are tightly scoped, and because
        // `LocalExecutor` is !Send, there is no way to have concurrent access to the
        // values in `State`, including the active field.
        unsafe { &*self.state().active.get() }.is_empty()
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
        let state = self.state_as_rc();
        // SAFETY: All UnsafeCell accesses to active are tightly scoped, and because
        // `LocalExecutor` is !Send, there is no way to have concurrent access to the
        // values in `State`, including the active field.
        let active = unsafe { &mut *self.state().active.get() };
        let schedule = Self::schedule(state.clone());
        Self::spawn_inner(state, future, active, schedule)
    }

    /// Spawns many tasks onto the executor.
    ///
    /// As opposed to the [`spawn`] method, this locks the executor's inner task lock once and
    /// spawns all of the tasks in one go. With large amounts of tasks this can improve
    /// contention.
    ///
    /// It is assumed that the iterator provided does not block; blocking iterators can lock up
    /// the internal mutex and therefore the entire executor. Unlike [`Executor::spawn`], the
    /// mutex is not released, as there are no other threads that can poll this executor.
    ///
    /// ## Example
    ///
    /// ```
    /// use async_executor::LocalExecutor;
    /// use futures_lite::{stream, prelude::*};
    /// use std::future::ready;
    ///
    /// # futures_lite::future::block_on(async {
    /// let mut ex = LocalExecutor::new();
    ///
    /// let futures = [
    ///     ready(1),
    ///     ready(2),
    ///     ready(3)
    /// ];
    ///
    /// // Spawn all of the futures onto the executor at once.
    /// let mut tasks = vec![];
    /// ex.spawn_many(futures, &mut tasks);
    ///
    /// // Await all of them.
    /// let results = ex.run(async move {
    ///     stream::iter(tasks).then(|x| x).collect::<Vec<_>>().await
    /// }).await;
    /// assert_eq!(results, [1, 2, 3]);
    /// # });
    /// ```
    ///
    /// [`spawn`]: LocalExecutor::spawn
    /// [`Executor::spawn_many`]: Executor::spawn_many
    pub fn spawn_many<T: 'a, F: Future<Output = T> + 'a>(
        &self,
        futures: impl IntoIterator<Item = F>,
        handles: &mut impl Extend<Task<F::Output>>,
    ) {
        let tasks = {
            let state = self.state_as_rc();

            // SAFETY: All UnsafeCell accesses to active are tightly scoped, and because
            // `LocalExecutor` is !Send, there is no way to have concurrent access to the
            // values in `State`, including the active field.
            let active = unsafe { &mut *state.active.get() };

            // Convert the futures into tasks.
            futures.into_iter().map(move |future| {
                let schedule = Self::schedule(state.clone());
                Self::spawn_inner(state.clone(), future, active, schedule)
            })
        };

        // Push the tasks to the user's collection.
        handles.extend(tasks);
    }

    /// Spawn a future while holding the inner lock.
    fn spawn_inner<T: 'a>(
        state: Rc<State>,
        future: impl Future<Output = T> + 'a,
        active: &mut Slab<Waker>,
        schedule: impl Schedule + 'static,
    ) -> Task<T> {
        // Remove the task from the set of active tasks when the future finishes.
        let entry = active.vacant_entry();
        let index = entry.key();
        let future = AsyncCallOnDrop::new(future, move || {
            // SAFETY: All UnsafeCell accesses to active are tightly scoped, and because
            // `LocalExecutor` is !Send, there is no way to have concurrent access to the
            // values in `State`, including the active field.
            drop(unsafe { &mut *state.active.get() }.try_remove(index))
        });

        // Create the task and register it in the set of active tasks.
        //
        // SAFETY:
        //
        // `future` may not `Send`. Since `LocalExecutor` is `!Sync`,
        // `try_tick`, `tick` and `run` can only be called from the origin
        // thread of the `LocalExecutor`. Similarly, `spawn` can only  be called
        // from the origin thread, ensuring that `future` and the executor share
        // the same origin thread. The `Runnable` can be scheduled from other
        // threads, but because of the above `Runnable` can only be called or
        // dropped on the origin thread.
        //
        // `future` is not `'static`, but we make sure that the `Runnable` does
        // not outlive `'a`. When the executor is dropped, the `active` field is
        // drained and all of the `Waker`s are woken. Then, the queue inside of
        // the `Executor` is drained of all of its runnables. This ensures that
        // runnables are dropped and this precondition is satisfied.
        //
        // `schedule` is not `Send` nor `Sync`. As LocalExecutor is not
        // `Send`, the `Waker` is guaranteed// to only be used on the same thread
        // it was spawned on.
        //
        // `schedule` is `'static`, and thus will outlive all borrowed
        // variables in the future.
        let (runnable, task) = unsafe {
            Builder::new()
                .propagate_panic(true)
                .spawn_unchecked(|()| future, schedule)
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
        self.state().try_tick()
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
        self.state().tick().await;
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
        self.state().run(future).await
    }

    /// Returns a function that schedules a runnable task when it gets woken up.
    fn schedule(state: Rc<State>) -> impl Fn(Runnable) + 'static {
        move |runnable| {
            {
                // SAFETY: All UnsafeCell accesses to queue are tightly scoped, and because
                // `LocalExecutor` is !Send, there is no way to have concurrent access to the
                // values in `State`, including the queue field.
                let queue = unsafe { &mut *state.queue.get() };
                queue.push_front(runnable);
            }
            state.notify();
        }
    }

    /// Returns a pointer to the inner state.
    #[inline]
    pub(crate) fn state_ptr(&self) -> *const State {
        #[cold]
        fn alloc_state(cell: &Cell<*mut State>) -> *mut State {
            debug_assert!(cell.get().is_null());
            let state = Rc::new(State::new());
            let ptr = Rc::into_raw(state) as *mut State;
            cell.set(ptr);
            ptr
        }

        let mut ptr = self.state.get();
        if ptr.is_null() {
            ptr = alloc_state(&self.state);
        }
        ptr
    }

    /// Returns a reference to the inner state.
    #[inline]
    fn state(&self) -> &State {
        // SAFETY: So long as a LocalExecutor lives, it's state pointer will always be valid
        // when accessed through state_ptr.
        unsafe { &*self.state_ptr() }
    }

    // Clones the inner state Rc
    #[inline]
    fn state_as_rc(&self) -> Rc<State> {
        // SAFETY: So long as a LocalExecutor lives, it's state pointer will always be a valid
        // Rc when accessed through state_ptr.
        let rc = unsafe { Rc::from_raw(self.state_ptr()) };
        let clone = rc.clone();
        std::mem::forget(rc);
        clone
    }
}

impl Drop for LocalExecutor<'_> {
    fn drop(&mut self) {
        let ptr = *self.state.get_mut();
        if ptr.is_null() {
            return;
        }

        // SAFETY: As ptr is not null, it was allocated via Rc::new and converted
        // via Rc::into_raw in state_ptr.
        let state = unsafe { Rc::from_raw(ptr) };

        {
            // SAFETY: All UnsafeCell accesses to active are tightly scoped, and because
            // `LocalExecutor` is !Send, there is no way to have concurrent access to the
            // values in `State`, including the active field.
            let active = unsafe { &mut *state.active.get() };
            for w in active.drain() {
                w.wake();
            }
        }

        // SAFETY: All UnsafeCell accesses to queue are tightly scoped, and because
        // `LocalExecutor` is !Send, there is no way to have concurrent access to the
        // values in `State`, including the queue field.
        unsafe { &mut *state.queue.get() }.clear();
    }
}

impl<'a> Default for LocalExecutor<'a> {
    fn default() -> LocalExecutor<'a> {
        LocalExecutor::new()
    }
}

/// The state of a executor.
pub(crate) struct State {
    /// The global queue.
    pub(crate) queue: UnsafeCell<VecDeque<Runnable>>,

    /// A list of sleeping tickers.
    sleepers: UnsafeCell<Sleepers>,

    /// Currently active tasks.
    pub(crate) active: UnsafeCell<Slab<Waker>>,
}

impl State {
    /// Creates state for a new executor.
    pub(crate) const fn new() -> State {
        State {
            queue: UnsafeCell::new(VecDeque::new()),
            sleepers: UnsafeCell::new(Sleepers {
                count: 0,
                wakers: Vec::new(),
                free_ids: Vec::new(),
            }),
            active: UnsafeCell::new(Slab::new()),
        }
    }

    /// Notifies a sleeping ticker.
    #[inline]
    pub(crate) fn notify(&self) {
        // SAFETY: All UnsafeCell accesses to sleepers are tightly scoped, and because
        // `LocalExecutor` is !Send, there is no way to have concurrent access to the
        // values in `State`, including the sleepers field.
        let waker = unsafe { &mut *self.sleepers.get() }.notify();
        if let Some(w) = waker {
            w.wake();
        }
    }

    pub(crate) fn try_tick(&self) -> bool {
        // SAFETY: All UnsafeCell accesses to queue are tightly scoped, and because
        // `LocalExecutor` is !Send, there is no way to have concurrent access to the
        // values in `State`, including the queue field.
        let runnable = unsafe { &mut *self.queue.get() }.pop_back();
        match runnable {
            None => false,
            Some(runnable) => {
                // Run the task.
                runnable.run();
                true
            }
        }
    }

    pub(crate) async fn tick(&self) {
        Ticker::new(self).runnable().await.run();
    }

    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        // A future that runs tasks forever.
        let run_forever = async {
            loop {
                for _ in 0..200 {
                    // SAFETY: All UnsafeCell accesses to queue are tightly scoped, and because
                    // `LocalExecutor` is !Send, there is no way to have concurrent access to the
                    // values in `State`, including the queue field.
                    match unsafe { &mut *self.queue.get() }.pop_back() {
                        Some(runnable) => {
                            runnable.run();
                        }
                        None => break,
                    }
                }
                future::yield_now().await;
            }
        };

        // Run `future` and `run_forever` concurrently until `future` completes.
        future.or(run_forever).await
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
    ///    2a) Sleeping and unnotified.
    ///    2b) Sleeping and notified.
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
        match self.sleeping {
            // Move to sleeping state.
            0 => {
                // SAFETY: All UnsafeCell accesses to sleepers are tightly scoped, and because
                // `LocalExecutor` is !Send, there is no way to have concurrent access to the
                // values in `State`, including the sleepers field.
                let sleepers = unsafe { &mut *self.state.sleepers.get() };
                self.sleeping = sleepers.insert(waker);
            }

            // Already sleeping, check if notified.
            id => {
                // SAFETY: All UnsafeCell accesses to sleepers are tightly scoped, and because
                // `LocalExecutor` is !Send, there is no way to have concurrent access to the
                // values in `State`, including the sleepers field.
                let sleepers = unsafe { &mut *self.state.sleepers.get() };
                if !sleepers.update(id, waker) {
                    return false;
                }
            }
        }

        true
    }

    /// Moves the ticker into woken state.
    fn wake(&mut self) {
        if self.sleeping != 0 {
            // SAFETY: All UnsafeCell accesses to sleepers are tightly scoped, and because
            // `LocalExecutor` is !Send, there is no way to have concurrent access to the
            // values in `State`, including the sleepers field.
            let sleepers = unsafe { &mut *self.state.sleepers.get() };
            sleepers.remove(self.sleeping);
        }
        self.sleeping = 0;
    }

    /// Waits for the next runnable task to run, given a function that searches for a task.
    async fn runnable(&mut self) -> Runnable {
        future::poll_fn(|cx| {
            loop {
                // SAFETY: All UnsafeCell accesses to queue are tightly scoped, and because
                // `LocalExecutor` is !Send, there is no way to have concurrent access to the
                // values in `State`, including the queue field.
                match unsafe { &mut *self.state.queue.get() }.pop_back() {
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
            let notified = {
                // SAFETY: All UnsafeCell accesses to sleepers are tightly scoped, and because
                // `LocalExecutor` is !Send, there is no way to have concurrent access to the
                // values in `State`, including the sleepers field.
                let sleepers = unsafe { &mut *self.state.sleepers.get() };
                sleepers.remove(self.sleeping)
            };

            // If this ticker was notified, then notify another ticker.
            if notified {
                self.state.notify();
            }
        }
    }
}

/// Debug implementation for `Executor` and `LocalExecutor`.
fn debug_executor(
    executor: &LocalExecutor<'_>,
    name: &str,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    // Get a reference to the state.
    let ptr = executor.state.get();
    if ptr.is_null() {
        // The executor has not been initialized.
        struct Uninitialized;

        impl fmt::Debug for Uninitialized {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<uninitialized>")
            }
        }

        return f.debug_tuple(name).field(&Uninitialized).finish();
    }

    // SAFETY: If the state pointer is not null, it must have been
    // allocated properly by Rc::new and converted via Rc::into_raw
    // in state_ptr.
    let state = unsafe { &*ptr };

    debug_state(state, name, f)
}

/// Debug implementation for `Executor` and `LocalExecutor`.
pub(crate) fn debug_state(state: &State, name: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    /// Debug wrapper for the number of active tasks.
    struct ActiveTasks<'a>(&'a UnsafeCell<Slab<Waker>>);

    impl fmt::Debug for ActiveTasks<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // SAFETY: All UnsafeCell accesses to active are tightly scoped, and because
            // `LocalExecutor` is !Send, there is no way to have concurrent access to the
            // values in `State`, including the active field.
            let active = unsafe { &*self.0.get() };
            fmt::Debug::fmt(&active.len(), f)
        }
    }

    /// Debug wrapper for the sleepers.
    struct SleepCount<'a>(&'a UnsafeCell<Sleepers>);

    impl fmt::Debug for SleepCount<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // SAFETY: All UnsafeCell accesses to sleepers are tightly scoped, and because
            // `LocalExecutor` is !Send, there is no way to have concurrent access to the
            // values in `State`, including the sleepers field.
            let sleepers = unsafe { &*self.0.get() };
            fmt::Debug::fmt(&sleepers.count, f)
        }
    }

    f.debug_struct(name)
        .field("active", &ActiveTasks(&state.active))
        // SAFETY: All UnsafeCell accesses to queue are tightly scoped, and because
        // `LocalExecutor` is !Send, there is no way to have concurrent access to the
        // values in `State`, including the queue field.
        .field("global_tasks", &unsafe { &*state.queue.get() }.len())
        .field("sleepers", &SleepCount(&state.sleepers))
        .finish()
}
