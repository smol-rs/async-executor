use std::collections::VecDeque;
use std::future::Future;
use std::mem;
use std::sync::Arc;
use std::task::{Poll, Waker};

use async_task::Runnable;
use futures_lite::{future, pin, FutureExt};
use parking_lot::{Mutex, MutexGuard};
use vec_arena::Arena;

use crate::Task;

/// Scheduler shared by the executor and wakers.
#[derive(Debug, Default)]
pub(crate) struct Scheduler {
    state: Mutex<State>,
}

/// The state of a scheduler.
#[derive(Debug, Default)]
struct State {
    /// The global queue.
    queue: VecDeque<Runnable>,

    /// Currently active tasks.
    active: Arena<Waker>,

    /// Total number of idle (sleeping or notified) tickers.
    idle_count: usize,

    /// IDs and wakers of sleeping tickers.
    sleeping: Vec<(u64, Waker)>,

    /// ID generator.
    id_gen: u64,
}

impl Scheduler {
    /// Spawns a task onto the scheduler.
    pub(crate) unsafe fn spawn_unchecked<T>(
        self: &Arc<Scheduler>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        // Keep the task's waker in the scheduler during its lifetime.
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

    /// Attempts to run a task if at least one is scheduled.
    pub(crate) fn try_tick(&self) -> bool {
        let mut state = self.state.lock();

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

    /// Runs a single task.
    pub(crate) async fn tick(&self) {
        let mut ticker = Ticker {
            sched: self,
            idle: None,
        };
        ticker.tick().await;
    }

    /// Runs until the given future completes.
    pub async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        // A future that runs tasks forever.
        let run_forever = async {
            let mut ticker = Ticker {
                sched: self,
                idle: None,
            };
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

    /// Drops all unfinished tasks.
    pub(crate) fn destroy(self: &Arc<Scheduler>) {
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
    let waker = if state.sleeping.len() == state.idle_count {
        state.sleeping.pop().map(|item| item.1)
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

    /// When in idle state, holds the ID and index in the sleeping tickers list.
    idle: Option<(u64, usize)>,
}

impl Ticker<'_> {
    /// Waits for the next runnable task to run.
    async fn tick(&mut self) {
        let runnable = future::poll_fn(|cx| {
            let mut state = self.sched.state.lock();

            match state.queue.pop_front() {
                None => {
                    // TODO: Spinning optimization:
                    // 1. increment nmspinning
                    // 2. cx.waker().wake_by_ref()
                    // 3. return Poll::Pending

                    // Transition to idle (sleeping) state.
                    match self.idle {
                        None => {
                            // Generate an ID unique to this state transition.
                            let id = state.id_gen;
                            state.id_gen += 1;

                            self.idle = Some((id, state.sleeping.len()));
                            state.sleeping.push((id, cx.waker().clone()));
                            state.idle_count += 1;
                        }
                        Some((id, index)) => {
                            if let Some((w_id, w)) = state.sleeping.get_mut(index) {
                                if *w_id == id {
                                    if !w.will_wake(cx.waker()) {
                                        *w = cx.waker().clone();
                                    }
                                    return Poll::Pending;
                                }
                            }

                            self.idle = Some((id, state.sleeping.len()));
                            state.sleeping.push((id, cx.waker().clone()));
                        }
                    }

                    Poll::Pending
                }
                Some(r) => {
                    // Transition to running state.
                    if let Some((id, index)) = self.idle.take() {
                        state.idle_count -= 1;

                        if let Some((w_id, _)) = state.sleeping.get(index) {
                            if *w_id == id {
                                state.sleeping.remove(index);
                            }
                        }
                    }

                    // Notify another ticker now to pick up where this ticker left off, just in
                    // case running the task takes a long time.
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
        // If idle, decrement the idle count.
        if let Some((id, index)) = self.idle {
            let mut state = self.sched.state.lock();
            state.idle_count -= 1;

            // If sleeping, remove it from the sleeping list.
            if let Some((w_id, _)) = state.sleeping.get(index) {
                if *w_id == id {
                    state.sleeping.remove(index);
                    return;
                }
            }

            // If notified, pass the notification to another ticker.
            notify(state);
        }
    }
}
