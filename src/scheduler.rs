use std::collections::VecDeque;
use std::future::Future;
use std::mem;
use std::sync::Arc;
use std::task::{Poll, Waker};

use async_task::Runnable;
use futures_lite::{future, pin};
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
    pub(crate) unsafe fn spawn_unchecked<T>(
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

    /// Creates a ticker.
    pub(crate) fn ticker(self: &Scheduler) -> Ticker<'_> {
        Ticker {
            sched: self,
            sleeping: None,
        }
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
pub(crate) struct Ticker<'a> {
    /// The scheduler.
    sched: &'a Scheduler,

    /// Sleeper ID and index in the wakers list when in sleeping state.
    ///
    /// States a ticker can be in:
    /// 1) Woken.
    /// 2a) Sleeping and unnotified.
    /// 2b) Sleeping and notified.
    sleeping: Option<(u64, usize)>,
}

impl Ticker<'_> {
    /// Attempts to run a task if at least one is scheduled.
    pub(crate) fn try_tick(&self) -> bool {
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
    pub(crate) async fn tick(&mut self) {
        let runnable = future::poll_fn(|cx| {
            let mut state = self.sched.state.lock();

            match state.queue.pop_front() {
                None => {
                    // Move to sleeping and unnotified state.
                    match self.sleeping {
                        None => {
                            let id = state.id_gen;
                            state.id_gen += 1;

                            self.sleeping = Some((id, state.wakers.len()));
                            state.sleeping += 1;

                            if state.sleeping > state.wakers.len() {
                                state.wakers.push((id, cx.waker().clone()));
                            } else {
                                drop(state);
                                cx.waker().wake_by_ref();
                            }
                        }
                        Some((id, index)) => {
                            if let Some((w_id, w)) = state.wakers.get_mut(index) {
                                if *w_id == id {
                                    if !w.will_wake(cx.waker()) {
                                        *w = cx.waker().clone();
                                    }
                                    return Poll::Pending;
                                }
                            }

                            self.sleeping = Some((id, state.wakers.len()));
                            state.wakers.push((id, cx.waker().clone()));
                        }
                    }

                    Poll::Pending
                }
                Some(r) => {
                    // Wake up.
                    if let Some((id, index)) = self.sleeping.take() {
                        state.sleeping -= 1;

                        if let Some((w_id, _)) = state.wakers.get(index) {
                            if *w_id == id {
                                state.wakers.remove(index);
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
        if let Some((id, index)) = self.sleeping {
            let mut state = self.sched.state.lock();
            state.sleeping -= 1;

            if let Some((w_id, _)) = state.wakers.get(index) {
                if *w_id == id {
                    state.wakers.remove(index);
                }
            }

            // If this ticker was notified, then notify another ticker.
            notify(state);
        }
    }
}
