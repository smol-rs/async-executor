/// A worker in a work-stealing executor.
///
/// This is just a ticker that also has an associated local queue for improved cache locality.
#[derive(Debug)]
pub struct Runner {
    /// The executor state.
    state: Arc<State>,

    /// Inner ticker.
    ticker: Ticker,

    /// The local queue.
    local: Rc<Worker<Runnable>>,

    /// Bumped every time a runnable task is found.
    ticks: AtomicUsize,

    /// Worker ID.
    worker_id: usize,
}

impl Runner {
    /// Creates a runner and registers it in the executor state.
    pub fn new(state: Arc<State>) -> Runner {
        let local = Rc::new(Worker::new_lifo());
        let worker_id = state.local_queues.write().insert(local.stealer());

        Runner {
            state: state.clone(),
            ticker: Ticker::new(state),
            local,
            ticks: AtomicUsize::new(0),
            worker_id,
        }
    }

    /// Waits for the next runnable task to run.
    pub async fn runnable(&self) -> Runnable {
        let runnable = self
            .ticker
            .runnable_with(|| {
                self.local.pop().or_else(|| {
                    std::iter::repeat_with(|| {
                        self.state
                            .queue
                            .steal_batch_and_pop(&self.local)
                            .or_else(|| {
                                self.state
                                    .local_queues
                                    .read()
                                    .iter()
                                    .filter(|(id, _)| *id != self.worker_id)
                                    .map(|s| s.1.steal())
                                    .collect()
                            })
                    })
                    .find(|s| !s.is_retry())
                    .and_then(|s| s.success())
                })
            })
            .await;

        // // Bump the tick counter.
        // let ticks = self.ticks.fetch_add(1, Ordering::SeqCst);

        // if ticks % 64 == 0 {
        //     // Steal tasks from the global queue to ensure fair task scheduling.
        //     steal(&self.state.queue, &self.local);
        // }

        runnable
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        // Remove the local queue.
        self.state
            .local_queues
            .write()
            .retain(|i, _| i != self.worker_id);

        // Re-schedule remaining tasks in the local queue.
        while let Some(r) = self.local.pop() {
            r.schedule();
        }
    }
}
