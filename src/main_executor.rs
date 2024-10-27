use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use event_listener::Event;

use crate::{Executor, LocalExecutor};

/// Wait for the executor to stop.
pub(crate) struct WaitForStop {
    /// Whether or not we need to stop.
    stopped: AtomicBool,

    /// Wait for the stop.
    events: Event,
}

impl WaitForStop {
    /// Create a new wait for stop.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            events: Event::new(),
        }
    }

    /// Wait for the event to stop.
    #[inline]
    pub(crate) async fn wait(&self) {
        loop {
            if self.stopped.load(Ordering::Relaxed) {
                return;
            }

            event_listener::listener!(&self.events => listener);

            if self.stopped.load(Ordering::Acquire) {
                return;
            }

            listener.await;
        }
    }

    /// Stop the waiter.
    #[inline]
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.events.notify_additional(usize::MAX);
    }
}

/// Something that can be set up as an executor.
pub trait MainExecutor: Sized {
    /// Create this type and pass it into `main`.
    fn with_main<T, F: FnOnce(&Self) -> T>(f: F) -> T;
}

impl MainExecutor for Arc<Executor<'_>> {
    #[inline]
    fn with_main<T, F: FnOnce(&Self) -> T>(f: F) -> T {
        let ex = Arc::new(Executor::new());
        with_thread_pool(&ex, || f(&ex))
    }
}

impl MainExecutor for Executor<'_> {
    #[inline]
    fn with_main<T, F: FnOnce(&Self) -> T>(f: F) -> T {
        let ex = Executor::new();
        with_thread_pool(&ex, || f(&ex))
    }
}

impl MainExecutor for Rc<LocalExecutor<'_>> {
    #[inline]
    fn with_main<T, F: FnOnce(&Self) -> T>(f: F) -> T {
        f(&Rc::new(LocalExecutor::new()))
    }
}

impl MainExecutor for LocalExecutor<'_> {
    fn with_main<T, F: FnOnce(&Self) -> T>(f: F) -> T {
        f(&LocalExecutor::new())
    }
}

/// Run a function that takes an `Executor` inside of a thread pool.
#[inline]
fn with_thread_pool<T>(ex: &Executor<'_>, f: impl FnOnce() -> T) -> T {
    let stopper = WaitForStop::new();

    // Create a thread for each CPU.
    thread::scope(|scope| {
        let num_threads = thread::available_parallelism().map_or(1, |num| num.get());
        for i in 0..num_threads {
            let ex = &ex;
            let stopper = &stopper;

            thread::Builder::new()
                .name(format!("smol-macros-{i}"))
                .spawn_scoped(scope, || {
                    async_io::block_on(ex.run(stopper.wait()));
                })
                .expect("failed to spawn thread");
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));

        stopper.stop();

        match result {
            Ok(value) => value,
            Err(err) => std::panic::resume_unwind(err),
        }
    })
}
