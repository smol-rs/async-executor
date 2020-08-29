//! An executor with task priorities.

use std::future::Future;
use std::thread;

use async_executor::{Executor, Task};
use futures_lite::{future, FutureExt};

/// Task priority.
#[repr(usize)]
#[derive(Debug, Clone, Copy)]
enum Priority {
    High = 0,
    Medium = 1,
    Low = 2,
}

/// An executor with task priorities.
///
/// Tasks with lower priorities only get polled when there are no tasks with higher priorities.
struct PriorityExecutor {
    ex: [Executor; 3],
}

impl PriorityExecutor {
    /// Creates a new executor.
    const fn new() -> PriorityExecutor {
        PriorityExecutor {
            ex: [Executor::new(), Executor::new(), Executor::new()],
        }
    }

    /// Spawns a task with the given priority.
    fn spawn<T: Send + 'static>(
        &self,
        priority: Priority,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T> {
        self.ex[priority as usize].spawn(future)
    }

    /// Runs the executor until the future completes.
    async fn run<T>(&self, future: impl Future<Output = T>) -> T {
        future
            .or(async {
                // Keep ticking inner executors forever.
                loop {
                    let t0 = self.ex[0].tick();
                    let t1 = self.ex[1].tick();
                    let t2 = self.ex[2].tick();

                    // Wait until one of the ticks completes, trying them in order from highest
                    // priority to lowest priority.
                    t0.or(t1).or(t2).await;
                }
            })
            .await
    }
}

fn main() {
    static EX: PriorityExecutor = PriorityExecutor::new();

    // Spawn a thread running the executor forever.
    thread::spawn(|| {
        let forever = future::pending::<()>();
        future::block_on(EX.run(forever));
    });

    let mut tasks = Vec::new();

    for _ in 0..20 {
        // Choose a random priority.
        let choice = [Priority::High, Priority::Medium, Priority::Low];
        let priority = choice[fastrand::usize(..choice.len())];

        // Spawn a task with this priority.
        tasks.push(EX.spawn(priority, async move {
            println!("{:?}", priority);
            future::yield_now().await;
            println!("{:?}", priority);
        }));
    }

    for task in tasks {
        future::block_on(task);
    }
}
