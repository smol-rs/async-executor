use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::future::Future;

use async_executor::{Executor, Task};
use futures_lite::{future, prelude::*};

const TASKS: usize = 300;
const STEPS: usize = 300;
const LIGHT_TASKS: usize = 25_000;

static EX: Executor<'_> = Executor::new();

fn run(f: impl FnOnce()) {
    let (s, r) = async_channel::bounded::<()>(1);
    easy_parallel::Parallel::new()
        .each(0..num_cpus::get(), |_| future::block_on(EX.run(r.recv())))
        .finish(move || {
            let _s = s;
            f()
        });
}

fn create(b: &mut criterion::Bencher) {
    b.iter(move || {
        let ex = Executor::new();
        let task = ex.spawn(async {});
        future::block_on(ex.run(task));
    });
}

fn spawn_one(b: &mut criterion::Bencher) {
    run(|| {
        b.iter(move || {
            future::block_on(async { EX.spawn(async {}).await });
        });
    });
}

fn spawn_many(b: &mut criterion::Bencher) {
    run(|| {
        b.iter(move || {
            future::block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..LIGHT_TASKS {
                    tasks.push(EX.spawn(async {}));
                }
                for task in tasks {
                    task.await;
                }
            });
        });
    });
}

fn spawn_recursively(b: &mut criterion::Bencher) {
    fn go(i: usize) -> impl Future<Output = ()> + Send + 'static {
        async move {
            if i != 0 {
                EX.spawn(async move {
                    let fut = go(i - 1).boxed();
                    fut.await;
                })
                .await;
            }
        }
    }

    run(|| {
        b.iter(move || {
            future::block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..TASKS {
                    tasks.push(EX.spawn(go(STEPS)));
                }
                for task in tasks {
                    task.await;
                }
            });
        });
    });
}

fn yield_now(b: &mut criterion::Bencher) {
    run(|| {
        b.iter(move || {
            future::block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..TASKS {
                    tasks.push(EX.spawn(async move {
                        for _ in 0..STEPS {
                            future::yield_now().await;
                        }
                    }));
                }
                for task in tasks {
                    task.await;
                }
            });
        });
    });
}

fn ping_pong(b: &mut criterion::Bencher) {
    const NUM_PINGS: usize = 1_000;

    let (send, recv) = async_channel::bounded::<async_oneshot::Sender<_>>(10);
    let _task: Task<Option<()>> = EX.spawn(async move {
        loop {
            let mut os = recv.recv().await.ok()?;
            os.send(0u8).ok()?;
        }
    });
    run(|| {
        b.iter(move || {
            future::block_on(async {
                for i in 0..NUM_PINGS {
                    let (os_send, os_recv) = async_oneshot::oneshot();
                    send.send(os_send).await.unwrap();
                    os_recv.await.unwrap();
                }
            });
        });
    });
}

fn context_switch_quiet(b: &mut criterion::Bencher) {
    let (send, mut recv) = async_channel::bounded::<usize>(1);
    let mut tasks: Vec<Task<Option<()>>> = vec![];
    for _ in 0..TASKS {
        let old_recv = recv.clone();
        let (new_send, new_recv) = async_channel::bounded(1);
        tasks.push(EX.spawn(async move {
            loop {
                new_send.send(old_recv.recv().await.ok()?).await.ok()?
            }
        }));
        recv = new_recv;
    }
    run(|| {
        b.iter(move || {
            future::block_on(async {
                send.send(1).await.unwrap();
                recv.recv().await.unwrap();
            });
        });
    });
}

fn context_switch_busy(b: &mut criterion::Bencher) {
    let (send, mut recv) = async_channel::bounded::<usize>(1);
    let mut tasks: Vec<Task<Option<()>>> = vec![];
    for num in 0..TASKS {
        let old_recv = recv.clone();
        let (new_send, new_recv) = async_channel::bounded(1);
        tasks.push(EX.spawn(async move {
            loop {
                // eprintln!("forward {}", num);
                new_send.send(old_recv.recv().await.ok()?).await.ok()?;
            }
        }));
        recv = new_recv;
    }
    for _ in 0..TASKS {
        tasks.push(EX.spawn(async move {
            loop {
                future::yield_now().await;
            }
        }))
    }
    run(|| {
        b.iter(move || {
            future::block_on(async {
                send.send(1).await.unwrap();
                recv.recv().await.unwrap();
            });
        });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("create", create);
    c.bench_function("spawn_one", spawn_one);
    c.bench_function("spawn_many", spawn_many);
    c.bench_function("yield_now", yield_now);
    c.bench_function("ping_pong", ping_pong);
    c.bench_function("context_switch_quiet", context_switch_quiet);
    c.bench_function("context_switch_busy", context_switch_busy);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
