use std::future::Future;

use async_executor::Executor;
use criterion::{criterion_group, criterion_main, Criterion};
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

fn create(c: &mut Criterion) {
    c.bench_function("executor::create", |b| {
        b.iter(|| {
            let ex = Executor::new();
            let task = ex.spawn(async {});
            future::block_on(ex.run(task));
        })
    });
}

fn spawn_one(c: &mut Criterion) {
    c.bench_function("executor::spawn_one", |b| {
        run(|| {
            b.iter(|| {
                future::block_on(async { EX.spawn(async {}).await });
            });
        });
    });
}

fn spawn_many(c: &mut Criterion) {
    c.bench_function("executor::spawn_many_local", |b| {
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
    });
}

fn spawn_recursively(c: &mut Criterion) {
    c.bench_function("executor::spawn_recursively", |b| {
        #[allow(clippy::manual_async_fn)]
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
    });
}

fn yield_now(c: &mut Criterion) {
    c.bench_function("executor::yield_now", |b| {
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
    });
}

criterion_group!(
    benches,
    create,
    spawn_one,
    spawn_many,
    spawn_recursively,
    yield_now,
);

criterion_main!(benches);
