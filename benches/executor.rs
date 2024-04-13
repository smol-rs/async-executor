use std::thread::available_parallelism;

use async_executor::{Executor, StaticExecutor};
use criterion::{criterion_group, criterion_main, Criterion};
use futures_lite::{future, prelude::*};

const TASKS: usize = 300;
const STEPS: usize = 300;
const LIGHT_TASKS: usize = 25_000;

static EX: Executor<'_> = Executor::new();

fn run(f: impl FnOnce(), multithread: bool) {
    let limit = if multithread {
        available_parallelism().unwrap().get()
    } else {
        1
    };

    let (s, r) = async_channel::bounded::<()>(1);
    easy_parallel::Parallel::new()
        .each(0..limit, |_| future::block_on(EX.run(r.recv())))
        .finish(move || {
            let _s = s;
            f()
        });
}

fn run_static(executor: StaticExecutor, f: impl FnOnce(), multithread: bool) {
    let limit = if multithread {
        available_parallelism().unwrap().get()
    } else {
        1
    };

    let (s, r) = async_channel::bounded::<()>(1);
    easy_parallel::Parallel::new()
        .each(0..limit, |_| future::block_on(executor.run(r.recv())))
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

fn running_benches(c: &mut Criterion) {
    let static_executor = Executor::new().leak();
    for with_static in [false, true] {
        for (group_name, multithread) in [("single_thread", false), ("multi_thread", true)].iter() {
            let prefix = if with_static {
                "static_executor"
            } else {
                "executor"
            };
            let mut group = c.benchmark_group(group_name.to_string());

            group.bench_function(format!("{}::spawn_one", prefix), |b| {
                if with_static {
                    run_static(
                        static_executor,
                        || {
                            b.iter(|| {
                                future::block_on(async { static_executor.spawn(async {}).await });
                            });
                        },
                        *multithread,
                    );
                } else {
                    run(
                        || {
                            b.iter(|| {
                                future::block_on(async { EX.spawn(async {}).await });
                            });
                        },
                        *multithread,
                    );
                }
            });

            if !with_static {
                group.bench_function("executor::spawn_batch", |b| {
                    run(
                        || {
                            let mut handles = vec![];

                            b.iter(|| {
                                EX.spawn_many((0..250).map(|_| future::yield_now()), &mut handles);
                            });

                            handles.clear();
                        },
                        *multithread,
                    )
                });
            }

            group.bench_function(format!("{}::spawn_many_local", prefix), |b| {
                if with_static {
                    run_static(
                        static_executor,
                        || {
                            b.iter(move || {
                                future::block_on(async {
                                    let mut tasks = Vec::new();
                                    for _ in 0..LIGHT_TASKS {
                                        tasks.push(static_executor.spawn(async {}));
                                    }
                                    for task in tasks {
                                        task.await;
                                    }
                                });
                            });
                        },
                        *multithread,
                    );
                } else {
                    run(
                        || {
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
                        },
                        *multithread,
                    );
                }
            });

            group.bench_function(format!("{}::spawn_recursively", prefix), |b| {
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

                #[allow(clippy::manual_async_fn)]
                fn go_static(
                    executor: StaticExecutor,
                    i: usize,
                ) -> impl Future<Output = ()> + Send + 'static {
                    async move {
                        if i != 0 {
                            executor
                                .spawn(async move {
                                    let fut = go_static(executor, i - 1).boxed();
                                    fut.await;
                                })
                                .await;
                        }
                    }
                }

                if with_static {
                    run_static(
                        static_executor,
                        || {
                            b.iter(move || {
                                future::block_on(async {
                                    let mut tasks = Vec::new();
                                    for _ in 0..TASKS {
                                        tasks.push(
                                            static_executor
                                                .spawn(go_static(static_executor, STEPS)),
                                        );
                                    }
                                    for task in tasks {
                                        task.await;
                                    }
                                });
                            });
                        },
                        *multithread,
                    );
                } else {
                    run(
                        || {
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
                        },
                        *multithread,
                    );
                }
            });

            group.bench_function(format!("{}::yield_now", prefix), |b| {
                if with_static {
                    run_static(
                        static_executor,
                        || {
                            b.iter(move || {
                                future::block_on(async {
                                    let mut tasks = Vec::new();
                                    for _ in 0..TASKS {
                                        tasks.push(static_executor.spawn(async move {
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
                        },
                        *multithread,
                    );
                } else {
                    run(
                        || {
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
                        },
                        *multithread,
                    );
                }
            });
        }
    }
}

criterion_group!(benches, create, running_benches);

criterion_main!(benches);
