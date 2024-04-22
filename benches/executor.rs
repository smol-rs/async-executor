use std::mem;
use std::thread::available_parallelism;

use async_executor::Executor;
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
    for (group_name, multithread) in [("single_thread", false), ("multi_thread", true)].iter() {
        let mut group = c.benchmark_group(group_name.to_string());

        group.bench_function("executor::spawn_one", |b| {
            run(
                || {
                    b.iter(|| {
                        future::block_on(async { EX.spawn(async {}).await });
                    });
                },
                *multithread,
            );
        });

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

        group.bench_function("executor::spawn_many_local", |b| {
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
        });

        group.bench_function("executor::spawn_recursively", |b| {
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
        });

        group.bench_function("executor::yield_now", |b| {
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
        });

        group.bench_function("executor::channels", |b| {
            run(
                || {
                    b.iter(move || {
                        future::block_on(async {
                            // Create channels.
                            let mut tasks = Vec::new();
                            let (first_send, first_recv) = async_channel::bounded(1);
                            let mut current_recv = first_recv;

                            for _ in 0..TASKS {
                                let (next_send, next_recv) = async_channel::bounded(1);
                                let current_recv = mem::replace(&mut current_recv, next_recv);

                                tasks.push(EX.spawn(async move {
                                    // Send a notification on to the next task.
                                    for _ in 0..STEPS {
                                        current_recv.recv().await.unwrap();
                                        next_send.send(()).await.unwrap();
                                    }
                                }));
                            }

                            for _ in 0..STEPS {
                                first_send.send(()).await.unwrap();
                                current_recv.recv().await.unwrap();
                            }

                            for task in tasks {
                                task.await;
                            }
                        });
                    });
                },
                *multithread,
            )
        });

        group.bench_function("executor::web_server", |b| {
            run(
                || {
                    b.iter(move || {
                        future::block_on(async {
                            let (db_send, db_recv) =
                                async_channel::bounded::<async_channel::Sender<_>>(TASKS / 5);
                            let mut db_rng = fastrand::Rng::with_seed(0x12345678);
                            let mut web_rng = db_rng.fork();

                            // This task simulates a database.
                            let db_task = EX.spawn(async move {
                                loop {
                                    // Wait for a new task.
                                    let incoming = match db_recv.recv().await {
                                        Ok(incoming) => incoming,
                                        Err(_) => break,
                                    };

                                    // Process the task. Maybe it takes a while.
                                    for _ in 0..db_rng.usize(..10) {
                                        future::yield_now().await;
                                    }

                                    // Send the data back.
                                    incoming.send(db_rng.usize(..)).await.ok();
                                }
                            });

                            // This task simulates a web server waiting for new tasks.
                            let server_task = EX.spawn(async move {
                                for i in 0..TASKS {
                                    // Get a new connection.
                                    if web_rng.usize(..=16) == 16 {
                                        future::yield_now().await;
                                    }

                                    let mut web_rng = web_rng.fork();
                                    let db_send = db_send.clone();
                                    let task = EX.spawn(async move {
                                        // Check if the data is cached...
                                        if web_rng.bool() {
                                            // ...it's in cache!
                                            future::yield_now().await;
                                            return;
                                        }

                                        // Otherwise we have to make a DB call or two.
                                        for _ in 0..web_rng.usize(STEPS / 2..STEPS) {
                                            let (resp_send, resp_recv) = async_channel::bounded(1);
                                            db_send.send(resp_send).await.unwrap();
                                            criterion::black_box(resp_recv.recv().await.unwrap());
                                        }

                                        // Send the data back...
                                        for _ in 0..web_rng.usize(3..16) {
                                            future::yield_now().await;
                                        }
                                    });

                                    task.detach();

                                    if i & 16 == 0 {
                                        future::yield_now().await;
                                    }
                                }
                            });

                            // Spawn and wait for it to stop.
                            server_task.await;
                            db_task.await;
                        });
                    })
                },
                *multithread,
            )
        });
    }
}

criterion_group!(benches, create, running_benches);

criterion_main!(benches);
