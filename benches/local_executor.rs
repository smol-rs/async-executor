use std::hint::black_box;
use std::mem;

use async_executor::{Executor, LocalExecutor, StaticLocalExecutor};
use criterion::{criterion_group, criterion_main, Criterion};
use futures_lite::{future, prelude::*};

const TASKS: usize = 300;
const STEPS: usize = 300;
const LIGHT_TASKS: usize = 25_000;

fn run(f: impl FnOnce(&'static LocalExecutor<'_>)) {
    f(Box::leak(Box::new(LocalExecutor::new())));
}

fn run_static(f: impl FnOnce(&'static StaticLocalExecutor)) {
    f(LocalExecutor::new().leak())
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
    for (prefix, with_static) in [("local_executor", false), ("static_local_executor", true)] {
        let mut group = c.benchmark_group("single_thread");

        group.bench_function(format!("{prefix}::spawn_one"), |b| {
            if with_static {
                run_static(|ex| {
                    b.iter(|| {
                        let task = ex.spawn(async {});
                        future::block_on(ex.run(task));
                    });
                });
            } else {
                run(|ex| {
                    b.iter(|| {
                        let task = ex.spawn(async {});
                        future::block_on(ex.run(task));
                    });
                });
            }
        });

        if !with_static {
            group.bench_function("executor::spawn_batch", |b| {
                run(|ex| {
                    let mut handles = vec![];

                    b.iter(|| {
                        ex.spawn_many((0..250).map(|_| future::yield_now()), &mut handles);
                        handles.clear();
                    });

                    handles.clear();
                })
            });
        }

        group.bench_function(format!("{prefix}::spawn_many_local"), |b| {
            if with_static {
                run_static(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let mut tasks = Vec::new();
                            for _ in 0..LIGHT_TASKS {
                                tasks.push(ex.spawn(async {}));
                            }
                            for task in tasks {
                                task.await;
                            }
                        }));
                    });
                });
            } else {
                run(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let mut tasks = Vec::new();
                            for _ in 0..LIGHT_TASKS {
                                tasks.push(ex.spawn(async {}));
                            }
                            for task in tasks {
                                task.await;
                            }
                        }));
                    });
                });
            }
        });

        group.bench_function(format!("{prefix}::spawn_recursively"), |b| {
            #[allow(clippy::manual_async_fn)]
            fn go(
                ex: &'static LocalExecutor<'static>,
                i: usize,
            ) -> impl Future<Output = ()> + 'static {
                async move {
                    if i != 0 {
                        ex.spawn(async move {
                            let fut = Box::pin(go(ex, i - 1));
                            fut.await;
                        })
                        .await;
                    }
                }
            }

            #[allow(clippy::manual_async_fn)]
            fn go_static(
                ex: &'static StaticLocalExecutor,
                i: usize,
            ) -> impl Future<Output = ()> + 'static {
                async move {
                    if i != 0 {
                        ex.spawn(async move {
                            let fut = Box::pin(go_static(ex, i - 1));
                            fut.await;
                        })
                        .await;
                    }
                }
            }

            if with_static {
                run_static(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let mut tasks = Vec::new();
                            for _ in 0..TASKS {
                                tasks.push(ex.spawn(go_static(ex, STEPS)));
                            }
                            for task in tasks {
                                task.await;
                            }
                        }));
                    });
                });
            } else {
                run(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let mut tasks = Vec::new();
                            for _ in 0..TASKS {
                                tasks.push(ex.spawn(go(ex, STEPS)));
                            }
                            for task in tasks {
                                task.await;
                            }
                        }));
                    });
                });
            }
        });

        group.bench_function(format!("{prefix}::yield_now"), |b| {
            if with_static {
                run_static(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let mut tasks = Vec::new();
                            for _ in 0..TASKS {
                                tasks.push(ex.spawn(async move {
                                    for _ in 0..STEPS {
                                        future::yield_now().await;
                                    }
                                }));
                            }
                            for task in tasks {
                                task.await;
                            }
                        }));
                    });
                });
            } else {
                run(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let mut tasks = Vec::new();
                            for _ in 0..TASKS {
                                tasks.push(ex.spawn(async move {
                                    for _ in 0..STEPS {
                                        future::yield_now().await;
                                    }
                                }));
                            }
                            for task in tasks {
                                task.await;
                            }
                        }));
                    });
                });
            }
        });

        group.bench_function(format!("{prefix}::channels"), |b| {
            if with_static {
                run_static(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            // Create channels.
                            let mut tasks = Vec::new();
                            let (first_send, first_recv) = async_channel::bounded(1);
                            let mut current_recv = first_recv;

                            for _ in 0..TASKS {
                                let (next_send, next_recv) = async_channel::bounded(1);
                                let current_recv = mem::replace(&mut current_recv, next_recv);

                                tasks.push(ex.spawn(async move {
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
                        }));
                    });
                })
            } else {
                run(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            // Create channels.
                            let mut tasks = Vec::new();
                            let (first_send, first_recv) = async_channel::bounded(1);
                            let mut current_recv = first_recv;

                            for _ in 0..TASKS {
                                let (next_send, next_recv) = async_channel::bounded(1);
                                let current_recv = mem::replace(&mut current_recv, next_recv);

                                tasks.push(ex.spawn(async move {
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
                        }));
                    });
                })
            }
        });

        group.bench_function(format!("{prefix}::web_server"), |b| {
            if with_static {
                run_static(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let (db_send, db_recv) =
                                async_channel::bounded::<async_channel::Sender<_>>(TASKS / 5);
                            let mut db_rng = fastrand::Rng::with_seed(0x12345678);
                            let mut web_rng = db_rng.fork();

                            // This task simulates a database.
                            let db_task = ex.spawn(async move {
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
                            let server_task = ex.spawn(async move {
                                for i in 0..TASKS {
                                    // Get a new connection.
                                    if web_rng.usize(..=16) == 16 {
                                        future::yield_now().await;
                                    }

                                    let mut web_rng = web_rng.fork();
                                    let db_send = db_send.clone();
                                    let task = ex.spawn(async move {
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
                                            black_box(resp_recv.recv().await.unwrap());
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
                        }));
                    })
                })
            } else {
                run(|ex| {
                    b.iter(move || {
                        future::block_on(ex.run(async {
                            let (db_send, db_recv) =
                                async_channel::bounded::<async_channel::Sender<_>>(TASKS / 5);
                            let mut db_rng = fastrand::Rng::with_seed(0x12345678);
                            let mut web_rng = db_rng.fork();

                            // This task simulates a database.
                            let db_task = ex.spawn(async move {
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
                            let server_task = ex.spawn(async move {
                                for i in 0..TASKS {
                                    // Get a new connection.
                                    if web_rng.usize(..=16) == 16 {
                                        future::yield_now().await;
                                    }

                                    let mut web_rng = web_rng.fork();
                                    let db_send = db_send.clone();
                                    let task = ex.spawn(async move {
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
                                            black_box(resp_recv.recv().await.unwrap());
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
                        }));
                    })
                })
            }
        });
    }
}

criterion_group!(benches, create, running_benches);

criterion_main!(benches);
