use async_executor::Executor;

#[test]
fn test_basic_scope() {
    let ex = Executor::new();
    let data = std::sync::Mutex::new(0i32);
    futures_lite::future::block_on(ex.run(async {
        ex.scope(|scope| {
            scope.spawn(async {
                *data.lock().unwrap() += 1;
            });
            scope.spawn(async {
                *data.lock().unwrap() += 4;
            });
        })
        .await;
    }));
    assert_eq!(*data.lock().unwrap(), 5);
}
