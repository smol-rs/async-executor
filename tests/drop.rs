use std::panic::catch_unwind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::{Poll, Waker};

use async_executor::Executor;
use futures_lite::future;
use once_cell::sync::Lazy;

#[test]
fn smoke() {
    static DROP: AtomicUsize = AtomicUsize::new(0);
    static WAKER: Lazy<Mutex<Option<Waker>>> = Lazy::new(|| Default::default());

    let ex = Executor::new();

    let task = ex.spawn(async {
        let _guard = CallOnDrop(|| {
            DROP.fetch_add(1, Ordering::SeqCst);
        });

        future::poll_fn(|cx| {
            *WAKER.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending::<()>
        })
        .await;
    });

    future::block_on(ex.tick());
    assert!(WAKER.lock().unwrap().is_some());
    assert_eq!(DROP.load(Ordering::SeqCst), 0);

    drop(ex);
    assert_eq!(DROP.load(Ordering::SeqCst), 1);

    assert!(catch_unwind(|| future::block_on(task)).is_err());
    assert_eq!(DROP.load(Ordering::SeqCst), 1);
}

struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
