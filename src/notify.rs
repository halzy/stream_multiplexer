use atomic_waker::AtomicWaker;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::task::Poll;

struct Inner {
    waker: AtomicWaker,
    set: AtomicBool,
}

pub struct Notifier {
    inner: Arc<Inner>,
}

impl Notifier {
    pub fn notify(&self) {
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();
    }
}

pub struct Notify {
    inner: Arc<Inner>,
}

impl Notify {
    pub fn new() -> (Notifier, Self) {
        let inner = Arc::new(Inner {
            set: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });

        (
            Notifier {
                inner: Arc::clone(&inner),
            },
            Self { inner },
        )
    }
}

impl Future for Notify {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            return Poll::Ready(());
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
