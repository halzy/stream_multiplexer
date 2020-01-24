use futures::task::{AtomicWaker, Context, Poll};
use tokio::io::{AsyncRead, Result as TioResult};

use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

struct Inner {
    waker: AtomicWaker,
    set: AtomicBool,
}

#[derive(Clone)]
pub struct HaltRead {
    inner: Arc<Inner>,
}

impl HaltRead {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                waker: AtomicWaker::new(),
                set: AtomicBool::new(false),
            }),
        }
    }

    pub fn signal(&self) {
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();
    }

    pub fn wrap<T>(&self, read: T) -> HaltAsyncRead<T>
    where
        T: AsyncRead,
    {
        HaltAsyncRead {
            inner: Arc::clone(&self.inner),
            read,
        }
    }
}

pub struct HaltAsyncRead<T> {
    inner: Arc<Inner>,
    read: T,
}
impl<T> Unpin for HaltAsyncRead<T> {}
impl<T: AsyncRead> AsyncRead for HaltAsyncRead<T>
where
    T: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<TioResult<usize>> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            return Poll::Ready(Ok(0)); // returning 0 will signal EOF and close the connection.
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            Poll::Ready(Ok(0)) // returning 0 will signal EOF and close the connection.
        } else {
            Pin::new(&mut self.read).poll_read(cx, buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::AsyncReadExt as _;

    use std::io::Cursor;

    #[tokio::test(basic_scheduler)]
    async fn halt() {
        // Stream of u8, from 0 to 15
        let data: Vec<u8> = (0..16).into_iter().collect();

        let halt = HaltRead::new();
        let mut reader = halt.wrap(Cursor::new(data));

        assert_eq!(0, reader.read_u8().await.unwrap());
        assert_eq!(1, reader.read_u8().await.unwrap());

        // Shut down the read stream
        halt.signal();

        assert!(reader.read_u8().await.is_err());
    }
}
