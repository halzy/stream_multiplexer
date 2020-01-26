use futures::task::{AtomicWaker, Context, Poll};
use tokio::io::{
    AsyncRead, AsyncWrite, Error as TioError, ErrorKind as TioErrorKind, ReadHalf,
    Result as TioResult, WriteHalf,
};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use std::net::Shutdown;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

pub trait StreamShutdown {
    fn shutdown(&self) -> TioResult<()>;
}

impl StreamShutdown for TcpStream {
    fn shutdown(&self) -> TioResult<()> {
        self.shutdown(Shutdown::Read)
    }
}

struct Inner {
    waker: AtomicWaker,
    set: AtomicBool,
}

pub struct HaltRead {
    inner: Arc<Inner>,
}

impl HaltRead {
    pub fn signal(&self) {
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();
    }

    pub fn wrap<T>(
        read: ReadHalf<T>,
        writer: oneshot::Receiver<WriteHalf<T>>,
    ) -> (Self, HaltAsyncRead<T>)
    where
        T: AsyncRead + AsyncWrite,
    {
        let inner = Arc::new(Inner {
            waker: AtomicWaker::new(),
            set: AtomicBool::new(false),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            HaltAsyncRead {
                inner,
                read: Some(read),
                writer,
            },
        )
    }
}

pub struct HaltAsyncRead<T> {
    inner: Arc<Inner>,
    read: Option<ReadHalf<T>>,
    writer: oneshot::Receiver<WriteHalf<T>>,
}
impl<T> HaltAsyncRead<T>
where
    T: StreamShutdown,
{
    fn shutdown(&mut self) -> Poll<TioResult<usize>> {
        use tokio::sync::oneshot::error::TryRecvError;

        match self.read {
            None => Poll::Ready(Err(TioError::new(
                TioErrorKind::Other,
                "Double shutdown on stream.",
            ))),
            Some(_) => {
                // _ = reader, it's taken below
                match self.writer.try_recv() {
                    Err(TryRecvError::Empty) => {
                        // Return pending if we do not yet have the write half
                        Poll::Pending
                    }
                    Err(TryRecvError::Closed) => {
                        // Because we take() the reader below and guard against none above.
                        unreachable!()
                    }
                    Ok(writer) => {
                        let reader = self
                            .read
                            .take()
                            .expect("Reader should still exist, was checked above");
                        let stream = reader.unsplit(writer);
                        stream.shutdown()?;
                        Poll::Ready(Ok(0)) // returning 0 will signal EOF and close the connection.
                    }
                }
            }
        }
    }
}
impl<T> Unpin for HaltAsyncRead<T> {}
impl<T> AsyncRead for HaltAsyncRead<T>
where
    T: StreamShutdown + AsyncRead + AsyncWrite,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<TioResult<usize>> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            return self.shutdown();
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            self.shutdown()
        } else {
            // is only ever Some() here because inner.set being true
            // causes self.read to become none, and we take the other
            // branches.
            Pin::new(&mut self.read.as_mut().unwrap()).poll_read(cx, buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::AsyncReadExt as _;
    use tokio::io::ErrorKind;

    use futures::future::FutureExt;

    use std::io::Cursor;

    impl StreamShutdown for Cursor<Vec<u8>> {
        fn shutdown(&self) -> TioResult<()> {
            Ok(())
        }
    }

    #[tokio::test(basic_scheduler)]
    async fn halt() {
        // Stream of u8, from 0 to 15
        let cursor: Cursor<Vec<u8>> = Cursor::new((0..16).into_iter().collect());
        let (reader, writer) = tokio::io::split(cursor);

        let (stop_tx, stop_rx) = oneshot::channel();
        let (halt, mut reader) = HaltRead::wrap(reader, stop_rx);

        assert_eq!(0_u8, reader.read_u8().await.unwrap());
        assert_eq!(1_u8, reader.read_u8().await.unwrap());

        // Shut down the read stream
        halt.signal();

        // Check that we can't read while waitng for the writer
        assert!(reader.read_u8().now_or_never().is_none());

        // Send the writer to finish the shutdown
        stop_tx.send(writer).unwrap();

        // check that reading has stopped
        assert_eq!(
            reader.read_u8().await.unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );

        // Shut down the read stream
        halt.signal();

        // Ensure the double shutdown error is returned
        assert_eq!(reader.read_u8().await.unwrap_err().kind(), ErrorKind::Other);
    }
}
