use futures::prelude::*;
use futures::task::{AtomicWaker, Context, Poll};

use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

#[derive(Debug)]
struct Inner {
    waker: AtomicWaker,
    set: AtomicBool,
}

#[derive(Clone, Debug)]
pub(crate) struct HaltRead {
    inner: Arc<Inner>,
}

impl HaltRead {
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) fn signal(&self) {
        tracing::trace!("setting atomic bool, triggering waker");
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();
    }

    #[tracing::instrument(level = "trace", skip(read))]
    pub(crate) fn wrap<St>(read: St) -> (Self, HaltAsyncRead<St>)
    where
        St: Stream,
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
            },
        )
    }
}

#[derive(Debug)]
pub(crate) struct HaltAsyncRead<St> {
    inner: Arc<Inner>,
    read: Option<St>,
}
impl<St> HaltAsyncRead<St>
where
    St: Stream,
{
    #[tracing::instrument(level = "trace", skip(self))]
    fn shutdown(&mut self) -> Poll<Option<St::Item>> {
        match self.read {
            None => {
                tracing::error!("stream already shutdown");
            }
            Some(_) => {
                let _ = self.read.take();
            }
        }

        Poll::Ready(None)
    }
}

impl<St> Unpin for HaltAsyncRead<St> where St: Stream + Unpin {}
impl<St> Stream for HaltAsyncRead<St>
where
    St: Stream + Unpin,
{
    type Item = St::Item;

    #[tracing::instrument(level = "trace", skip(self, ctx))]
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            tracing::trace!("pre-waker shutdown");
            return self.shutdown();
        }

        tracing::trace!("waker registration");
        self.inner.waker.register(ctx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            tracing::trace!("shutting down");
            self.shutdown()
        } else {
            // is only ever Some() here because inner.set being true
            // causes self.read to become none, and we take the other
            // branches.
            tracing::trace!("self.read.poll_read()");
            Pin::new(&mut self.read.as_mut().unwrap()).poll_next(ctx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use tokio_util::codec::length_delimited::LengthDelimitedCodec;

    use std::io::Cursor;

    #[tokio::test(basic_scheduler)]
    async fn halt() {
        //crate::tests::init_logging();

        // Stream of u8, from 0 to 15
        let cursor: Cursor<Vec<u8>> = Cursor::new((0..16).into_iter().collect());
        let (reader, _writer) = tokio::io::split(cursor);
        let framed_reader = LengthDelimitedCodec::builder()
            .length_field_length(1)
            .new_read(reader);

        let (halt, mut reader) = HaltRead::wrap(framed_reader);

        // Zero bytes,
        assert_eq!(Bytes::from(vec![]), reader.next().await.unwrap().unwrap());

        // 1 byte, value of 2
        assert_eq!(
            Bytes::from(vec![2_u8]),
            reader.next().await.unwrap().unwrap()
        );

        // Shut down the read stream
        halt.signal();

        // Check that we can't read while waitng for the writer
        assert!(reader.next().await.is_none());

        // check that reading has stopped
        assert!(reader.next().await.is_none());

        // Shut down the read stream
        halt.signal();

        // Ensure the double shutdown error is returned
        assert!(reader.next().await.is_none());
    }
}
