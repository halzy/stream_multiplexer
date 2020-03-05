use futures::prelude::*;
use futures::task::{AtomicWaker, Context, Poll};
use tokio::sync::oneshot;

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
pub(crate) struct StreamMoverControl {
    inner: Arc<Inner>,
}

impl StreamMoverControl {
    pub(crate) fn signal(&self) {
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();
    }

    #[tracing::instrument(level = "trace", skip(stream, move_channel))]
    pub(crate) fn wrap<S>(stream: S, move_channel: oneshot::Sender<S>) -> (Self, StreamMover<S>) {
        let inner = Arc::new(Inner {
            waker: AtomicWaker::new(),
            set: AtomicBool::new(false),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            StreamMover {
                inner,
                stream: Some(stream),
                move_channel: Some(move_channel),
            },
        )
    }
}

#[derive(Debug)]
pub(crate) struct StreamMover<St> {
    inner: Arc<Inner>,
    stream: Option<St>,
    move_channel: Option<oneshot::Sender<St>>,
}
impl<St> StreamMover<St>
where
    St: Stream,
{
    pub(crate) fn stream(&self) -> Option<&St> {
        self.stream.as_ref()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn send_stream(&mut self) -> Poll<Option<St::Item>> {
        match self.stream {
            None => Poll::Ready(None),
            Some(_) => {
                tracing::trace!("giving up stream");
                let move_channel = self.move_channel.take().unwrap();
                let stream = self.stream.take().unwrap();
                if let Err(_) = move_channel.send(stream) {
                    tracing::error!("Could not send stream, was the receiver deallocated?");
                }
                Poll::Ready(None)
            }
        }
    }
}
impl<St> Unpin for StreamMover<St> {}
impl<St> Stream for StreamMover<St>
where
    St: Stream + Unpin,
{
    type Item = St::Item;

    #[tracing::instrument(level = "trace", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<St::Item>> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            return self.send_stream();
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            self.send_stream()
        } else {
            // is only ever Some() here because inner.set being true
            // causes self.read to become none, and we take the other
            // branches.
            self.stream
                .as_mut()
                .expect("Stream should exist, haven't shut down yet.")
                .poll_next_unpin(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::StreamExt;

    #[tokio::test(basic_scheduler)]
    async fn move_stream() {
        // Stream of u8, from 0 to 15
        let numbers = stream::iter(1_u8..=32);
        let (move_tx, move_rx) = oneshot::channel();
        let (control, mut mover) = StreamMoverControl::wrap(numbers, move_tx);
        assert_eq!(1_u8, mover.next().await.unwrap());

        // Signal to move  the stream out of the wrapper
        control.signal();

        // Check that we can't read while waitng for stream
        assert!(mover.next().await.is_none());

        // Get the stream out of the oneshot channel
        let mut numbers = move_rx.await.unwrap();
        assert_eq!(2_u8, numbers.next().await.unwrap());

        // check that reading has stopped
        assert!(mover.next().await.is_none());

        // Shut down the read stream (again, make sure it's not panicing)
        control.signal();

        // Ensure the double shutdown error is returned
        assert!(mover.next().await.is_none());
    }
}
