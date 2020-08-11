use async_channel as channel;
use futures_util::future::Either;
use futures_util::stream::{Stream, StreamExt};
use futures_util::task::{AtomicWaker, Context, Poll};
use pin_project_lite::pin_project;

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
pub(crate) struct StreamHolderControl<T> {
    inner: Arc<Inner>,
    return_channel: channel::Receiver<T>,
}

impl<T> StreamHolderControl<T> {
    pub(crate) async fn return_stream(&self) -> Option<T> {
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();

        match self.return_channel.recv().await {
            Err(err) => {
                log::error!("return_stream failed to receive stream: {:?}", err);
                None
            }
            Ok(stream) => Some(stream),
        }
    }

    pub(crate) fn wrap<Id>(stream_id: Id, stream: T) -> (Self, StreamHolder<T, Id>) {
        let (tx, rx) = channel::bounded(1);
        let inner = Arc::new(Inner {
            set: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });
        (
            Self {
                inner: Arc::clone(&inner),
                return_channel: rx,
            },
            StreamHolder {
                stream_id,
                inner,
                return_channel: tx,
                stream: Some(stream),
            },
        )
    }
}

pin_project! {
    #[derive(Debug)]
    pub(crate) struct StreamHolder<T, Id> {
        pub stream_id: Id,
        #[pin]
        pub stream: Option<T>,
        inner: Arc<Inner>,
        return_channel: channel::Sender<T>,
    }
}

impl<T, Id> StreamHolder<T, Id>
where
    T: Stream + Unpin,
{
    fn return_stream(self: Pin<&mut Self>) -> Poll<Option<T::Item>> {
        let this = self.project();

        let stream: Option<T> = Option::take(this.stream.get_mut());

        match stream {
            None => Poll::Ready(None),
            Some(stream) => {
                if let Err(err) = this.return_channel.try_send(stream) {
                    log::error!(
                        "Could not return the stream, return_channel failed: {:?}",
                        err
                    );
                }
                Poll::Ready(None)
            }
        }
    }
}

impl<T, Id> Stream for StreamHolder<T, Id>
where
    T: Stream + Unpin,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            return Self::return_stream(self);
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            Self::return_stream(self)
        } else {
            let this = self.project();
            // is only ever Some() here because inner.set being true
            // causes self.read to become none, and we take the other
            // branches.
            this.stream
                .as_pin_mut()
                .expect("Stream should exist, haven't shut down yet.")
                .poll_next(cx)
        }
    }
}

/*
FIXME: put some tests back
#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::StreamExt;

    #[tokio::test(basic_scheduler)]
    async fn move_stream() {
        // Stream of u8, from 0 to 15
        let numbers = stream::iter(1_u8..=32);
        let (move_tx, move_rx) = oneshot::channel();
        let (control, mut mover) = StreamDropControl::wrap(numbers, move_tx);
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
*/
