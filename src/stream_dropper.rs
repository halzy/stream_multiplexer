use crate::*;

use futures_util::stream::Stream;
use futures_util::task::{AtomicWaker, Context, Poll};
use pin_project_lite::pin_project;

use std::pin::Pin;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

#[derive(Debug)]
struct Inner {
    change_channel: AtomicBool,
    next_channel: AtomicUsize,
    waker: AtomicWaker,
    set: AtomicBool,
}

#[derive(Clone, Debug)]
pub(crate) struct StreamDropControl {
    inner: Arc<Inner>,
}

// FIXME: Rename, does more than just drop now, also changes channels.
impl StreamDropControl {
    pub(crate) fn change_channel(&self, channel_id: ChannelId) {
        self.inner.set.store(true, Relaxed);
        self.inner.change_channel.store(true, Relaxed);
        self.inner.next_channel.store(channel_id, Relaxed);
        self.inner.waker.wake();
    }

    pub(crate) fn drop_stream(&self) {
        self.inner.set.store(true, Relaxed);
        self.inner.waker.wake();
    }

    pub(crate) fn wrap<T>(
        id: StreamId,
        stream: T,
        channel_change_tx: async_channel::Sender<ChannelChange<T>>,
    ) -> (Self, StreamDropper<T>) {
        let inner = Arc::new(Inner {
            change_channel: AtomicBool::new(false),
            next_channel: AtomicUsize::new(0),
            set: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            StreamDropper {
                channel_change_tx,
                id,
                inner,
                stream: Some(stream),
            },
        )
    }
}

pin_project! {
    #[derive(Debug)]
    pub(crate) struct StreamDropper<T> {
        pub id: StreamId,
        #[pin]
        pub stream: Option<T>,
        inner: Arc<Inner>,
        channel_change_tx: async_channel::Sender<ChannelChange<T>>,
    }
}

impl<T> StreamDropper<T>
where
    T: Stream + Unpin,
{
    fn drop_stream(self: Pin<&mut Self>) -> Poll<Option<T::Item>> {
        let this = self.project();

        let stream: Option<T> = Option::take(this.stream.get_mut());

        match stream {
            None => Poll::Ready(None),
            Some(stream) => {
                if this.inner.change_channel.load(Relaxed) {
                    // Either drop or put the stream into the async_channel
                    let next_channel_id = this.inner.next_channel.load(Relaxed);
                    let channel_change = ChannelChange {
                        next_channel_id,
                        stream_id: *this.id,
                        stream,
                    };
                    if let Err(error) = this.channel_change_tx.try_send(channel_change) {
                        log::error!("Failed to send to change channel stream: {:?}", error);
                    }
                }
                Poll::Ready(None)
            }
        }
    }
}

impl<T> Stream for StreamDropper<T>
where
    T: Stream + Unpin,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // quick check to avoid registration if already done.
        if self.inner.set.load(Relaxed) {
            return Self::drop_stream(self);
        }

        self.inner.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.inner.set.load(Relaxed) {
            Self::drop_stream(self)
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
