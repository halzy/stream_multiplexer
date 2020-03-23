use crate::*;
use futures::prelude::*;
use futures::stream::Fuse;

use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) struct SendAllOwn<Si, St>
where
    St: Stream,
{
    sender: Sender<Si>,
    stream: Option<Fuse<St>>,
    buffered: Option<St::Item>,
    should_flush: bool,
}

impl<Si, St> SendAllOwn<Si, St>
where
    St: Stream,
{
    pub(crate) fn new(sender: Sender<Si>, stream: St) -> Self {
        Self {
            sender,
            stream: Some(stream.fuse()),
            buffered: None,
            should_flush: false,
        }
    }

    pub(crate) fn inner(&self) -> &Sender<Si> {
        &self.sender
    }
}

impl<Si, St> SendAllOwn<Si, St>
where
    Si: Sink<St::Item> + Unpin,
    St: Stream,
{
    fn try_start_send(
        &mut self,
        cx: &mut Context<'_>,
        item: St::Item,
    ) -> Poll<Result<(), Si::Error>> {
        debug_assert!(self.buffered.is_none());
        match self.sender.sink() {
            Some(mut sink) => match Pin::new(&mut sink).poll_ready(cx)? {
                Poll::Ready(()) => {
                    // We only want to flush if we have sent data
                    self.should_flush = true;

                    Poll::Ready(Pin::new(&mut sink).start_send(item))
                }
                Poll::Pending => {
                    self.buffered.replace(item);
                    Poll::Pending
                }
            },
            None => Poll::Ready(Ok(())),
        }
    }
}

impl<Si, St> Unpin for SendAllOwn<Si, St> where St: Stream {}

impl<Si, St> Stream for SendAllOwn<Si, St>
where
    Si: Sink<St::Item> + Unpin,
    St: Stream + Unpin,
{
    type Item = Result<(), Si::Error>;

    #[tracing::instrument(level = "trace", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        tracing::trace!("Starting poll");
        if this.stream.is_none() {
            panic!("Future is complete.");
        }

        // If we've got an item buffered already, we need to write it to the
        // sink before we can do anything else
        if let Some(item) = this.buffered.take() {
            tracing::trace!("have buffered item");
            futures::ready!(this.try_start_send(cx, item))?;
        }

        loop {
            tracing::trace!("starting loop");
            match this.stream.as_mut().unwrap().poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    tracing::trace!("pulled item from stream");
                    futures::ready!(this.try_start_send(cx, item))?;
                }
                Poll::Ready(None) => {
                    tracing::trace!("stream has None");
                    if let Some(mut sink) = this.sender.sink() {
                        if this.should_flush {
                            this.should_flush = false;
                            futures::ready!(Pin::new(&mut sink).poll_flush(cx))?;
                        }
                    }
                    return Poll::Ready(Some(Ok(())));
                }
                Poll::Pending => {
                    tracing::trace!("Stream is pending");
                    if let Some(mut sink) = this.sender.sink() {
                        if this.should_flush {
                            this.should_flush = false;
                            futures::ready!(Pin::new(&mut sink).poll_flush(cx))?;
                        }
                    }
                    return Poll::Ready(Some(Ok(())));
                    //return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::channel::mpsc;

    #[tokio::test]
    async fn simple() {
        // crate::tests::init_logging();

        let (tx, rx) = mpsc::channel::<u8>(10);
        let (mut sender, mut reader) = crate::tests::sender_reader(tx, rx);

        sender.set_stream_id(42);
        reader.set_stream_id(42);

        let (mut tx, rx) = mpsc::channel(8);
        let send_all = SendAllOwn::new(sender, rx);

        tx.send(1_u8).await.unwrap();
        tx.send(2_u8).await.unwrap();
        tx.send(3_u8).await.unwrap();

        let (result, _send_all) = send_all.into_future().await;
        assert!(result.unwrap().is_ok());

        assert_eq!(&1_u8, reader.next().await.unwrap().value().unwrap());
        assert_eq!(&2_u8, reader.next().await.unwrap().value().unwrap());
        assert_eq!(&3_u8, reader.next().await.unwrap().value().unwrap());
    }

    #[tokio::test]
    async fn extract() {
        //crate::tests::init_logging();

        let (tx, rx) = mpsc::channel::<u8>(10);
        let (mut sender, mut reader) = crate::tests::sender_reader(tx, rx);

        sender.set_stream_id(42);
        reader.set_stream_id(42);

        let (mut tx, rx) = mpsc::channel(8);
        let send_all = SendAllOwn::new(sender, rx);

        tx.send(1_u8).await.unwrap();
        let (result, send_all) = send_all.into_future().await;
        assert!(result.unwrap().is_ok());
        assert_eq!(&1_u8, reader.next().await.unwrap().value().unwrap());

        tx.send(2_u8).await.unwrap();
        let (result, _send_all) = send_all.into_future().await;
        assert!(result.unwrap().is_ok());
        assert_eq!(&2_u8, reader.next().await.unwrap().value().unwrap());
    }
}
