use crate::*;

use async_channel as channel;
use futures_util::future::{Either, FutureExt};
use futures_util::stream::{StreamExt, StreamFuture};

type ReadStream<T> = StreamHolder<T>;

pub(crate) enum ChannelKind<T>
where
    T: Stream,
{
    Value(T::Item),
    Connected,
    Disconnected,
    ChannelChange(ChannelChange<T>),
}

pub(crate) struct ChannelItem<T>
where
    T: Stream,
{
    pub(crate) id: StreamId,
    pub(crate) kind: ChannelKind<T>,
}

#[derive(Debug)]
pub(crate) struct Channel<T> {
    id: ChannelId,
    add_rx: channel::Receiver<ReadStream<T>>,
    stream_futures: FuturesUnordered<StreamFuture<ReadStream<T>>>,
}

impl<T> Channel<T>
where
    T: Stream,
{
    pub(crate) fn new(id: ChannelId) -> (channel::Sender<ReadStream<T>>, Self) {
        let (add_tx, add_rx) = channel::unbounded::<ReadStream<T>>();
        let stream_futures = FuturesUnordered::new();

        (
            add_tx,
            Self {
                add_rx,
                id,
                stream_futures,
            },
        )
    }

    pub(crate) async fn next(&mut self) -> ChannelItem<T>
    where
        T: Send + Sync + Unpin,
        T::Item: Send + Sync,
    {
        loop {
            // select on the stream-add channel and the FuturesUnordered
            if self.stream_futures.is_empty() {
                log::trace!("Channel({})::next() awaiting streams.", self.id);
                let added_stream_res = self.add_rx.next().await;
                // return the erorr if there was one
                return self.process_stream_add(added_stream_res);
            } else {
                log::trace!("Channel({})::next() awaiting streams or packets", self.id);
                futures_util::select! {
                    added_stream_res = self.add_rx.next().fuse() => {
                        return self.process_stream_add(added_stream_res);
                    }
                    stream_output = self.stream_futures.next().fuse() => {
                        log::trace!("Channel({}) processing stream data", self.id);
                        return self.process_stream_output(stream_output.unwrap()).await;
                    }
                }
            }
        }
    }

    fn process_stream_add(&mut self, added_stream: Option<ReadStream<T>>) -> ChannelItem<T>
    where
        T: Send + Sync + Unpin,
    {
        // Unwrapping result as it should not be possbile to have the other end of the channel
        // closed, we hold both ends.
        let added_stream = added_stream.expect("Add channel was closed for adding.");
        let stream_id = added_stream.id;

        log::debug!("Channel({}) adding StreamId({})", self.id, stream_id);

        if added_stream.stream.is_some() {
            // Add to the FuturesUnordered
            self.stream_futures.push(added_stream.into_future());
        } else {
            log::warn!(
                "Trying to enqueue dropped stream {} into channel {}",
                stream_id,
                self.id
            );
        }

        ChannelItem {
            id: stream_id,
            kind: ChannelKind::Connected,
        }
    }

    async fn process_stream_output(
        &self,
        (output, stream): (
            Option<Either<T::Item, ChannelChange<T>>>, // TryStream
            ReadStream<T>,
        ),
    ) -> ChannelItem<T>
    where
        T: Send + Sync + Unpin,
    {
        let stream_id = stream.id;

        if output.is_some() {
            // Add the stream back into the FuturesUnordered
            self.stream_futures.push(stream.into_future());
        }

        let kind = match output {
            None => ChannelKind::Disconnected,
            Some(Either::Right(channel_change)) => ChannelKind::ChannelChange(channel_change),
            Some(Either::Left(value)) => ChannelKind::Value(value),
        };

        ChannelItem {
            id: stream_id,
            kind,
        }
    }
}
