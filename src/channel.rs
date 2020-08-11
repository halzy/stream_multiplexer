use crate::*;

use async_channel as channel;
use futures_util::future::FutureExt;
use futures_util::stream::{StreamExt, StreamFuture};

#[derive(Debug)]
pub(crate) struct Channel<T, Id> {
    add_rx: channel::Receiver<StreamHolder<T, Id>>,
    stream_futures: FuturesUnordered<StreamFuture<StreamHolder<T, Id>>>,
}

impl<T, Id> Channel<T, Id>
where
    T: Stream,
{
    pub(crate) fn new() -> (channel::Sender<StreamHolder<T, Id>>, Self) {
        let (add_tx, add_rx) = channel::unbounded::<StreamHolder<T, Id>>();
        let stream_futures = FuturesUnordered::new();

        (
            add_tx,
            Self {
                add_rx,
                stream_futures,
            },
        )
    }

    pub(crate) async fn next(&mut self) -> StreamItem<T::Item, Id>
    where
        T: Send + Sync + Unpin,
        T::Item: Send + Sync,
        Id: std::fmt::Debug + Clone,
    {
        loop {
            // select on the stream-add channel and the FuturesUnordered
            if self.stream_futures.is_empty() {
                log::trace!("Channel()::next() awaiting streams.");
                let added_stream_res = self.add_rx.next().await;
                // return the erorr if there was one
                if let Some(value) = self.process_stream_add(added_stream_res) {
                    return value;
                }
            } else {
                log::trace!("Channel()::next() awaiting streams or packets");
                futures_util::select! {
                    added_stream_res = self.add_rx.next().fuse() => {
                        if let Some(value) = self.process_stream_add(added_stream_res) {
                            return value;
                        }
                    }
                    stream_output = self.stream_futures.next().fuse() => {
                        log::trace!("Channel() processing stream data");
                        if let Some(value) = self.process_stream_output(stream_output.unwrap()).await{
                            return value;
                        }
                    }
                }
            }
        }
    }

    fn process_stream_add(
        &mut self,
        added_stream: Option<StreamHolder<T, Id>>,
    ) -> Option<StreamItem<T::Item, Id>>
    where
        T: Send + Sync + Unpin,
        Id: std::fmt::Debug + Clone,
    {
        // Unwrapping result as it should not be possbile to have the other end of the channel
        // closed, we hold both ends.
        let added_stream = added_stream.expect("Add channel was closed for adding.");

        if added_stream.stream.is_none() {
            // stream was returned
            return None;
        }

        let stream_id = added_stream.stream_id.clone();
        log::debug!("Channel() adding StreamId({:?})", stream_id);

        if added_stream.stream.is_some() {
            // Add to the FuturesUnordered
            self.stream_futures.push(added_stream.next());
        } else {
            log::warn!(
                "Trying to enqueue dropped stream {:?} into channel",
                stream_id,
            );
        }

        Some(StreamItem {
            stream_id,
            kind: ItemKind::Connected,
        })
    }

    async fn process_stream_output(
        &self,
        (output, stream_holder): (
            Option<T::Item>, // TryStream
            StreamHolder<T, Id>,
        ),
    ) -> Option<StreamItem<T::Item, Id>>
    where
        T: Send + Sync + Unpin,
        Id: Clone,
    {
        if stream_holder.stream.is_none() {
            // stream has been returned
            return None;
        }

        let stream_id = stream_holder.stream_id.clone();

        let kind = match output {
            None => ItemKind::Disconnected,
            Some(value) => {
                // Add the stream back into the FuturesUnordered
                self.stream_futures.push(stream_holder.into_future());
                ItemKind::Value(value)
            }
        };

        Some(StreamItem { stream_id, kind })
    }
}
