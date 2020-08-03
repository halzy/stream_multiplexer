use crate::*;

use async_channel as channel;
use futures_util::future::FutureExt;
use futures_util::stream::{StreamExt, StreamFuture, TryStream};

type ReadStream<T> = StreamDropper<T>;

#[derive(Debug)]
pub(crate) struct Channel<T> {
    id: ChannelId,
    add_rx: channel::Receiver<ReadStream<T>>,
    stream_futures: FuturesUnordered<StreamFuture<ReadStream<T>>>,
}

impl<T> Channel<T>
where
    T: TryStream + Unpin,
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

    pub(crate) async fn next(&mut self) -> (StreamId, Option<T::Item>) {
        log::trace!("next() for {}", self.id);
        // select on the stream-add channel and the FuturesUnordered
        loop {
            if self.stream_futures.is_empty() {
                log::trace!("channel {} is empty, awaiting streams.", self.id);
                let added_stream_res = self.add_rx.recv().await;
                // return the erorr if there was one
                self.process_stream_add(added_stream_res);
            } else {
                log::trace!("channel {} is awaiting streams or packets", self.id);
                futures_util::select_biased! {
                    added_stream_res = self.add_rx.recv().fuse() => {
                        log::trace!("channel {} adding new stream", self.id);
                        self.process_stream_add(added_stream_res);
                    }
                    stream_output = self.stream_futures.next().fuse() => {
                        log::trace!("channel {} processing stream data", self.id);
                        return self.process_stream_output(stream_output.unwrap()).await;
                    }
                };
            }
        }
    }

    fn process_stream_add(
        &mut self,
        added_stream: Result<ReadStream<T>, async_channel::RecvError>,
    ) {
        // Unwrapping result as it should not be possbile to have the other end of the channel
        // closed, we hold both ends.
        let added_stream = added_stream.expect("Add channel was closed for adding.");

        let stream_id = added_stream.id;
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
    }

    async fn process_stream_output(
        &self,
        (output, stream): (
            Option<T::Item>, // TryStream
            ReadStream<T>,
        ),
    ) -> (StreamId, Option<T::Item>) {
        let stream_id = stream.id;

        // re-enqueue the stream if it's viable
        if output.is_some() && stream.stream.is_some() {
            self.stream_futures.push(stream.into_future());
        }

        // return the bytes from the stream
        (stream_id, output)
    }
}
