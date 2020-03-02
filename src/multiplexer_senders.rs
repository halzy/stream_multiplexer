use crate::*;

use futures::prelude::*;
use futures::stream::{FuturesUnordered, StreamFuture};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

use std::collections::hash_map::Entry;
use std::collections::HashMap;

type SenderMessageStream<Item> = mpsc::Receiver<Item>;

type SendAll<Si, Item> = SendAllOwn<Si, SenderMessageStream<Item>>;

struct SenderPair<Si, Item> {
    sender: mpsc::Sender<Item>,
    send_all: Option<SendAll<Si, Item>>,
}
impl<Si, Item> SenderPair<Si, Item> {
    fn new(sender: mpsc::Sender<Item>, send_all: SendAll<Si, Item>) -> Self {
        Self {
            sender,
            send_all: send_all.into(),
        }
    }

    fn give(&mut self, value: SendAll<Si, Item>) {
        debug_assert!(
            self.send_all.is_none(),
            "SenderPair already had an item, can't hold that much!"
        );
        self.send_all.replace(value);
    }

    fn take(&mut self) -> Option<SendAll<Si, Item>> {
        self.send_all.take()
    }

    fn try_send(&mut self, message: Item) -> Result<(), TrySendError<Item>> {
        self.sender.try_send(message)
    }
}

/// Stores Sender and provides a generated ID
pub(crate) struct MultiplexerSenders<Item, Si, Id> {
    sender_buffer_size: usize,
    id_gen: Id,

    // Sender ownership bounces between these two, depending on if it has outgoing data
    senders_stream: FuturesUnordered<StreamFuture<SendAll<Si, Item>>>,
    sender_pairs: HashMap<StreamId, SenderPair<Si, Item>>,

    // Channels that provide new senders and messages
    senders_channel: mpsc::UnboundedReceiver<(Sender<Si>, oneshot::Sender<StreamId>)>,
    messages_channel: mpsc::UnboundedReceiver<OutgoingMessage<Item>>,
}

impl<Item, Si, Id> MultiplexerSenders<Item, Si, Id>
where
    Si: Sink<Item> + Unpin,
{
    pub(crate) fn new(
        sender_buffer_size: usize,
        id_gen: Id,
        senders_channel: mpsc::UnboundedReceiver<(Sender<Si>, oneshot::Sender<StreamId>)>,
        messages_channel: mpsc::UnboundedReceiver<OutgoingMessage<Item>>,
    ) -> Self {
        Self {
            sender_buffer_size,
            id_gen,
            senders_stream: FuturesUnordered::new(),
            sender_pairs: HashMap::new(),
            senders_channel,
            messages_channel,
        }
    }
}

impl<Item, Si, Id> std::fmt::Debug for MultiplexerSenders<Item, Si, Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexerSenders").finish()
    }
}

impl<Item, Si, Id> MultiplexerSenders<Item, Si, Id>
where
    Si: Sink<Item> + Unpin,
    Id: IdGen,
    Item: Clone,
{
    #[tracing::instrument(level = "trace", skip(self, sender, stream_id_channel))]
    fn add(&mut self, mut sender: Sender<Si>, stream_id_channel: oneshot::Sender<StreamId>) {
        loop {
            let id = self.id_gen.next();

            if !self.sender_pairs.contains_key(&id) {
                tracing::trace!(stream_id = id, "inserting");

                sender.set_stream_id(id);

                let (tx, rx) = mpsc::channel(self.sender_buffer_size);
                let sender_stream = SendAllOwn::new(sender, rx);

                let sender_pair = SenderPair::new(tx, sender_stream);

                let res = self.sender_pairs.insert(id, sender_pair);
                assert!(res.is_none(), "MPSender: Highly unlikely");

                if let Err(_) = stream_id_channel.send(id) {
                    tracing::error!("could not return stream_id for new sender");
                }

                return;
            }
        }
    }

    /// Returns true if it was shut-down, false otherwise
    pub(crate) fn shutdown_streams(&mut self, stream_ids: &[StreamId]) {
        for stream_id in stream_ids {
            match self.sender_pairs.remove(&stream_id) {
                Some(_) => {
                    // The Sender could still be in the FuturesUnordered, it should not be
                    // re-inserted into the sender_pairs hashmap.
                    tracing::trace!(stream_id, "Shutting down stream sender.");
                }
                None => {
                    tracing::warn!(stream_id, "Trying to shut down non-existent sender.");
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_lengths(&self) -> (usize, usize) {
        let futures_len = self.senders_stream.len();
        let sender_pairs_len = self.sender_pairs.len();
        (futures_len, sender_pairs_len)
    }

    fn handle_new_message(&mut self, mut message: OutgoingMessage<Item>) {
        for stream_id in message.stream_ids.iter().cloned().flatten() {
            match self.sender_pairs.entry(stream_id) {
                Entry::Vacant(_) => {
                    tracing::warn!(stream_id, "Tring to send message to non-existent stream.");
                }
                Entry::Occupied(mut sender_pair_entry) => {
                    let mut should_remove = false;
                    for value in message.values.drain(..).flatten() {
                        let sender_pair = sender_pair_entry.get_mut();
                        match sender_pair.try_send(value.clone()) {
                            Ok(()) => {
                                // Enqueue the message and move the sender into the FuturesUnordered
                                tracing::trace!(stream_id, "Enqueued message.");
                                if let Some(sender) = sender_pair.take() {
                                    self.senders_stream.push(sender.into_future());
                                }
                            }
                            Err(TrySendError::Full(_)) => {
                                tracing::error!(stream_id, "Stream is full, shutting down sender.");
                                should_remove = true;
                                break;
                            }
                            Err(TrySendError::Closed(_)) => {
                                tracing::error!(
                                    stream_id,
                                    "Stream is closed, shutting down sender."
                                );
                                should_remove = true;
                                break;
                            }
                        };
                    }
                    if should_remove {
                        sender_pair_entry.remove_entry();
                    }
                }
            };
        }
    }

    fn handle_finished_sink(
        &mut self,
        opt_res: (
            Option<Result<(), Si::Error>>,
            SendAllOwn<Si, SenderMessageStream<Item>>,
        ),
    ) {
        match opt_res {
            // Sender finished sending, put it back in the hashmap
            (Some(Ok(())), sender) => {
                tracing::trace!("senders produced Some(Ok(())");
                let stream_id = sender.inner().stream_id();
                match self.sender_pairs.entry(stream_id) {
                    Entry::Vacant(_) => {
                        // It could have been shut-down while sending packets.
                        tracing::warn!(stream_id, "No sender_pairs entry, dropping");
                    }
                    Entry::Occupied(mut sender_pair_entry) => {
                        sender_pair_entry.get_mut().give(sender);
                    }
                }
            }
            (Some(Err(_err)), _sender) => {
                tracing::error!("senders produced an error");
                todo!();
            }
            (None, _sender) => todo!(),
        }
    }

    pub(crate) async fn run_to_completion(
        &mut self,
        mut shutdown_ids: mpsc::UnboundedReceiver<Vec<StreamId>>,
    ) {
        tracing::trace!("get_next() starting");

        loop {
            tracing::trace!("loop");
            tokio::select! {
                Some(ids) = shutdown_ids.next() => {
                    self.shutdown_streams(&ids);
                }
                new_sender = self.senders_channel.next() => {
                    match new_sender {
                        None => {
                            tracing::trace!("senders_channel() had None, returning");
                            return;
                        }
                        Some((sender, return_channel))  => {
                            tracing::trace!("senders_channel");
                            self.add(sender, return_channel);
                        }
                    }
                }
                new_message = self.messages_channel.next() => {
                    match new_message {
                        None => {
                            tracing::trace!("message_channel() had None, returning");
                            return;
                        }
                        Some(message) => {
                            tracing::trace!("messages_channel");
                            self.handle_new_message(message);
                        }
                    }
                }
                // Polling to pump the outgoing message futures
                Some(senders_opt) = self.senders_stream.next(), if !self.senders_stream.is_empty() => {
                    tracing::trace!("senders_stream");
                    self.handle_finished_sink(senders_opt);
                },
                else => {
                    tracing::trace!("get_next() exiting with None");
                    return;
                }
            }
        }
    }
}

// FIXME: TODO:
//  - Check to see if the sender should be re-inserted when they come out of the FuturesUnordered
//  - If the reader is closed, what do we do?

#[cfg(test)]
mod tests {
    use super::*;

    use futures::channel::mpsc as fut_mpsc;

    #[tokio::test(basic_scheduler)]
    async fn send_message_and_shutdown() {
        //crate::tests::init_logging();

        // Set up a teest incrementer
        let id_gen = IncrementIdGen::default();

        // Set up channels for new connections & messages
        let (sender_channel, sender_channel_rx) = mpsc::unbounded_channel();
        let (message_channel, message_channel_rx) = mpsc::unbounded_channel();

        // The struct we're testing
        let mut multiplexer_senders =
            MultiplexerSenders::new(32, id_gen, sender_channel_rx, message_channel_rx);

        let (shutdown_ids_tx, shutdown_ids_rx) = mpsc::unbounded_channel();

        let ms_join = tokio::task::spawn(async move {
            multiplexer_senders.run_to_completion(shutdown_ids_rx).await;
            multiplexer_senders
        });

        // Create Sender & Reader test pair
        let (tx, rx) = fut_mpsc::channel::<u8>(10);
        let (sender, mut reader) = crate::tests::sender_reader(tx.clone(), rx);

        // Return channel for the new stream id
        let (stream_id_tx, stream_id_rx) = oneshot::channel();

        // Give the new sender to multiplexer_senders
        sender_channel
            .send((sender, stream_id_tx))
            .expect("should be able to send");

        let stream_id = stream_id_rx.await.expect("Should get stream id back.");

        reader.set_stream_id(stream_id);

        // Send some data to the stream
        for x in 0_u8..10 {
            let message = OutgoingMessage::new(vec![stream_id], vec![x]);
            message_channel.send(message).unwrap();
        }

        // Send some data to the stream
        for x in 0_u8..10 {
            assert_eq!(
                &x,
                reader
                    .next()
                    .await
                    .expect("should have data")
                    .value()
                    .unwrap()
            );
        }

        // Send the shutdown message to the stream
        shutdown_ids_tx
            .send(vec![stream_id])
            .expect("should be able to send stream shutdown");

        // Await the reader so that it shuts down
        reader.next().await;

        // Drop these channels to shut down multixplexer_senders
        drop(sender_channel);
        drop(message_channel);

        // Should no longer have futures or senders
        let multiplexer_senders = ms_join.await.unwrap();
        assert_eq!((0, 0), multiplexer_senders.test_lengths());
    }
}
