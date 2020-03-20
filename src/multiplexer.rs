use crate::*;

use futures::prelude::*;
use futures::stream::{FuturesUnordered, StreamExt, StreamFuture};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use std::collections::HashMap;
use std::io::Result as IoResult;

type IncomingPacketReader<ReadSt> = HaltAsyncRead<ReadSt>;
type IncomingPacketReaderTx<ReadSt> =
    mpsc::UnboundedSender<(ChannelId, StreamMover<IncomingPacketReader<ReadSt>>)>;
type StreamMovers<ReadSt> = HashMap<
    StreamId,
    (
        ChannelId, // Channel that they are in
        StreamMoverControl,
        oneshot::Receiver<IncomingPacketReader<ReadSt>>,
    ),
>;

// FIXME: Document what the different generics mean
/// Manages incoming streams of data and the enqueueing of outgoing data.
///
/// Outgoing streams have their own buffer of messages and do not affect other streams.
/// Incoming streams have their messages routed into channels that have their own backpressure.
pub struct Multiplexer<Item, ReadSt, WriteSi, OutSt, Id = IncrementIdGen>
where
    ReadSt: Stream,
{
    readers: Option<Vec<FuturesUnordered<StreamFuture<StreamMover<IncomingPacketReader<ReadSt>>>>>>,
    senders: Option<MultiplexerSenders<Item, WriteSi, Id>>,
    outgoing: OutSt,
    incoming_packet_sinks: Vec<mpsc::Sender<IncomingPacket<ReadSt::Item>>>,
    stream_movers: StreamMovers<ReadSt>,
    senders_channel: mpsc::UnboundedSender<(Sender<WriteSi>, oneshot::Sender<StreamId>)>,
    messages_channel: mpsc::UnboundedSender<OutgoingMessage<Item>>,
}

impl<Item, ReadSt, WriteSi, OutSt, Id> std::fmt::Debug
    for Multiplexer<Item, ReadSt, WriteSi, OutSt, Id>
where
    ReadSt: Stream,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Multiplexer").finish()
    }
}

impl<Item, ReadSt, WriteSi, OutSt> Multiplexer<Item, ReadSt, WriteSi, OutSt, IncrementIdGen>
where
    ReadSt: Stream + Unpin,
    WriteSi: Sink<Item> + Unpin,
{
    // FIXME: Consider taking a function that can determine which channel a packet should be in
    /// Calls `with_id_gen`, giving it an IncrementIdGen as well as the rest of the arguments.
    pub fn new(
        sender_buffer_size: usize,
        outgoing: OutSt,
        incoming_packet_sinks: Vec<mpsc::Sender<IncomingPacket<ReadSt::Item>>>,
    ) -> Self {
        let id_gen = IncrementIdGen::default();

        Self::with_id_gen(sender_buffer_size, id_gen, outgoing, incoming_packet_sinks)
    }
}

impl<Item, ReadSt, WriteSi, OutSt, Id> Multiplexer<Item, ReadSt, WriteSi, OutSt, Id>
where
    ReadSt: Stream + Unpin,
    WriteSi: Sink<Item> + Unpin,
{
    /// Initializes with a stream that provides the outgoing packets which will be enqueued to the
    /// corresponding streams, and a vector of sinks that represent different "channels" or
    /// "categories" of data.
    pub fn with_id_gen(
        sender_buffer_size: usize,
        id_gen: Id,
        outgoing: OutSt,
        incoming_packet_sinks: Vec<mpsc::Sender<IncomingPacket<ReadSt::Item>>>,
    ) -> Self {
        if incoming_packet_sinks.is_empty() {
            panic!("Must have at least one packet sink");
        }

        let (senders_channel, senders_channel_rx) = mpsc::unbounded_channel();
        let (messages_channel, messages_channel_rx) = mpsc::unbounded_channel();
        let senders = MultiplexerSenders::new(
            sender_buffer_size,
            id_gen,
            senders_channel_rx,
            messages_channel_rx,
        );

        let readers = (0..incoming_packet_sinks.len())
            .into_iter()
            .map(|_| FuturesUnordered::new())
            .collect();

        let stream_movers = StreamMovers::default();

        Self {
            senders: Some(senders),
            outgoing,
            readers: Some(readers),
            incoming_packet_sinks,
            stream_movers,
            senders_channel,
            messages_channel,
        }
    }
}

impl<Item, ReadSt, WriteSi, OutSt, Id> Multiplexer<Item, ReadSt, WriteSi, OutSt, Id>
where
    ReadSt: Stream + Unpin,
    ReadSt::Item: std::fmt::Debug,
    Item: Clone + std::fmt::Debug,
{
    async fn change_channel(
        &mut self,
        ids: Vec<StreamId>,
        channel: ChannelId,
        incoming_packet_reader_tx: &mut IncomingPacketReaderTx<ReadSt>,
    ) {
        tracing::trace!(?ids, ?channel, "change channel");
        for id in ids {
            // Fetch the StreamMoverController
            match self.stream_movers.remove(&id) {
                None => {
                    tracing::error!(id, "was not in stream_movers");
                }
                Some((channel_id, control, receiver)) => {
                    // Signal so that it moves the PacketReader to us
                    control.signal();

                    // wait for the oneshot to receive the PacketReader
                    match receiver.await {
                        Ok(packet_reader) => {
                            // Let the current stream know that the packet has left.
                            let packet_leave = IncomingPacket::StreamDisconnected(
                                id,
                                DisconnectReason::ChannelChange(channel),
                            );
                            if let Err(err) = self.incoming_packet_sinks[channel_id]
                                .send(packet_leave)
                                .await
                            {
                                tracing::error!(?err, "Could not send out stream disconnect.");
                                return;
                            }

                            // Send the stream into the change channel queue
                            self.enqueue_packet_reader(
                                id,
                                channel,
                                packet_reader,
                                incoming_packet_reader_tx,
                            );
                        }
                        Err(err) => {
                            tracing::error!(?err, "Could not receive for channel change");
                        }
                    }
                }
            }
        }
    }

    fn enqueue_packet_reader(
        &mut self,
        stream_id: StreamId,
        channel: ChannelId,
        reader: IncomingPacketReader<ReadSt>,
        incoming_packet_reader_tx: &mut IncomingPacketReaderTx<ReadSt>,
    ) {
        // Wrap the packet reader in a StreamMover
        // FIXME: Duplicated in change_channel
        let (move_stream_tx, move_stream_rx) = oneshot::channel();
        let (control, stream_mover) = StreamMoverControl::wrap(reader, move_stream_tx);

        tracing::trace!(stream_id, channel, "inserting into stream_movers");
        if self
            .stream_movers
            .insert(stream_id, (channel, control, move_stream_rx))
            .is_some()
        {
            tracing::error!(stream_id, "was already in the stream_movers");
        }

        tracing::trace!(stream_id, channel, "enqueueing");
        // Send the PacketReader to channel zero
        if let Err(_) = incoming_packet_reader_tx.send((channel, stream_mover)) {
            tracing::error!("Error enqueueing incoming connection");
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, write_half, read_half, incoming_packet_reader_tx)
    )]
    async fn handle_incoming_connection(
        &mut self,
        write_half: WriteSi,
        read_half: ReadSt,
        incoming_packet_reader_tx: &mut IncomingPacketReaderTx<ReadSt>,
    ) {
        tracing::trace!("new connection");

        // used to re-join the two halves so that we can shut down the reader
        let (halt, mut async_read_halt) = HaltRead::wrap(read_half);

        // Keep track of the write_half and generate a stream_id
        let sender: Sender<WriteSi> = Sender::new(write_half, halt);

        let (stream_id_tx, stream_id_rx) = oneshot::channel();
        self.senders_channel
            .send((sender, stream_id_tx))
            .expect("should be able to send");
        let stream_id = stream_id_rx.await.expect("Id should be sent back.");

        async_read_halt.set_stream_id(stream_id);

        self.enqueue_packet_reader(stream_id, 0, async_read_halt, incoming_packet_reader_tx);
    }
}

impl<Item, ReadSt, WriteSi, OutSt, Id> Multiplexer<Item, ReadSt, WriteSi, OutSt, Id>
where
    ReadSt: Stream + Send + Unpin + 'static,
    ReadSt::Item: Send,
{
    /// Awaits incoming channel joins and messages from those streams in the channel.
    ///
    /// If there is backpressure, joining the channel also slows.
    fn run_channel(
        channel: ChannelId,
        mut reader: FuturesUnordered<StreamFuture<StreamMover<IncomingPacketReader<ReadSt>>>>,
        mut incoming_packet_sink: mpsc::Sender<IncomingPacket<ReadSt::Item>>,
        mut incoming_packet_reader_rx: mpsc::UnboundedReceiver<
            StreamMover<IncomingPacketReader<ReadSt>>,
        >,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                tracing::trace!(channel, "incoming loop start");
                tokio::select! {
                    packet_reader = incoming_packet_reader_rx.recv() => {
                        // There is a new stream to join this channel
                        match packet_reader {
                            Some(packet_reader) => {
                                // Get the stream_id out of the packet_reader
                                let stream_id = packet_reader
                                    .stream()
                                    .expect("Stream is moving channels, should exist.")
                                    .stream_id()
                                    .unwrap();


                                // Announce that the stream is joining this channel
                                let join_packet = IncomingPacket::StreamConnected(stream_id);
                                if let Err(_) = incoming_packet_sink.send(join_packet).await {
                                    tracing::error!("Could not send channel join. Shutting down receive loop");
                                    return;
                                }

                                reader.push(packet_reader.into_future());
                            }
                            None => {
                                tracing::error!("incoming packet reader received None, exiting loop");
                                return;
                            }
                        }
                    }
                    next_reader = reader.next(), if !reader.is_empty() => {
                        let mut next_reader = next_reader;
                        let (packet_res, packet_reader) = next_reader.take().expect("reader.next() is never called if it's empty");

                        tracing::trace!("incoming data");
                        match packet_res {
                            Some(packet) => {
                                tracing::trace!(channel, "sending data");
                                if let Err(_) = incoming_packet_sink.send(packet).await {
                                    tracing::error!("Could not proxy channel data. Shutting down receive loop");
                                    return;
                                }
                                reader.push(packet_reader.into_future());
                            }
                            None => {
                                // Dropping packet reader, stream is done
                                tracing::error!("incoming reader received None");
                            }
                        }
                    }
                }
            }
        })
    }
}

impl<Item, ReadSt, WriteSi, OutSt, Id> Multiplexer<Item, ReadSt, WriteSi, OutSt, Id>
where
    Id: IdGen,
    OutSt: Stream<Item = OutgoingPacket<Item>>,
    ReadSt: Stream,
    WriteSi: Sink<Item>,
    WriteSi::Error: std::fmt::Debug,

    Id: Send + Unpin + 'static,
    Item: Clone + Send + Sync + 'static + std::fmt::Debug,
    OutSt: Send + Unpin + 'static,
    ReadSt: Send + Unpin + 'static,
    ReadSt::Item: Send + std::fmt::Debug,
    WriteSi: Send + Unpin + 'static,
{
    /// Start the multiplexer. Giving it a stream of incoming connection halves and a stream for
    /// ControlMessages.
    #[tracing::instrument(level = "debug", skip(incoming_halves, control))]
    pub async fn run<V, U>(
        mut self,
        mut incoming_halves: V,
        mut control: U,
    ) -> JoinHandle<IoResult<()>>
    where
        V: Stream<Item = IoResult<(WriteSi, ReadSt)>>,
        V: Unpin + Send + 'static,
        U: Stream<Item = ControlMessage> + Send + Unpin + 'static,
    {
        tracing::info!("Waiting for connections");

        let mut readers = self.readers.take().expect("Should have readers!");

        let channel_count = self.incoming_packet_sinks.len();
        let mut incoming_packet_readers_tx = Vec::with_capacity(channel_count);

        for channel in 0..channel_count {
            let reader = readers.remove(0);
            let incoming_packet_sink = self.incoming_packet_sinks[channel].clone();

            let (incoming_packet_reader_tx, incoming_packet_reader_rx) = mpsc::unbounded_channel();
            incoming_packet_readers_tx.push(incoming_packet_reader_tx);

            Self::run_channel(
                channel,
                reader,
                incoming_packet_sink,
                incoming_packet_reader_rx,
            );
        }

        // FIXME: Consider better names...
        let (mut incoming_packet_reader_tx, mut incoming_packet_reader_rx) =
            mpsc::unbounded_channel();

        tokio::task::spawn(async move {
            // Distribute incoming PacketReaders into the different channels
            while let Some((channel, packet_reader)) = incoming_packet_reader_rx.recv().await {
                let ipr_tx: &mpsc::UnboundedSender<_> = &incoming_packet_readers_tx[channel];
                if let Err(_) = ipr_tx.send(packet_reader) {
                    tracing::error!("Error moving incoming stream to channel");
                }
            }
        });

        let (shutdown_ids_tx, shutdown_ids_rx) = mpsc::unbounded_channel();

        let mut senders = self.senders.take().expect("Senders should exist until now");
        tokio::task::spawn(async move {
            tracing::trace!("starting senders poll() loop task");
            senders.run_to_completion(shutdown_ids_rx).await;
            tracing::trace!("leaving senders poll() loop");
        });

        tokio::task::spawn(async move {
            loop {
                tokio::select!(
                    incoming_opt = incoming_halves.next() => {
                        match incoming_opt {
                            Some(Ok((write_half, read_half))) => {
                                self.handle_incoming_connection(write_half, read_half, &mut incoming_packet_reader_tx).await;
                            }
                            Some(Err(error)) => {
                                tracing::error!("ERROR: {}", error);
                            }
                            None => unreachable!()
                        }
                    }
                    outgoing_opt = self.outgoing.next() => {
                        if let Some(outgoing_packet) = outgoing_opt {
                            match outgoing_packet {
                                OutgoingPacket::ChangeChannel(ids, channel) => {
                                    self.change_channel(ids, channel, &mut incoming_packet_reader_tx).await;
                                }
                                OutgoingPacket::Shutdown(ids) => {
                                    if let Err(err) = shutdown_ids_tx.send(ids) {
                                        tracing::error!(?err, "Error sending shutdowns.");
                                    }
                                }
                                OutgoingPacket::Message(message) => {
                                    if let Err(error) = self.messages_channel.send(message) {
                                        tracing::error!(%error, "outgoing packet");
                                    }
                                }
                            }
                        }
                    }
                    control_message_opt = control.next() => {
                        if let Some(control_message) = control_message_opt {
                            match control_message {
                                ControlMessage::Shutdown => { return Ok(()); }
                            }
                        }
                    }
                )
            }
        })
    }
}
