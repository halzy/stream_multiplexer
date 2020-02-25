use crate::*;

use futures::prelude::*;
use futures::stream::{SelectAll, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use std::collections::HashMap;
use std::io::Result as IoResult;

type IncomingPacketReader<InSt> = HaltAsyncRead<InSt>;
type IncomingPacketReaderTx<InSt> =
    mpsc::UnboundedSender<(usize, StreamMover<IncomingPacketReader<InSt>>)>;
type StreamMovers<InSt> = HashMap<
    StreamId,
    (
        StreamMoverControl,
        oneshot::Receiver<IncomingPacketReader<InSt>>,
    ),
>;

/// Manages incoming streams of data and the enqueueing of outgoing data.
///
/// Outgoing streams have their own buffer of messages and do not affect other streams.
/// Incoming streams have their messages routed into channels that have their own backpressure.
pub struct Multiplexer<InSt, Out, OutItem, OutSi, Id>
where
    InSt: Stream,
{
    readers: Option<Vec<SelectAll<StreamMover<IncomingPacketReader<InSt>>>>>,
    senders: Option<MultiplexerSenders<OutItem, OutSi, Id>>,
    outgoing: Out,
    incoming_packet_sinks: Option<Vec<mpsc::Sender<IncomingPacket<InSt::Item>>>>,
    stream_movers: StreamMovers<InSt>,
    senders_channel: mpsc::UnboundedSender<(Sender<OutSi>, oneshot::Sender<StreamId>)>,
    messages_channel: mpsc::UnboundedSender<(StreamId, OutgoingMessage<OutItem>)>,
}

impl<InSt, Out, OutItem, OutSi, Id> std::fmt::Debug for Multiplexer<InSt, Out, OutItem, OutSi, Id>
where
    InSt: Stream,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Multiplexer").finish()
    }
}

impl<InSt, Out, OutItem, OutSi> Multiplexer<InSt, Out, OutItem, OutSi, IncrementIdGen>
where
    InSt: Stream + Unpin,
    OutSi: Sink<OutItem> + Unpin,
{
    // FIXME: Consider taking a function that can determine which channel a packet should be in
    /// Calls `with_id_gen`, giving it an IncrementIdGen as well as the rest of the arguments.
    pub fn new(
        sender_buffer_size: usize,
        outgoing: Out,
        incoming_packet_sinks: Vec<mpsc::Sender<IncomingPacket<InSt::Item>>>,
    ) -> Self {
        let id_gen = IncrementIdGen::default();

        Self::with_id_gen(sender_buffer_size, id_gen, outgoing, incoming_packet_sinks)
    }
}

impl<InSt, Out, OutItem, OutSi, Id> Multiplexer<InSt, Out, OutItem, OutSi, Id>
where
    InSt: Stream + Unpin,
    OutSi: Sink<OutItem> + Unpin,
{
    /// Initializes with a stream that provides the outgoing packets which will be enqueued to the
    /// corresponding streams, and a vector of sinks that represent different "channels" or
    /// "categories" of data.
    pub fn with_id_gen(
        sender_buffer_size: usize,
        id_gen: Id,
        outgoing: Out,
        incoming_packet_sinks: Vec<mpsc::Sender<IncomingPacket<InSt::Item>>>,
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
            .map(|_| SelectAll::new())
            .collect();

        let stream_movers = StreamMovers::default();

        Self {
            senders: Some(senders),
            outgoing,
            readers: Some(readers),
            incoming_packet_sinks: Some(incoming_packet_sinks),
            stream_movers,
            senders_channel,
            messages_channel,
        }
    }
}

impl<InSt, Out, OutItem, OutSi, Id> Multiplexer<InSt, Out, OutItem, OutSi, Id>
where
    InSt: Stream + Unpin,
    OutItem: Clone,
{
    async fn change_channel(
        &mut self,
        ids: &Vec<StreamId>,
        channel: usize,
        incoming_packet_reader_tx: &mut IncomingPacketReaderTx<InSt>,
    ) {
        tracing::trace!(?ids, ?channel, "change channel");
        for id in ids {
            // Fetch the StreamMoverController
            match self.stream_movers.remove(&id) {
                None => {
                    tracing::error!(id, "was not in stream_movers");
                }
                Some((control, receiver)) => {
                    // Signal so that it moves the PacketReader to us
                    control.signal();

                    // wait for the oneshot to receive the PacketReader
                    match receiver.await {
                        Ok(packet_reader) => {
                            self.enqueue_packet_reader(
                                *id,
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
        channel: usize,
        reader: IncomingPacketReader<InSt>,
        incoming_packet_reader_tx: &mut IncomingPacketReaderTx<InSt>,
    ) {
        // Wrap the packet reader in a StreamMover
        // FIXME: Duplicated in change_channel
        let (move_stream_tx, move_stream_rx) = oneshot::channel();
        let (control, stream_mover) = StreamMoverControl::wrap(reader, move_stream_tx);

        tracing::trace!(stream_id, channel, "inserting into stream_movers");
        if self
            .stream_movers
            .insert(stream_id, (control, move_stream_rx))
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
        write_half: OutSi,
        read_half: InSt,
        incoming_packet_reader_tx: &mut IncomingPacketReaderTx<InSt>,
    ) {
        tracing::trace!("new connection");

        // used to re-join the two halves so that we can shut down the reader
        let (halt, async_read_halt) = HaltRead::wrap(read_half);

        // Keep track of the write_half and generate a stream_id
        let sender: Sender<OutSi> = Sender::new(write_half, halt);

        let (stream_id_tx, stream_id_rx) = oneshot::channel();
        self.senders_channel
            .send((sender, stream_id_tx))
            .expect("should be able to send");
        let stream_id = stream_id_rx.await.expect("Id should be sent back.");

        self.enqueue_packet_reader(stream_id, 0, async_read_halt, incoming_packet_reader_tx);
    }

    #[tracing::instrument(level = "trace", skip(self, ids, message))]
    async fn handle_outgoing_message(
        &mut self,
        ids: &Vec<StreamId>,
        message: &OutgoingMessage<OutItem>,
    ) {
        for id in ids {
            tracing::trace!(stream=?id, "sending");
            // FIXME: What should we do if there are send errors?
            if let Err(error) = self.messages_channel.send((*id, message.clone())) {
                tracing::error!(%error, "outgoing packet");
            }
        }
    }
}

impl<InSt, Out, OutItem, OutSi, Id> Multiplexer<InSt, Out, OutItem, OutSi, Id>
where
    InSt: Stream + Send + Unpin + 'static,
    InSt::Item: Send,
{
    /// Awaits incoming channel joins and messages from those streams in the channel.
    ///
    /// If there is backpressure, joining the channel also slows.
    fn run_channel(
        channel: usize,
        mut reader: SelectAll<StreamMover<IncomingPacketReader<InSt>>>,
        mut incoming_packet_sink: mpsc::Sender<IncomingPacket<InSt::Item>>,
        mut incoming_packet_reader_rx: mpsc::UnboundedReceiver<
            StreamMover<IncomingPacketReader<InSt>>,
        >,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                tracing::trace!(channel, "incoming loop start");
                tokio::select! {
                    packet_reader = incoming_packet_reader_rx.recv() => {
                        tracing::trace!("incoming socket (none)");
                        match packet_reader {
                            Some(packet_reader) => {
                                reader.push(packet_reader);
                            }
                            None => {
                                tracing::error!("incoming packet reader received None, exiting loop");
                                return;
                            }
                        }
                    }
                    packet_res = reader.next(), if !reader.is_empty() => {
                        tracing::trace!("incoming data");
                        match packet_res {
                            Some(packet) => {
                                let message = IncomingMessage::Value(packet);
                                // FIXME: 0 should be the actual stream ID
                                let packet = IncomingPacket::new(channel, message);
                                tracing::trace!(channel, "sending data");
                                if let Err(_) = incoming_packet_sink.send(packet).await {
                                    tracing::error!("Shutting down receive loop");
                                    return;
                                }
                            }
                            None => {
                                tracing::error!("incoming reader received None");
                            }
                        }
                    }
                }
            }
        })
    }
}

impl<InSt, Out, OutItem, OutSi, Id> Multiplexer<InSt, Out, OutItem, OutSi, Id>
where
    Id: IdGen,
    InSt: Stream,
    Out: Stream<Item = OutgoingPacket<OutItem>>,
    OutItem: Clone,
    OutSi: Sink<OutItem>,

    Id: Send + Unpin + 'static,
    InSt: Send + Unpin + 'static,
    InSt::Item: Send,
    Out: Send + Unpin + 'static,
    OutItem: Send + Sync + 'static,
    OutSi: Send + Unpin + 'static,
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
        V: Stream<Item = IoResult<(OutSi, InSt)>>,
        V: Unpin + Send + 'static,
        U: Stream<Item = ControlMessage> + Send + Unpin + 'static,
    {
        tracing::info!("Waiting for connections");

        let mut readers = self.readers.take().expect("Should have readers!");
        let mut incoming_packet_sinks = self
            .incoming_packet_sinks
            .take()
            .expect("Should have incoming_packet_sinks!");

        let channel_count = incoming_packet_sinks.len();
        let mut incoming_packet_readers_tx = Vec::with_capacity(channel_count);

        for channel in 0..channel_count {
            let reader = readers.remove(0);
            let incoming_packet_sink = incoming_packet_sinks.remove(0);

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

        let mut senders = self.senders.take().expect("Senders should exist until now");
        tokio::task::spawn(async move {
            tracing::trace!("starting senders poll() loop task");
            senders.run_to_completion().await;
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
                            match &outgoing_packet.message {
                                OutgoingMessage::ChangeChannel(channel) => {
                                    self.change_channel(&outgoing_packet.ids, *channel, &mut incoming_packet_reader_tx).await;
                                }
                                message => {
                                    self.handle_outgoing_message(&outgoing_packet.ids, message).await;
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

