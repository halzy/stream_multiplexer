use super::{
    ControlMessage, HaltAsyncRead, HaltRead, IdGen, IncomingPacket, IncrementIdGen,
    MultiplexerSenders, OutgoingMessage, OutgoingPacket, PacketReader, Sender, StreamId,
    StreamShutdown,
};

use bytes::Bytes;
use futures::stream::SelectAll;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::stream::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::FramedRead;

use std::io::{Error, ErrorKind, Result as IoResult};

type IncomingPacketSink = mpsc::Sender<IncomingPacket>;
type IncomingPacketReader<T> = PacketReader<FramedRead<HaltAsyncRead<T>, LengthDelimitedCodec>>;

pub struct PacketMultiplexer<Out, T, I: IdGen = IncrementIdGen>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Unpin,
    Out: Stream<Item = OutgoingPacket>,
{
    readers: Option<Vec<SelectAll<IncomingPacketReader<T>>>>,
    senders: MultiplexerSenders<T, I>,
    outgoing: Out,
    incoming_packet_sinks: Option<Vec<IncomingPacketSink>>,
}

impl<Out, T, I> std::fmt::Debug for PacketMultiplexer<Out, T, I>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    I: IdGen,
    Out: Stream<Item = OutgoingPacket>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketMultiplexer ").finish()
    }
}

impl<Out, T> PacketMultiplexer<Out, T>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Unpin,
    Out: Stream<Item = OutgoingPacket>,
{
    pub fn new(outgoing: Out, incoming_packet_sinks: Vec<IncomingPacketSink>) -> Self {
        let senders = MultiplexerSenders::new(IncrementIdGen::default());
        Self::with_senders(senders, outgoing, incoming_packet_sinks)
    }
}

impl<S, T, I> PacketMultiplexer<S, T, I>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Unpin,
    I: IdGen,
    S: Stream<Item = OutgoingPacket>,
{
    pub fn with_senders(
        senders: MultiplexerSenders<T, I>,
        outgoing: S,
        incoming_packet_sinks: Vec<IncomingPacketSink>,
    ) -> Self {
        if incoming_packet_sinks.is_empty() {
            panic!("Must have at least one packet sink");
        }

        let readers = (0..incoming_packet_sinks.len())
            .into_iter()
            .map(|_| SelectAll::new())
            .collect();

        Self {
            senders,
            outgoing,
            readers: Some(readers),
            incoming_packet_sinks: Some(incoming_packet_sinks),
        }
    }
}

impl<S, T, I> PacketMultiplexer<S, T, I>
where
    T: StreamShutdown + tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Send + Unpin + std::fmt::Debug + 'static,
    I: IdGen + Send + 'static,
    S: Stream<Item = OutgoingPacket> + Unpin + Send + 'static,
{
    #[tracing::instrument(level = "debug", skip(incoming_tcp_streams, control))]
    pub async fn run<V, U>(
        mut self,
        mut incoming_tcp_streams: V,
        mut control: U,
    ) -> JoinHandle<IoResult<()>>
    where
        V: Stream<Item = IoResult<T>> + Send + Unpin + 'static,
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

        for _ in 0..channel_count {
            let reader = readers.remove(0);
            let incoming_packet_sink = incoming_packet_sinks.remove(0);

            let (incoming_packet_reader_tx, incoming_packet_reader_rx) = mpsc::unbounded_channel();
            incoming_packet_readers_tx.push(incoming_packet_reader_tx);

            Self::run_channel(reader, incoming_packet_sink, incoming_packet_reader_rx);
        }

        let (mut incoming_packet_reader_tx, mut incoming_packet_reader_rx) =
            mpsc::unbounded_channel();

        tokio::task::spawn(async move {
            while let Some((channel, packet_reader)) = incoming_packet_reader_rx.recv().await {
                let ipr_tx: &mpsc::UnboundedSender<IncomingPacketReader<T>> =
                    &incoming_packet_readers_tx[channel];
                if let Err(err) = ipr_tx.send(packet_reader) {
                    tracing::error!(?err, "Error moving incoming stream to channel");
                }
            }
        });

        tokio::task::spawn(async move {
            loop {
                tokio::select!(
                    incoming_opt = incoming_tcp_streams.next() => {
                        match incoming_opt {
                            Some(Ok(stream)) => {
                                self.handle_incoming_connection(stream, &mut incoming_packet_reader_tx);
                            }
                            Some(Err(error)) => {
                                tracing::error!("ERROR: {}", error);
                            }
                            None => unreachable!()
                        }
                    }
                    outgoing_opt = self.outgoing.next() => {
                        if let Some(outgoing_packet) = outgoing_opt {
                            self.handle_outgoing_packet(outgoing_packet).await;
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

    fn run_channel(
        mut reader: SelectAll<IncomingPacketReader<T>>,
        mut incoming_packet_sink: IncomingPacketSink,
        mut incoming_packet_reader_rx: mpsc::UnboundedReceiver<IncomingPacketReader<T>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            let mut incoming_packet: Option<IncomingPacket> = None;
            loop {
                tracing::trace!(?incoming_packet, "incoming loop start");
                match incoming_packet.clone() {
                    // We do not have an incoming packet
                    None => tokio::select!(
                        // FIXME: This block is duplicated, down below
                        packet_reader = incoming_packet_reader_rx.recv() => {
                            tracing::trace!(?packet_reader, "incoming socket (none)");
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
                            tracing::trace!(?packet_res, "incoming data");
                            match packet_res {
                                Some(Ok(packet)) => {
                                    incoming_packet.replace(packet);
                                }
                                Some(Err(err)) => {
                                    tracing::error!(?err, "Error multiplexing packet");
                                }
                                None => {
                                    tracing::error!("incoming reader received None");
                                }
                            }
                        }
                    ),
                    // We HAVE an incoming packet
                    Some(packet) => tokio::select!(
                        // FIXME: This block is duplicated, up above
                        packet_reader = incoming_packet_reader_rx.recv() => {
                            tracing::trace!(?packet_reader, "incoming socket (some)");
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
                        send_result = incoming_packet_sink.send(packet) => {
                            tracing::trace!(?send_result, "sending data");
                            // We have to convert the option back to None
                            incoming_packet.take();

                            if let Err(err) = send_result {
                                tracing::error!(?err, "Shutting down receive loop");
                                return;
                            }
                        }
                    ),
                }
            }
        })
    }

    #[tracing::instrument(level = "trace", skip(self, packet))]
    async fn handle_outgoing_packet(&mut self, packet: OutgoingPacket) {
        for id in packet.ids.iter().copied() {
            match &packet.message {
                OutgoingMessage::Bytes(bytes) => {
                    tracing::trace!(stream=?id, bytes=?bytes, "sending");
                    // FIXME: What should we do if there are send errors?
                    if let Err(error) = self.send(id, bytes.clone()).await {
                        tracing::error!(%error, "outgoing packet");
                    }
                }
                OutgoingMessage::ChangeChannel(channel) => {
                    tracing::trace!(?id, ?channel, "change channel");
                    self.change_channel(id, *channel);
                }
            }
        }
    }

    fn change_channel(&self, _id: StreamId, _channel: usize) {
        unimplemented!()
    }

    #[tracing::instrument(level = "trace", skip(self, stream))]
    fn handle_incoming_connection(
        &mut self,
        stream: T,
        incoming_packet_reader_tx: &mut mpsc::UnboundedSender<(usize, IncomingPacketReader<T>)>,
    ) {
        tracing::trace!(?stream, "new connection");
        // Add it to the hashmap so that we own it
        let (rx, tx): (ReadHalf<T>, WriteHalf<T>) = tokio::io::split(stream);

        // Used to send the writer to the reader when shutdown is needed
        let (writer_sender, writer_receiver) = oneshot::channel();

        // used to re-join the two halves so that we can shut down the reader
        let (halt, async_read_halt) = HaltRead::wrap(rx, writer_receiver);

        // Keep track of the write_half and generate a stream_id
        let framed_write = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_write(tx);
        let sender = Sender::new(framed_write, halt, writer_sender);
        let stream_id = self.senders.insert(sender);

        // Wrap the reader a bit more, now in a codec
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(async_read_halt);
        let reader = PacketReader::new(stream_id, framed_read);

        // Wrap the packet reader in a StreamMover

        /*
        Stream + Holds Another Stream + Can give up it's inner stream
        takes oneshot::receiver<()> and listenes for when to give up it's inner steam
        takes oneshot::sender<Stream..> and sends it's inner stream when told
        */

        // Send the PacketReader to channel zero
        if let Err(_) = incoming_packet_reader_tx.send((0, reader)) {
            tracing::error!("Error enqueueing incoming connection");
        }
    }

    #[tracing::instrument(level = "trace", skip(stream_id))]
    pub async fn close_stream(&mut self, stream_id: StreamId) -> IoResult<()> {
        tracing::trace!(%stream_id, "attempt to close stream");
        // is it in the map?
        match self.senders.remove(&stream_id) {
            // borrow the stream from the writer
            Some(sender) => sender.shutdown().await,
            None => Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Trying to remove a stream that does not exist: {}",
                    stream_id
                ),
            )),
        }
    }

    #[tracing::instrument(level = "trace", skip(stream_id, bytes))]
    pub async fn send(&mut self, stream_id: StreamId, bytes: Bytes) -> IoResult<()> {
        tracing::trace!(%stream_id, ?bytes, "sending");
        match self.senders.get_mut(stream_id) {
            None => Err(Error::new(
                ErrorKind::Other,
                format!("Sending to non-existent stream: {}", stream_id),
            )),
            Some(sender) => sender.send(bytes).await,
        }
    }
}
