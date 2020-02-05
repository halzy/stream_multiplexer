use super::{
    ControlMessage, HaltAsyncRead, HaltRead, IdGen, IncomingPacket, IncrementIdGen,
    MultiplexerSenders, OutgoingPacket, PacketReader, Sender, StreamId, StreamShutdown,
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
    readers: Option<SelectAll<IncomingPacketReader<T>>>,
    senders: MultiplexerSenders<T, I>,
    outgoing: Out,
    incoming_packet_sink: Option<IncomingPacketSink>,
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
    pub fn new(outgoing: Out, incoming_packet_sink: IncomingPacketSink) -> Self {
        let senders = MultiplexerSenders::new(IncrementIdGen::default());
        Self::with_senders(senders, outgoing, incoming_packet_sink)
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
        incoming_packet_sink: IncomingPacketSink,
    ) -> Self {
        let readers = SelectAll::new();
        Self {
            senders,
            outgoing,
            readers: Some(readers),
            incoming_packet_sink: Some(incoming_packet_sink),
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

        let (mut incoming_packet_reader_tx, mut incoming_packet_reader_rx) =
            mpsc::unbounded_channel();

        let mut readers = self.readers.take().expect("Should have readers!");
        let mut incoming_packet_sink = self
            .incoming_packet_sink
            .take()
            .expect("Should have incoming_packet_sink!");

        tokio::task::spawn(async move {
            let mut incoming_packet: Option<IncomingPacket> = None;
            loop {
                tracing::trace!(?incoming_packet, "incoming loop start");
                match incoming_packet.clone() {
                    // We do not have an incoming packet
                    None => tokio::select!(
                        packet_reader = incoming_packet_reader_rx.recv() => {
                            tracing::trace!(?packet_reader, "incoming socket (none)");
                            match packet_reader {
                                Some(packet_reader) => {
                                    readers.push(packet_reader);
                                }
                                None => {
                                    tracing::error!("incoming packet reader received None, exiting loop");
                                    return;
                                }
                            }
                        }
                        packet_res = readers.next(), if !readers.is_empty() => {
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
                        packet_reader = incoming_packet_reader_rx.recv() => {
                            tracing::trace!(?packet_reader, "incoming socket (some)");
                            match packet_reader {
                                Some(packet_reader) => {
                                    readers.push(packet_reader);
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

    #[tracing::instrument(level = "trace", skip(self, packet))]
    async fn handle_outgoing_packet(&mut self, packet: OutgoingPacket) {
        for id in packet.ids.iter().copied() {
            tracing::trace!(stream=?id, bytes=?packet.bytes, "sending");
            // FIXME: What should we do if there are send errors?
            if let Err(error) = self.send(id, packet.bytes.clone()).await {
                tracing::error!(%error, "outgoing packet");
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, stream))]
    fn handle_incoming_connection(
        &mut self,
        stream: T,
        incoming_packet_reader_tx: &mut mpsc::UnboundedSender<IncomingPacketReader<T>>,
    ) {
        tracing::trace!(?stream, "new connection");
        // Add it to the hashmap so that we own it
        let (rx, tx): (ReadHalf<T>, WriteHalf<T>) = tokio::io::split(stream);
        let (writer_sender, writer_receiver) = oneshot::channel();

        // used to re-join the two halves so that we can shut down the reader
        let (halt, async_read_halt) = HaltRead::wrap(rx, writer_receiver);

        let framed_write = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_write(tx);
        let sender = Sender::new(framed_write, halt, writer_sender);
        let stream_id = self.senders.insert(sender);

        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(async_read_halt);
        let reader = PacketReader::new(stream_id, framed_read);
        if let Err(_) = incoming_packet_reader_tx.send(reader) {
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
