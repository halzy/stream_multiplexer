use super::{
    ControlMessage, HaltAsyncRead, HaltRead, IdGen, IncrementIdGen, MultiplexerSenders,
    OutgoingPacket, PacketReader, Sender, StreamId, StreamShutdown,
};

use bytes::Bytes;
use futures::stream::SelectAll;
use std::io::{Error, ErrorKind, Result as IoResult};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::stream::{Stream, StreamExt};
use tokio::sync::oneshot;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::FramedRead;

#[derive(Debug)]
pub struct TcpStreamProducer {
    inner: tokio::net::TcpListener,
}
impl TcpStreamProducer {
    pub fn new(inner: tokio::net::TcpListener) -> Self {
        Self { inner }
    }
}
impl futures::Stream for TcpStreamProducer {
    type Item = Result<tokio::net::TcpStream, std::io::Error>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Result<(stream, sockaddr), _> => Option<Result<stream, _>>
        self.inner
            .poll_accept(ctx)
            .map_ok(|(s, _)| s)
            .map(Into::into)
    }
}

pub struct PacketMultiplexer<S, T, I: IdGen = IncrementIdGen>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    S: Stream<Item = OutgoingPacket>,
{
    readers: SelectAll<PacketReader<FramedRead<HaltAsyncRead<T>, LengthDelimitedCodec>>>,
    senders: MultiplexerSenders<T, I>,
    outgoing: S,
}

impl<S, T, I> std::fmt::Debug for PacketMultiplexer<S, T, I>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    S: Stream<Item = OutgoingPacket>,
    I: IdGen,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketMultiplexer ").finish()
    }
}

impl<S, T> PacketMultiplexer<S, T>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Unpin,
    S: Stream<Item = OutgoingPacket>,
{
    pub fn new(outgoing: S) -> Self {
        let readers = SelectAll::new();
        let senders = MultiplexerSenders::default();
        Self {
            readers,
            senders,
            outgoing,
        }
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
    pub fn with_senders(senders: MultiplexerSenders<T, I>, outgoing: S) -> Self {
        let readers = SelectAll::new();
        Self {
            senders,
            outgoing,
            readers,
        }
    }
}

impl<S, T, I> PacketMultiplexer<S, T, I>
where
    T: StreamShutdown + tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Send + Unpin + std::fmt::Debug,
    I: IdGen,
    S: Stream<Item = OutgoingPacket> + Unpin,
{
    #[tracing::instrument(level = "debug", skip(incoming, control))]
    pub async fn run<V, U>(mut self, mut incoming: V, mut control: U) -> IoResult<()>
    where
        V: Stream<Item = IoResult<T>> + std::fmt::Debug + Unpin,
        U: Stream<Item = ControlMessage> + std::fmt::Debug + Unpin,
    {
        tracing::info!("Waiting for connections");

        loop {
            tokio::select!(
                incoming_opt = incoming.next() => {
                    match incoming_opt {
                        Some(Ok(stream)) => {
                            self.handle_incoming_connection(stream);
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
    fn handle_incoming_connection(&mut self, stream: T) {
        tracing::trace!(?stream, "new connection");
        // Add it to the hashmap so that we own it
        let (rx, tx): (ReadHalf<T>, WriteHalf<T>) = tokio::io::split(stream);
        let (writer_sender, writer_receiver) = oneshot::channel();

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
        self.readers.push(reader);
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
        match self.senders.get_mut(&stream_id) {
            None => Err(Error::new(
                ErrorKind::Other,
                format!("Sending to non-existent stream: {}", stream_id),
            )),
            Some(sender) => sender.send(bytes).await,
        }
    }
}
