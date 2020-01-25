use byteorder::{BigEndian, ByteOrder};
use bytes::buf::BufMut;
use bytes::{Buf, Bytes, BytesMut};
use futures::sink::SinkExt;
use futures::stream::{SelectAll, StreamExt};
use tokio::io::{AsyncWriteExt as _, ReadHalf, WriteHalf};
use tokio::stream::Stream;
use tokio::sync::oneshot;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use std::collections::HashMap;
use std::io::Result as IoResult;
use std::io::{Error, ErrorKind};

mod stream_control;
use stream_control::*;

/*
fn listen_address() -> impl tokio::net::ToSocketAddrs + std::fmt::Debug {
    if cfg!(test) {
        // the :0 gives us a random port, chosen by the OS
        "127.0.0.1:0".to_string()
    } else {
        env::var("LISTEN_ADDR")
            .expect("LISTEN_ADDR missing from environment.")
            .parse::<String>()
            .expect("LISTEN_ADDR should be a string: 127.0.0.1:12345")
    }
}
*/

type StreamId = usize;
type Readers<T> = SelectAll<FramedRead<HaltAsyncRead<T>, PacketReader>>;

struct Sender<T, C> {
    sender: FramedWrite<WriteHalf<T>, C>,
    read_halt: HaltRead,
    writer_tx: oneshot::Sender<WriteHalf<T>>,
}
impl<T, C> Sender<T, C> {
    pub fn new(
        sender: FramedWrite<WriteHalf<T>, C>,
        read_halt: HaltRead,
        writer_tx: oneshot::Sender<WriteHalf<T>>,
    ) -> Self {
        Self {
            sender,
            read_halt,
            writer_tx,
        }
    }
}
impl<T, C> Sender<T, C>
where
    T: tokio::io::AsyncWrite + Unpin + std::fmt::Debug,
    C: Encoder<Item = BytesPacket, Error = std::io::Error>,
{
    pub async fn shutdown(self) -> IoResult<()> {
        let mut write_half = self.sender.into_inner();

        self.read_halt.signal();
        let result = write_half.shutdown().await;
        self.writer_tx
            .send(write_half)
            .expect("Should be able to send.");

        result
    }

    pub async fn send(&mut self, packet: BytesPacket) -> IoResult<()> {
        self.sender.send(packet).await
    }
}

pub struct PacketMultiplexer<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    readers: Readers<T>,
    prev_stream_id: StreamId,
    senders: HashMap<StreamId, Sender<T, PacketWriter>>,
}
impl<T> std::fmt::Debug for PacketMultiplexer<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketMultiplexer ").finish()
    }
}
impl<T> Default for PacketMultiplexer<T>
where
    T: StreamShutdown + tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn default() -> Self {
        let readers = SelectAll::new();
        Self {
            readers,
            prev_stream_id: Default::default(),
            senders: Default::default(),
        }
    }
}

impl<T> PacketMultiplexer<T>
where
    T: StreamShutdown + tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    pub fn new() -> Self {
        Default::default()
    }
}

pub enum ControlMessage {
    Shutdown,
}

impl<T> PacketMultiplexer<T>
where
    T: StreamShutdown + tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Send + Unpin + std::fmt::Debug,
{
    pub async fn run<I, U>(&mut self, incoming: I, control: U) -> IoResult<()>
    where
        I: Stream<Item = IoResult<T>>,
        U: Stream<Item = ControlMessage>,
    {
        tracing::info!("Waiting for connections");

        let incoming_fused = incoming.fuse();
        let control_fused = control.fuse();

        futures::pin_mut!(incoming_fused);
        futures::pin_mut!(control_fused);

        loop {
            futures::select!(
                control_message_opt = control_fused.next() => {
                    if let Some(control_message) = control_message_opt {
                        match control_message {
                            ControlMessage::Shutdown => { return Ok(()); }
                        }
                    }
                }
                stream_opt = incoming_fused.next() => {
                    if let Some(stream) = stream_opt {
                        self.handle_incoming_connection(stream);
                    }
                }
            )
        }
    }

    fn handle_incoming_connection(&mut self, incoming_res: Result<T, std::io::Error>) {
        match incoming_res {
            Ok(stream) => {
                tracing::trace!("new stream! {:?}", &stream);
                // Add it to the hashmap so that we own it
                let stream_id = self.next_stream_id();
                let (rx, tx): (ReadHalf<T>, WriteHalf<T>) = tokio::io::split(stream);
                let (writer_sender, writer_receiver) = oneshot::channel();

                let (halt, async_read_halt) = HaltRead::wrap(rx, writer_receiver);

                let framed_write = FramedWrite::new(tx, PacketWriter {});
                let framed_read = FramedRead::new(async_read_halt, PacketReader { stream_id });

                self.readers.push(framed_read);

                let sender = Sender::new(framed_write, halt, writer_sender);
                self.senders.insert(stream_id, sender);
            }
            Err(error) => {
                tracing::error!("ERROR: {}", error);
            }
        }
    }

    /// Find the next available StreamId
    fn next_stream_id(&mut self) -> StreamId {
        loop {
            let next_id = self.prev_stream_id.wrapping_add(1);
            if !self.senders.contains_key(&next_id) {
                self.prev_stream_id = next_id;
                return next_id;
            }
        }
    }

    pub async fn close_stream(&mut self, stream_id: StreamId) -> IoResult<()> {
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

    pub async fn send(&mut self, packet: BytesPacket) -> IoResult<()> {
        match self.senders.get_mut(&packet.stream_id) {
            None => Err(Error::new(
                ErrorKind::Other,
                format!("Sending to non-existent stream: {}", packet.stream_id),
            )),
            Some(sender) => sender.send(packet).await,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct BytesPacket {
    stream_id: Vec<StreamId>,
    bytes: Bytes,
}

#[derive(Copy, Clone, PartialEq, Debug)]
struct PacketReader {
    stream_id: StreamId,
}

impl Decoder for PacketReader {
    type Item = BytesPacket;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        tracing::trace!("decode: {:#?}", src);
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        // Reduce the number of allocations by reserving the size of the packet
        // size = packet data, 2 = packet size header, 2 = next packet size header
        src.reserve(size + 2 + 2);

        if src.len() >= (size + 2) {
            src.advance(2); // Remove Packet Size
            let bytes = src.split_to(size).freeze();

            let packet = BytesPacket {
                stream_id: self.stream_id,
                bytes,
            };

            Ok(Some(packet))
        } else {
            Ok(None)
        }
    }
}

struct PacketWriter {}
impl Encoder for PacketWriter {
    type Item = BytesPacket;
    type Error = std::io::Error;

    fn encode(&mut self, packet: BytesPacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        tracing::trace!("encode: {:#?}", packet);
        let length = packet.bytes.len();
        dst.reserve(length + 2); // +2 for size header
        dst.put_u16(length as u16); // Header
        dst.put_slice(&packet.bytes); // Bytes
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream;
    use tokio::net::TcpListener;

    pub async fn bind() -> IoResult<TcpListener> {
        tracing::info!("Starting");
        let addrs = "127.0.0.1:0".to_string();
        tracing::info!("Binding to {:?}", &addrs);
        TcpListener::bind(&addrs).await
    }

    #[tokio::test(basic_scheduler)]
    async fn shutdown() {
        let mut socket = bind().await.unwrap();
        let control_stream = stream::once(async { ControlMessage::Shutdown });
        let mut tcp_streams = PacketMultiplexer::new();
        let shutdown_status =
            tokio::task::spawn(
                async move { tcp_streams.run(socket.incoming(), control_stream).await },
            );

        assert!(shutdown_status.await.is_ok());
    }

    #[tokio::test(basic_scheduler)]
    async fn socket_shutdown() {
        let mut socket = bind().await.unwrap();
        let local_addr = socket.local_addr().unwrap();

        let mut stream = tokio::net::TcpStream::connect(local_addr).await.unwrap();
        let (_rx, _tx) = stream.split();
        stream.shutdown(std::net::Shutdown::Both).unwrap();

        let control_stream = stream::once(async { ControlMessage::Shutdown });
        let mut tcp_streams = PacketMultiplexer::new();
        let shutdown_status =
            tokio::task::spawn(
                async move { tcp_streams.run(socket.incoming(), control_stream).await },
            );

        assert!(shutdown_status.await.is_ok());
    }
}
