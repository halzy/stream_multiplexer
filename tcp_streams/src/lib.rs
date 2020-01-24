use byteorder::{BigEndian, ByteOrder};
use bytes::buf::BufMut;
use bytes::{Buf, BytesMut};
use futures::prelude::*;
use futures::stream::SelectAll;
use tokio::io::{AsyncWriteExt as _, ReadHalf, Result as TioResult, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use std::collections::HashMap;
use std::env;

mod stream_control;
use stream_control::*;

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

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Shutdown {
    Normal,
}

type StreamId = usize;
type Readers = SelectAll<FramedRead<HaltAsyncRead<ReadHalf<TcpStream>>, PacketReader>>;

struct Sender<W, C> {
    sender: FramedWrite<W, C>,
    read_halt: HaltRead,
}
impl<W, C> Sender<W, C> {
    pub fn new(sender: FramedWrite<W, C>, read_halt: HaltRead) -> Self {
        Self { sender, read_halt }
    }
}
impl<W, C> Sender<W, C>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    pub async fn shutdown(self) -> TioResult<()> {
        self.read_halt.signal();
        self.sender.into_inner().shutdown().await
    }
}

#[derive(Default)]
pub struct TcpStreams {
    listener: Option<TcpListener>,
    readers: Readers,
    prev_stream_id: StreamId,
    senders: HashMap<StreamId, Sender<WriteHalf<TcpStream>, PacketWriter>>,
}
impl std::fmt::Debug for TcpStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpStreams").finish()
    }
}

impl TcpStreams {
    pub fn new() -> Self {
        Default::default()
    }
}

impl TcpStreams {
    pub async fn bind(&mut self) -> Result<std::net::SocketAddr, std::io::Error> {
        tracing::info!("Starting");

        let addrs = listen_address();
        tracing::info!("Binding to {:?}", &addrs);
        let listener = TcpListener::bind(&addrs).await?;
        let local_addr = listener.local_addr();
        self.listener = Some(listener);
        local_addr
    }

    pub async fn start(
        &mut self,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<Shutdown, std::io::Error> {
        let mut shutdown = shutdown.fuse();
        let mut listener = self
            .listener
            .take()
            .expect("Should have successfully bound to a socket.");

        tracing::info!("Waiting for connections");
        loop {
            let incoming_fut = listener.incoming();
            futures::pin_mut!(incoming_fut);

            futures::select! {
                _ = shutdown => {
                    tracing::warn!("shutting down!");
                    return Ok::<_, std::io::Error>(Shutdown::Normal);
                }
                incoming_res = incoming_fut.next().fuse() => {
                    self.handle_incoming(incoming_res.expect("TcpStream.incoming() should not return None"));
                }
            }
        }
    }

    fn handle_incoming(&mut self, incoming_res: Result<TcpStream, std::io::Error>) {
        match incoming_res {
            Ok(stream) => {
                tracing::trace!("new stream! {:?}", &stream);
                // Add it to the hashmap so that we own it
                let stream_id = self.next_stream_id();
                let (rx, tx) = tokio::io::split(stream);

                let halt = HaltRead::new();
                let async_read_halt = halt.wrap(rx);

                let framed_write = FramedWrite::new(tx, PacketWriter {});
                let framed_read = FramedRead::new(async_read_halt, PacketReader { stream_id });

                self.readers.push(framed_read);

                let sender = Sender::new(framed_write, halt);
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

    pub async fn close_stream(&mut self, stream_id: StreamId) -> TioResult<()> {
        use std::io::{Error, ErrorKind};
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
}

#[derive(Clone, PartialEq, Debug)]
struct BytesPacket {
    stream_id: StreamId,
    bytes: BytesMut,
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
            let bytes = src.split_to(size);

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

    #[tokio::test(basic_scheduler)]
    async fn shutdown() {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mut tcp_streams = TcpStreams::new();
        let _socket_addr = tcp_streams.bind().await.unwrap();
        let shutdown_status = tcp_streams.start(shutdown_rx).await.unwrap();

        let _ = shutdown_tx.send(());
        assert_eq!(Shutdown::Normal, shutdown_status);
    }

    #[tokio::test(basic_scheduler)]
    async fn socket_shutdown() {
        // Checking if we can shutdown a socket after splitting it.
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mut tcp_streams = TcpStreams::new();
        let socket_addr = tcp_streams.bind().await.unwrap();
        let shutdown_status = tcp_streams.start(shutdown_rx).await.unwrap();

        let mut stream = tokio::net::TcpStream::connect(socket_addr).await.unwrap();
        let (_rx, _tx) = stream.split();
        stream.shutdown(std::net::Shutdown::Both).unwrap();

        let _ = shutdown_tx.send(());
        assert_eq!(Shutdown::Normal, shutdown_status);
    }
}
