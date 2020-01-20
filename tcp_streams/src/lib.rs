use metrics::EventSender;

use byteorder::{BigEndian, ByteOrder};
use bytes::buf::BufMut;
use bytes::{Buf, BytesMut};
use futures::prelude::*;
use futures::stream::SelectAll;
use tokio::net::tcp::ReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use std::env;

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

type Readers<'a> = SelectAll<FramedRead<ReadHalf<'a>, PacketReader>>;

pub struct TcpStreams<'a> {
    metrics_sender: EventSender,
    readers: Readers<'a>,
    listener: Option<TcpListener>,
}
impl<'a> std::fmt::Debug for TcpStreams<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpStreams").finish()
    }
}

impl<'a> TcpStreams<'a> {
    pub fn new(metrics_sender: EventSender) -> Self {
        let readers = SelectAll::new();
        Self {
            metrics_sender,
            readers,
            listener: None,
        }
    }
}

impl<'a> TcpStreams<'a> {
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
        mut self,
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
                    self.handle_incoming(incoming_res);
                }
            }
        }
    }

    fn handle_incoming(&mut self, incoming_res: Option<Result<TcpStream, std::io::Error>>) {
        match incoming_res {
            Some(Ok(mut stream)) => {
                tracing::trace!("new stream! {:?}", &stream);
                let (rx, tx) = stream.split();
                let framed_read = FramedRead::new(rx, PacketReader {});
                let _framed_write = FramedWrite::new(tx, PacketWriter {});
                self.readers.push(framed_read);
            }
            Some(Err(error)) => {
                tracing::error!("ERROR: {}", error);
            }
            None => unreachable!(),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
struct BytesPacket {
    packet_type: u8,
    bytes: BytesMut,
}

#[derive(Copy, Clone, PartialEq, Debug)]
struct PacketReader {}
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

        if src.len() >= (size + 2) {
            src.advance(2); // Remove Packet Size
            let packet_type = src[0] as u8;
            src.advance(1); // Remove Packet Type
            let bytes = src.split_to(size - 1); // -1 for previously removed packet type

            let packet = BytesPacket { packet_type, bytes };

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
        let length = packet.bytes.len() + 1; // +1 for type
        dst.reserve(length + 2); // +2 for size header
        dst.put_u16(length as u16); // Header
        dst.put_u8(packet.packet_type); // Type
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
        let (metrics_sender, _metrics_receiver) = metrics::channel(100);

        let mut tcp_streams = TcpStreams::new(metrics_sender);
        let _socket_addr = tcp_streams.bind().await.unwrap();
        let shutdown_status = tcp_streams.start(shutdown_rx).await.unwrap();

        let _ = shutdown_tx.send(());
        assert_eq!(Shutdown::Normal, shutdown_status);
    }

    #[tokio::test(basic_scheduler)]
    async fn socket_shutdown() {
        // Checking if we can shutdown a socket after splitting it.
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (metrics_sender, _metrics_receiver) = metrics::channel(100);

        let mut tcp_streams = TcpStreams::new(metrics_sender);
        let socket_addr = tcp_streams.bind().await.unwrap();
        let shutdown_status = tcp_streams.start(shutdown_rx).await.unwrap();

        let mut stream = tokio::net::TcpStream::connect(socket_addr).await.unwrap();
        let (_rx, _tx) = stream.split();
        stream.shutdown(std::net::Shutdown::Both).unwrap();

        let _ = shutdown_tx.send(());
        assert_eq!(Shutdown::Normal, shutdown_status);
    }
}
