use metrics::EventSender;

use byteorder::{BigEndian, ByteOrder};
use bytes::buf::BufMut;
use bytes::{Buf, BytesMut};
use futures::prelude::*;
use futures::stream::SelectAll;
use tokio::io::ReadHalf;
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

pub struct TcpStreams {
    metrics_sender: EventSender,
    readers: SelectAll<FramedRead<ReadHalf<TcpStream>, PacketReader>>,
}
impl std::fmt::Debug for TcpStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpStreams").finish()
    }
}

impl TcpStreams {
    pub fn new(metrics_sender: EventSender) -> Self {
        let readers = SelectAll::new();
        Self {
            metrics_sender,
            readers,
        }
    }
}

impl TcpStreams {
    pub async fn start(
        &mut self,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<Shutdown, std::io::Error> {
        tracing::info!("Starting");

        let addrs = listen_address();
        tracing::info!("Binding to {:?}", &addrs);
        let mut listener = TcpListener::bind(&addrs).await?;

        let mut shutdown = shutdown.fuse();

        tracing::info!("Waiting for connections");
        loop {
            let incoming_fut = listener.incoming();
            futures::pin_mut!(incoming_fut);

            futures::select! {
                _ = shutdown => {
                    tracing::warn!("shutting down!");
                    return Ok(Shutdown::Normal);
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
                let (rx, tx) = tokio::io::split(stream);
                let framed_read = FramedRead::new(rx, PacketReader {});
                let framed_write = FramedWrite::new(tx, PacketWriter {});
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
        let start_fut = tcp_streams.start(shutdown_rx);

        let _ = shutdown_tx.send(());
        let shutdown_status = start_fut.await.unwrap();
        assert_eq!(Shutdown::Normal, shutdown_status);
    }
}
