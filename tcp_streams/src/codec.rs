use crate::{IncomingPacket, StreamId};

use byteorder::{BigEndian, ByteOrder};
use bytes::buf::BufMut;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub(crate) struct PacketWriter {}
impl Encoder for PacketWriter {
    type Item = Bytes;
    type Error = std::io::Error;

    fn encode(&mut self, bytes: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        tracing::trace!("encode: {:#?}", bytes);
        let length = bytes.len();
        dst.reserve(length + 2); // +2 for size header
        dst.put_u16(length as u16); // Header
        dst.put_slice(&bytes); // Bytes
        Ok(())
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub(crate) struct PacketReader {
    stream_id: StreamId,
}
impl PacketReader {
    pub fn new(stream_id: StreamId) -> Self {
        Self { stream_id }
    }
}

impl Decoder for PacketReader {
    type Item = IncomingPacket;
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

            let packet = IncomingPacket {
                stream_id: self.stream_id,
                bytes,
            };

            Ok(Some(packet))
        } else {
            Ok(None)
        }
    }
}
