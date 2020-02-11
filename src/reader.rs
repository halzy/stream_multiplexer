use crate::{IncomingMessage, IncomingPacket, StreamId};

use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps the incoming stream data in a structure with it's stream_id
#[derive(Copy, Clone, PartialEq, Debug)]
pub(crate) struct PacketReader<St> {
    stream_id: StreamId,
    inner: St,
}
impl<St> PacketReader<St> {
    pub(crate) fn new(stream_id: StreamId, inner: St) -> Self {
        Self { stream_id, inner }
    }
}

impl<St> futures::stream::Stream for PacketReader<St>
where
    St: TryStream + Unpin,
{
    type Item = Result<IncomingPacket<St::Ok>, St::Error>;

    #[tracing::instrument(level = "trace", skip(self, ctx))]
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let inner_pin = Pin::new(&mut self.inner);
        tracing::trace!("poll_next()");
        match futures::ready!(inner_pin.poll_next(ctx)) {
            None => None.into(),
            Some(Ok(bytes)) => {
                tracing::trace!(?bytes, "reading");
                Some(Ok(IncomingPacket {
                    id: self.stream_id,
                    message: IncomingMessage::Value(bytes.freeze()),
                }))
                .into()
            }
            Some(Err(error)) => Some(Err(error)).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::stream::StreamExt;
    use tokio_util::codec::*;

    #[tokio::test(basic_scheduler)]
    async fn read_zero() {
        let buffer: Vec<u8> = vec![];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);
        let result = framed_read.next().await;
        assert!(result.is_none());
    }

    #[tokio::test(basic_scheduler)]
    async fn read_one() {
        let buffer = vec![1_u8];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);
        let result = framed_read.next().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test(basic_scheduler)]
    async fn read_two() {
        // Tell it that we have data of 1 byte
        let buffer = vec![0_u8, 1];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);
        let result = framed_read.next().await;
        assert!(result.is_none());
    }

    #[tokio::test(basic_scheduler)]
    async fn read_message() {
        // Tell it that we have data of 1 byte
        let buffer = vec![0_u8, 1, 0];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);
        let result = framed_read.next().await.unwrap().unwrap();
        assert_eq!(
            IncomingMessage::Bytes(Bytes::from(vec![0_u8])),
            result.message
        );
    }

    #[tokio::test(basic_scheduler)]
    async fn read_message_and_partial_header() {
        // Tell it that we have data of 1 byte and the beginning of a packet
        let buffer = vec![0_u8, 1, 0xFF, 0];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);

        // Read the 1st packet
        let result = framed_read.next().await.unwrap().unwrap();
        assert_eq!(
            IncomingMessage::Bytes(Bytes::from(vec![0xFF_u8])),
            result.message
        );

        // Read the second
        let result = framed_read.next().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test(basic_scheduler)]
    async fn read_message_and_partial_message() {
        // Tell it that we have data of 1 byte and the beginning of a packet
        let buffer = vec![0_u8, 1, 0xFF, 0, 2, 0xFF];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);

        // Read the 1st packet
        let result = framed_read.next().await.unwrap().unwrap();
        assert_eq!(
            IncomingMessage::Bytes(Bytes::from(vec![0xFF_u8])),
            result.message
        );

        // Read the second
        let result = framed_read.next().await.unwrap();
        dbg!(&result);
        assert!(result.is_err());
    }

    #[tokio::test(basic_scheduler)]
    async fn read_two_messages() {
        // Tell it that we have data of 1 byte and the beginning of a packet
        let buffer = vec![0_u8, 1, 0xFF, 0, 1, 0xFF];
        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);

        // Read the 1st packet
        let result = framed_read.next().await.unwrap().unwrap();
        assert_eq!(
            IncomingMessage::Bytes(Bytes::from(vec![0xFF_u8])),
            result.message
        );

        // Read the second packet
        let result = framed_read.next().await.unwrap().unwrap();
        assert_eq!(
            IncomingMessage::Bytes(Bytes::from(vec![0xFF_u8])),
            result.message
        );
    }

    #[tokio::test(basic_scheduler)]
    async fn read_two_large_messages() {
        // Ensuring that we can handle packets that are 32k in size, twice
        // Tell it that we have data of 1 byte and the beginning of a packet
        let mut buffer = vec![0xFF_u8; 2 << 15]; // 64k
        buffer[0] = 0x7F;
        buffer[1] = 0xFE;
        buffer[2 << 14] = 0x7F;
        buffer[(2 << 14) + 1] = 0xFE;

        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(buffer.as_slice());
        let mut framed_read = PacketReader::new(1234, framed_read);

        // 2 for the size header
        let payload_size = (2 << 14) - 2;

        // Read the 1st packet
        let result = framed_read.next().await.unwrap().unwrap();
        match result.message {
            IncomingMessage::Bytes(data) => {
                assert_eq!(payload_size, data.len());
                assert_eq!(Bytes::from(vec![0xFF_u8; payload_size]), data);
            }
            _ => panic!("should have bytes"),
        }

        // Read the second packet
        let result = framed_read.next().await.unwrap().unwrap();
        match result.message {
            IncomingMessage::Value(data) => {
                assert_eq!(payload_size, data.len());
                assert_eq!(Bytes::from(vec![0xFF_u8; payload_size]), data);
            }
            _ => panic!("should have bytes"),
        }
    }
}
