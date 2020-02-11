use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

use std::task::Poll;

/// Used to own a TcpListener and provide a Stream of incoming TcpStream
#[derive(Debug)]
pub struct TcpStreamProducer {
    inner: tokio::net::TcpListener,
}
impl TcpStreamProducer {
    /// Takes a TcpListener to help own the listener while producing TcpStreams
    pub fn new(inner: tokio::net::TcpListener) -> Self {
        Self { inner }
    }
}

impl futures::Stream for TcpStreamProducer {
    type Item = Result<
        (
            FramedWrite<WriteHalf<TcpStream>, LengthDelimitedCodec>,
            FramedRead<ReadHalf<TcpStream>, LengthDelimitedCodec>,
        ),
        std::io::Error,
    >;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> Poll<Option<Self::Item>> {
        let (stream, _sockaddr) = futures::ready!(self.inner.poll_accept(ctx))?;

        let (reader, writer) = tokio::io::split(stream);

        // Wrap the writer in a FramedCodec
        let framed_write = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_write(writer);

        let framed_read = LengthDelimitedCodec::builder()
            .length_field_length(2)
            .new_read(reader);

        Poll::Ready(Some(Ok((framed_write, framed_read))))
    }
}
