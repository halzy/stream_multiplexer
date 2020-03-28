use crate::IncomingStream;

use futures::stream::TryStream;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use std::pin::Pin;
use std::task::Poll;

/// Takes a Stream<Item=AsyncRead + AsyncWrite> and provides a
/// Stream<Item=IncomingStream<FramedRead<ReadHalf, LengthDelimitedCodec, FramedWrite<WriteHalf, LengthDelimitedCodec>>>
#[derive(Debug)]
pub struct HalvesStream<St> {
    inner: St,
    length_field_length: usize,
}

impl<St> HalvesStream<St> {
    /// Takes a TcpListener to help own the listener while producing TcpStreams
    pub fn new(inner: St, length_field_length: usize) -> Self {
        Self {
            inner,
            length_field_length,
        }
    }
}

impl<St> Stream for HalvesStream<St>
where
    St: TryStream<Error = std::io::Error> + Unpin,
    St::Ok: AsyncRead + AsyncWrite,
{
    type Item = Result<
        IncomingStream<
            FramedRead<ReadHalf<St::Ok>, LengthDelimitedCodec>,
            FramedWrite<WriteHalf<St::Ok>, LengthDelimitedCodec>,
        >,
        St::Error,
    >;
    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> Poll<Option<Self::Item>> {
        match futures::ready!(Pin::new(&mut self.inner).try_poll_next(ctx)) {
            None => None.into(),
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            Some(Ok(stream)) => {
                let (reader, writer) = tokio::io::split(stream);

                // Wrap the writer in a FramedCodec
                let framed_write = LengthDelimitedCodec::builder()
                    .length_field_length(self.length_field_length)
                    .new_write(writer);

                let framed_read = LengthDelimitedCodec::builder()
                    .length_field_length(self.length_field_length)
                    .new_read(reader);

                let channel = 0;
                let incoming_stream = IncomingStream::new(channel, framed_write, framed_read);

                Poll::Ready(Some(Ok(incoming_stream)))
            }
        }
    }
}
