use futures::stream::StreamExt;
use tokio::io::{ReadHalf, WriteHalf};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

use std::task::Poll;

/// Used to own a Stream that produces AsyncRead + AsyncWrite.
#[derive(Debug)]
pub struct TestStreamProducer<T> {
    inner: T,
}
impl<T> TestStreamProducer<T> {
    /// Takes a Stream to help own the listener while producing AsyncRead + AsyncWrite.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T, U> futures::stream::Stream for TestStreamProducer<T>
where
    T: futures::stream::Stream<Item = Result<U, std::io::Error>> + Unpin,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    type Item = Result<
        (
            FramedWrite<WriteHalf<U>, LengthDelimitedCodec>,
            FramedRead<ReadHalf<U>, LengthDelimitedCodec>,
        ),
        std::io::Error,
    >;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> Poll<Option<Self::Item>> {
        match futures::ready!(self.inner.poll_next_unpin(ctx)) {
            None => Poll::Ready(None),
            Some(Ok(stream)) => {
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
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
        }
    }
}
