use super::HaltRead;

use bytes::Bytes;
use futures::sink::SinkExt;
use tokio::io::{AsyncWriteExt as _, WriteHalf};
use tokio::sync::oneshot;
use tokio_util::codec::{Encoder, FramedWrite};

use std::io::Result as IoResult;

pub(crate) struct Sender<T, C> {
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
    C: Encoder<Item = Bytes, Error = std::io::Error>,
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

    pub async fn send(&mut self, bytes: Bytes) -> IoResult<()> {
        self.sender.send(bytes).await
    }
}
