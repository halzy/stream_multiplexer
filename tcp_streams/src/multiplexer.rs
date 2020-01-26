use super::{
    ControlMessage, HaltAsyncRead, HaltRead, PacketReader, PacketWriter, Sender, StreamId,
    StreamShutdown,
};

use bytes::Bytes;
use futures::stream::{SelectAll, StreamExt};
use std::io::Result as IoResult;
use std::io::{Error, ErrorKind};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::stream::Stream;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};

use std::collections::HashMap;

type Readers<T> = SelectAll<FramedRead<HaltAsyncRead<T>, PacketReader>>;

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
                let framed_read = FramedRead::new(async_read_halt, PacketReader::new(stream_id));

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

    pub async fn send(&mut self, stream_id: StreamId, bytes: Bytes) -> IoResult<()> {
        match self.senders.get_mut(&stream_id) {
            None => Err(Error::new(
                ErrorKind::Other,
                format!("Sending to non-existent stream: {}", stream_id),
            )),
            Some(sender) => sender.send(bytes).await,
        }
    }
}
