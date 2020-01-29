use super::{
    ControlMessage, HaltAsyncRead, HaltRead, IdGen, OutgoingPacket, PacketReader, PacketWriter,
    Sender, StreamId, StreamShutdown,
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

type MultiplexerSender<T> = Sender<T, PacketWriter>;

pub struct MultiplexerSenders<T, I: IdGen = IncrementIdGen> {
    id_gen: I,
    senders: HashMap<StreamId, MultiplexerSender<T>>,
}
impl<T, I> MultiplexerSenders<T, I>
where
    I: IdGen,
{
    pub fn get_mut(&mut self, stream_id: &StreamId) -> Option<&mut MultiplexerSender<T>> {
        self.senders.get_mut(stream_id)
    }

    pub fn get(&mut self, stream_id: &StreamId) -> Option<&MultiplexerSender<T>> {
        self.senders.get(stream_id)
    }

    pub fn remove(&mut self, stream_id: &StreamId) -> Option<MultiplexerSender<T>> {
        self.senders.remove(stream_id)
    }

    pub fn insert(&mut self, sender: MultiplexerSender<T>) -> StreamId {
        loop {
            let id = self.id_gen.next();
            eprintln!("insert: {}", &id);

            if !self.senders.contains_key(&id) {
                let res = self.senders.insert(id, sender);
                assert!(res.is_none(), "MPSender: Highly unlikely");
                break id;
            }
        }
    }
}
impl<T, I> MultiplexerSenders<T, I>
where
    I: IdGen,
{
    pub fn new(id_gen: I) -> Self {
        Self {
            id_gen,
            senders: HashMap::new(),
        }
    }
}

impl<T, I> Default for MultiplexerSenders<T, I>
where
    I: IdGen,
{
    fn default() -> Self {
        Self {
            id_gen: Default::default(),
            senders: HashMap::new(),
        }
    }
}

pub struct PacketMultiplexer<S, T, I: IdGen = IncrementIdGen>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    S: Stream<Item = OutgoingPacket>,
{
    readers: Readers<T>,
    senders: MultiplexerSenders<T, I>,
    outgoing: S,
}

impl<S, T> std::fmt::Debug for PacketMultiplexer<S, T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    S: Stream<Item = OutgoingPacket>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketMultiplexer ").finish()
    }
}

impl<S, T> PacketMultiplexer<S, T>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Unpin,
    S: Stream<Item = OutgoingPacket>,
{
    pub fn new(outgoing: S) -> Self {
        let readers = SelectAll::new();
        let senders = MultiplexerSenders::default();
        Self {
            readers,
            senders,
            outgoing,
        }
    }
}

impl<S, T, I> PacketMultiplexer<S, T, I>
where
    T: StreamShutdown,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Unpin,
    I: IdGen,
    S: Stream<Item = OutgoingPacket>,
{
    pub fn with_senders(senders: MultiplexerSenders<T, I>, outgoing: S) -> Self {
        let readers = SelectAll::new();
        Self {
            senders,
            outgoing,
            readers,
        }
    }
}

impl<S, T, I> PacketMultiplexer<S, T, I>
where
    T: StreamShutdown + tokio::io::AsyncRead + tokio::io::AsyncWrite,
    T: Send + Unpin + std::fmt::Debug,
    I: IdGen,
    S: Stream<Item = OutgoingPacket> + Unpin,
{
    pub async fn run<V, U>(&mut self, mut incoming: V, mut control: U) -> IoResult<()>
    where
        V: Stream<Item = IoResult<T>> + Unpin,
        U: Stream<Item = ControlMessage> + Unpin,
    {
        tracing::info!("Waiting for connections");

        loop {
            tokio::select!(
                incoming_opt = incoming.next() => {
                    eprintln!("incoming connection");
                    if let Some(stream) = incoming_opt {
                        self.handle_incoming_connection(stream);
                    }
                }
                outgoing_opt = self.outgoing.next() => {
                    if let Some(outgoing_packet) = outgoing_opt {
                        self.handle_outgoing_packet(outgoing_packet).await;
                    }
                }
                control_message_opt = control.next() => {
                    if let Some(control_message) = control_message_opt {
                        match control_message {
                            ControlMessage::Shutdown => { return Ok(()); }
                        }
                    }
                }
            )
        }
    }

    async fn handle_outgoing_packet(&mut self, packet: OutgoingPacket) {
        for id in packet.ids.iter().copied() {
            // FIXME: What should we do if there are send errors?
            if let Err(error) = self.send(id, packet.bytes.clone()).await {
                eprintln!("outgoing packet error: {}", error);
            }
        }
    }

    fn handle_incoming_connection(&mut self, incoming_res: Result<T, std::io::Error>) {
        match incoming_res {
            Ok(stream) => {
                tracing::trace!("new stream! {:?}", &stream);
                // Add it to the hashmap so that we own it
                let (rx, tx): (ReadHalf<T>, WriteHalf<T>) = tokio::io::split(stream);
                let (writer_sender, writer_receiver) = oneshot::channel();

                let (halt, async_read_halt) = HaltRead::wrap(rx, writer_receiver);

                let framed_write = FramedWrite::new(tx, PacketWriter {});
                let sender = Sender::new(framed_write, halt, writer_sender);
                let stream_id = self.senders.insert(sender);

                let framed_read = FramedRead::new(async_read_halt, PacketReader::new(stream_id));
                self.readers.push(framed_read);
            }
            Err(error) => {
                tracing::error!("ERROR: {}", error);
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
        eprintln!("sending to {}, {} bytes", stream_id, bytes.len());
        match self.senders.get_mut(&stream_id) {
            None => Err(Error::new(
                ErrorKind::Other,
                format!("Sending to non-existent stream: {}", stream_id),
            )),
            Some(sender) => sender.send(bytes).await,
        }
    }
}

#[derive(Default, Copy, Clone, PartialEq, Debug)]
pub struct IncrementIdGen {
    id: StreamId,
}
impl IdGen for IncrementIdGen {
    /// Find the next available StreamId
    fn next(&mut self) -> StreamId {
        self.id = self.id.wrapping_add(1);
        self.id
    }
    fn id(&self) -> StreamId {
        self.id
    }
    fn seed(&mut self, seed: StreamId) {
        self.id = seed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment() {
        let mut id = IncrementIdGen::default();

        id.seed(usize::max_value());
        assert_eq!(usize::max_value(), id.id());

        let zero = id.next();
        assert_eq!(0, zero);

        let one = id.next();
        assert_eq!(1, one);
        assert_eq!(one, id.id());
    }
}
