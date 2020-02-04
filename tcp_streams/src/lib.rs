use bytes::Bytes;

mod reader;
use reader::*;

mod halt;
use halt::*;

mod sender;
use sender::*;

mod id_gen;
pub use id_gen::*;

mod multiplexer;
pub use multiplexer::*;

mod multiplexer_senders;
pub use multiplexer_senders::*;

mod stream_producer;
pub use stream_producer::*;

type StreamId = usize;

#[derive(Clone, PartialEq, Debug)]
pub enum IncomingMessage {
    Bytes(Bytes),
    Linkdead,
}

#[derive(Clone, PartialEq, Debug)]
pub struct IncomingPacket {
    id: StreamId,
    message: IncomingMessage,
}

#[derive(Default, Clone, PartialEq, Debug)]
pub struct OutgoingPacket {
    ids: Vec<StreamId>,
    bytes: Bytes,
}
impl OutgoingPacket {
    pub fn new(ids: Vec<StreamId>, bytes: Bytes) -> Self {
        Self { ids, bytes }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ControlMessage {
    Shutdown,
}
