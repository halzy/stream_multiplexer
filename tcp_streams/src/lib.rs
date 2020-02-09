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

mod stream_mover;
use stream_mover::*;

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
impl IncomingPacket {
    pub fn id(&self) -> StreamId {
        self.id
    }
    pub fn message(&self) -> &IncomingMessage {
        &self.message
    }
    pub fn bytes(&self) -> Option<&Bytes> {
        match &self.message {
            IncomingMessage::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum OutgoingMessage {
    Bytes(Bytes),
    ChangeChannel(usize),
}

#[derive(Clone, PartialEq, Debug)]
pub struct OutgoingPacket {
    ids: Vec<StreamId>,
    message: OutgoingMessage,
}
impl OutgoingPacket {
    pub fn new(ids: Vec<StreamId>, message: OutgoingMessage) -> Self {
        Self { ids, message }
    }
    pub fn change_channel(id: StreamId, channel_id: usize) -> Self {
        let message = OutgoingMessage::ChangeChannel(channel_id);
        Self {
            ids: vec![id],
            message,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ControlMessage {
    Shutdown,
}
