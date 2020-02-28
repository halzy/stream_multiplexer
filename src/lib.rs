#![warn(
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]
/* FIXME
#![cfg_attr(debug_assertions, allow(dead_code))]
#![cfg_attr(test, allow(dead_code))]
*/
/*!
This crate provides stream multiplexing with channels.

Channels have their own backpressure that does not affect other channels.

Incoming streams are by default set to channel 0 and can be moved to other channels via `ControlMessage`s.
*/
mod error;
mod halt;
mod id_gen;
mod multiplexer;
mod multiplexer_senders;
mod send_all_own;
mod sender;
mod stream_mover;

pub use error::*;
use halt::*;
pub use id_gen::*;
pub use multiplexer::*;
use multiplexer_senders::*;
use send_all_own::*;
use sender::*;
use stream_mover::*;

type StreamId = usize;

/// Produced by the incoming stream
pub struct IncomingMessage<V> {
    /// Stream Id that the message if for
    id: StreamId,
    /// Value received from a stream
    value: V,
}

impl<V> std::fmt::Debug for IncomingMessage<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingMessage")
            .field("id", &self.id)
            .finish()
    }
}

impl<V> IncomingMessage<V> {
    pub fn new(id: StreamId, value: V) -> Self {
        Self { id, value }
    }
}

/// A packet representing a message from a stream.
pub enum IncomingPacket<V> {
    Linkdead(StreamId),
    Message(IncomingMessage<V>),
}

impl<V> IncomingPacket<V> {
    pub fn id(&self) -> StreamId {
        match self {
            IncomingPacket::Message(IncomingMessage { id, .. }) => *id,
            IncomingPacket::Linkdead(id) => *id,
        }
    }
    pub fn value(&self) -> Option<&V> {
        match self {
            IncomingPacket::Message(IncomingMessage { value, .. }) => Some(value),
            _ => None,
        }
    }
}

impl<V> std::fmt::Debug for IncomingPacket<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncomingPacket::Linkdead(id) => write!(f, "IncomingPacket::Linkdead({})", id),
            IncomingPacket::Message(message) => {
                write!(f, "IncomingPacket::IncomingMessage({:?})", &message)
            }
        }
    }
}

/// The payload of an OutgoingPacket
#[derive(Clone)]
pub enum OutgoingMessage<V> {
    /// Value to send to the stream
    Value(V),
    /// Which channel to change to
    ChangeChannel(usize),
    /// Shutdown the socket
    Shutdown,
}
impl<V> Unpin for OutgoingMessage<V> where V: Unpin {}
impl<V> std::fmt::Debug for OutgoingMessage<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutgoingMessage::Value(_) => write!(f, "OutgoingMessage::Value(_)"),
            OutgoingMessage::ChangeChannel(channel) => {
                write!(f, "OutgoingMessage::ChangeChannel({})", channel)
            }
            OutgoingMessage::Shutdown => write!(f, "OutgoingMessage::Shutdown"),
        }
    }
}

/// For sending Value or causing the stream to change to a different channel
pub struct OutgoingPacket<V> {
    /// List of streams this packet is for.
    ids: Vec<StreamId>,
    /// The packet payload
    message: OutgoingMessage<V>,
}
impl<V> std::fmt::Debug for OutgoingPacket<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingPacket")
            .field("ids", &self.ids)
            .field("message", &self.message)
            .finish()
    }
}
impl<V> OutgoingPacket<V> {
    /// Creates an OutoingPacket message for a list of streams.
    pub fn new(ids: Vec<StreamId>, message: OutgoingMessage<V>) -> Self {
        Self { ids, message }
    }

    /// Creates an OutgoingPacket with OutgoingMessage::Value(value)
    pub fn with_value(ids: Vec<StreamId>, value: V) -> Self {
        Self {
            ids,
            message: OutgoingMessage::Value(value),
        }
    }
    /// Utility function to create a ChangeChannel packet.
    pub fn change_channel(id: StreamId, channel_id: usize) -> Self {
        let message = OutgoingMessage::ChangeChannel(channel_id);
        Self {
            ids: vec![id],
            message,
        }
    }
}

/// To control the multiplexer, `ControlMessage` can be sent to the `control` channel passed into
/// `Multiplexer.run()`
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ControlMessage {
    /// Shut down the `Multiplexer`
    Shutdown,
}

#[cfg(test)]
mod tests {

    use super::*;
    use futures::prelude::*;

    #[allow(dead_code)]
    pub(crate) fn init_logging() {
        use tracing_subscriber::FmtSubscriber;

        let subscriber = FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }

    pub(crate) fn sender_reader<St, Si>(sink: Si, stream: St) -> (Sender<Si>, HaltAsyncRead<St>)
    where
        St: Stream + Unpin,
        Si: Unpin,
    {
        // Wrap the reader so that it can be retrieved
        let (halt, reader) = HaltRead::wrap(stream);

        // Give the Sender the other end of the ejection channel
        let sender = Sender::new(sink, halt);

        (sender, reader)
    }
}
