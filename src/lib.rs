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
    pub id: StreamId,
    /// Value received from a stream
    pub value: V,
}

impl<V> std::fmt::Debug for IncomingMessage<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingMessage")
            .field("id", &self.id)
            .finish()
    }
}

impl<V> IncomingMessage<V> {
    pub(crate) fn new(id: StreamId, value: V) -> Self {
        Self { id, value }
    }
}

/// A packet representing a message from a stream.
pub enum IncomingPacket<V> {
    /// The stream with ID has gone linkdead.
    Linkdead(StreamId),

    /// The stream has produced a message.
    Message(IncomingMessage<V>),
}

impl<V> IncomingPacket<V> {
    /// Return the ID of the stream that the packet represents.
    pub fn id(&self) -> StreamId {
        match self {
            IncomingPacket::Message(IncomingMessage { id, .. }) => *id,
            IncomingPacket::Linkdead(id) => *id,
        }
    }

    /// If there is a value, return a reference to it
    pub fn value(&self) -> Option<&V> {
        match self {
            IncomingPacket::Message(IncomingMessage { value, .. }) => Some(value),
            _ => None,
        }
    }
}
impl<V> From<IncomingMessage<V>> for IncomingPacket<V> {
    fn from(message: IncomingMessage<V>) -> Self {
        Self::Message(message)
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
pub struct OutgoingMessage<V> {
    ids: Vec<StreamId>,
    value: V,
}
impl<V> OutgoingMessage<V> {
    /// Creates a new message that is to be delivered to streams with `ids`.
    pub fn new(ids: Vec<StreamId>, value: V) -> Self {
        Self { ids, value }
    }
}

impl<V> std::fmt::Debug for OutgoingMessage<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingMessage")
            .field("ids", &self.ids)
            .finish()
    }
}
impl<V> Unpin for OutgoingMessage<V> where V: Unpin {}

/// For sending a message or causing the stream to change to a different channel
pub enum OutgoingPacket<V> {
    /// Message to send to the stream
    Message(OutgoingMessage<V>),

    /// Change change of stream_id to channel_id.
    ChangeChannel(Vec<StreamId>, usize),

    /// Shutdown the stream
    Shutdown(Vec<StreamId>),
}
impl<V> std::fmt::Debug for OutgoingPacket<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutgoingPacket::Message(message) => write!(f, "OutgoingPacket::Message({:?})", message),
            OutgoingPacket::ChangeChannel(ids, channel) => {
                write!(f, "OutgoingPacket::ChangeChannel({:?}, {})", ids, channel)
            }
            OutgoingPacket::Shutdown(ids) => write!(f, "OutgoingPacket::Shutdown({:?})", ids),
        }
    }
}

impl<V> From<OutgoingMessage<V>> for OutgoingPacket<V> {
    fn from(message: OutgoingMessage<V>) -> Self {
        Self::Message(message)
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
