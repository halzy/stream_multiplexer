/* FIXME
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
#![cfg_attr(debug_assertions, allow(dead_code))]
#![cfg_attr(test, allow(dead_code))]
*/
mod error;
mod halt;
mod id_gen;
mod multiplexer;
mod multiplexer_senders;
mod send_all_own;
mod sender;
mod stream_mover;
mod stream_producer;

pub use error::*;
use halt::*;
pub use id_gen::*;
pub use multiplexer::*;
pub use multiplexer_senders::*;
use send_all_own::*;
use sender::*;
use stream_mover::*;
pub use stream_producer::*;

type StreamId = usize;

/// Produced by the incoming stream
#[derive(Clone, PartialEq, Debug)]
pub enum IncomingMessage<V> {
    /// Value received from a stream
    Value(V),
    /// Sent when the stream has gone linkdead
    Linkdead,
}
/// A packet representing a message for a stream
#[derive(Clone, PartialEq, Debug)]
pub struct IncomingPacket<V> {
    id: StreamId,
    message: IncomingMessage<V>,
}
impl<V> IncomingPacket<V> {
    pub fn new(id: StreamId, message: IncomingMessage<V>) -> Self {
        Self { id, message }
    }

    /// The id the message is from.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// The payload of the message.
    pub fn message(&self) -> &IncomingMessage<V> {
        &self.message
    }

    /// If the message has a value, returns `Some(value)`, otherwise `None`
    pub fn value(&self) -> Option<&V> {
        match &self.message {
            IncomingMessage::Value(value) => Some(value),
            _ => None,
        }
    }
}

/// The payload of an OutgoingPacket
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum OutgoingMessage<V> {
    /// Value to send to the stream
    Value(V),
    /// Which channel to change to
    ChangeChannel(usize),
    /// Shutdown the socket
    Shutdown,
}
impl<V> Unpin for OutgoingMessage<V> where V: Unpin {}

/// For sending Value or causing the stream to change to a different channel
#[derive(Clone, PartialEq, Debug)]
pub struct OutgoingPacket<V> {
    /// List of streams this packet is for.
    ids: Vec<StreamId>,
    /// The packet payload
    message: OutgoingMessage<V>,
}
impl<V> OutgoingPacket<V>
where
    V: std::fmt::Debug + PartialEq + Clone,
{
    /// Creates an OutoingPacket message for a list of streams.
    pub fn new(ids: Vec<StreamId>, message: OutgoingMessage<V>) -> Self {
        Self { ids, message }
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
