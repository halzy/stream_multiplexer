use crate::{ChannelId, StreamId};

use either::Either;
use std::iter::FromIterator;

/// Produced by the incoming stream
#[derive(Clone, PartialEq, Debug)]
pub struct IncomingMessage<V> {
    /// Stream Id that the message if for
    pub stream_id: StreamId,

    /// Value received from a stream
    pub value: V,
}

impl<V> IncomingMessage<V> {
    /// Creates a new IncomingMessage
    pub fn new(stream_id: StreamId, value: V) -> Self {
        Self { stream_id, value }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
/// The reason why a stream was removed from a channel.
pub enum DisconnectReason {
    /// Stream client disconnected.
    /// Could be a normal disconnect, or caused by an IO Error.
    Linkdead,

    /// Stream was moved into the given channel.
    ChannelChange(ChannelId),
}

/// A packet representing a message from a stream.
#[derive(Clone, PartialEq, Debug)]
pub enum IncomingPacket<V> {
    /// A new stream has connected to the channel.
    StreamConnected(StreamId),

    /// Stream has been removed from the channel.
    StreamDisconnected(StreamId, DisconnectReason),

    /// The stream has produced a message.
    Message(IncomingMessage<V>),
}

impl<V> IncomingPacket<V> {
    /// Return the ID of the stream that the packet represents.
    pub fn id(&self) -> StreamId {
        match self {
            IncomingPacket::Message(IncomingMessage { stream_id, .. }) => *stream_id,
            IncomingPacket::StreamConnected(stream_id) => *stream_id,
            IncomingPacket::StreamDisconnected(stream_id, _) => *stream_id,
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

/// The payload of an OutgoingPacket
#[derive(Clone, PartialEq, Debug)]
pub struct OutgoingMessage<V> {
    pub(crate) stream_ids: tinyvec::TinyVec<[Option<StreamId>; 16]>,
    pub(crate) values: tinyvec::TinyVec<[Option<V>; 16]>,
}
impl<V> OutgoingMessage<V> {
    /// Creates a new message that is to be delivered to streams with `ids`.
    pub fn new(
        stream_ids: impl IntoIterator<Item = StreamId>,
        values: impl IntoIterator<Item = V>,
    ) -> Self {
        let stream_ids = tinyvec::TinyVec::from_iter(stream_ids.into_iter().map(Some));
        let values = tinyvec::TinyVec::from_iter(values.into_iter().map(Some));
        Self { stream_ids, values }
    }
}

impl<V> Unpin for OutgoingMessage<V> where V: Unpin {}

/// For sending a message or causing the stream to change to a different channel
#[derive(Clone, PartialEq, Debug)]
pub enum OutgoingPacket<V> {
    /// Message to send to the stream
    Message(OutgoingMessage<V>),

    /// Change change of stream_id to channel_id.
    ChangeChannel(Vec<StreamId>, ChannelId),

    /// Shutdown the stream
    Shutdown(Vec<StreamId>),
}

impl<V> OutgoingPacket<V> {
    /// Return the ID of the stream that the packet represents.
    pub fn stream_ids(&self) -> Iter<StreamId> {
        match self {
            OutgoingPacket::Message(OutgoingMessage { stream_ids, .. }) => Iter {
                inner: Either::Left(OptionSliceIter::new(&stream_ids[..])),
            },
            OutgoingPacket::ChangeChannel(stream_ids, _) | OutgoingPacket::Shutdown(stream_ids) => {
                Iter {
                    inner: Either::Right(stream_ids.iter()),
                }
            }
        }
    }

    /// If there is a value, return a reference to it
    pub fn values(&self) -> Option<Iter<V>> {
        match self {
            OutgoingPacket::Message(OutgoingMessage { values, .. }) => Some(Iter {
                inner: Either::Left(OptionSliceIter::new(values)),
            }),
            _ => None,
        }
    }
}

/// An iterator for Stream IDs and Values
#[derive(Clone, Debug)]
pub struct Iter<'a, T> {
    inner: Either<OptionSliceIter<'a, T>, std::slice::Iter<'a, T>>,
}
impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'a, T> ExactSizeIterator for Iter<'a, T> {}

#[derive(Clone, PartialEq, Debug)]
struct OptionSliceIter<'a, V> {
    parent: &'a [Option<V>],
    position: usize,
    back_position: usize,
    size: usize,
}
impl<'a, V> OptionSliceIter<'a, V> {
    fn new(parent: &'a [Option<V>]) -> Self {
        let size = parent
            .iter()
            .position(|v| v.is_none())
            .unwrap_or(parent.len());

        Self {
            parent,
            position: 0,
            back_position: size - 1,
            size,
        }
    }
}

impl<'a, V> ExactSizeIterator for OptionSliceIter<'a, V> {}
impl<'a, V> Iterator for OptionSliceIter<'a, V> {
    type Item = &'a V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.parent.get(self.position) {
            Some(None) | None => None,
            Some(Some(next)) => {
                self.position += 1;
                Some(next)
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.size))
    }
}
impl<'a, V> DoubleEndedIterator for OptionSliceIter<'a, V> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.parent.get(self.back_position) {
            Some(None) | None => None,
            Some(Some(next)) => {
                self.back_position -= 1;
                Some(next)
            }
        }
    }
}

impl<V> From<OutgoingMessage<V>> for OutgoingPacket<V> {
    fn from(message: OutgoingMessage<V>) -> Self {
        Self::Message(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outgoing_iters_small() {
        let message = OutgoingMessage::new(vec![1, 2, 3], vec![4, 5, 6]);
        let packet: OutgoingPacket<u8> = message.into();

        let ids: Vec<usize> = packet.stream_ids().cloned().collect();
        assert_eq!(ids, vec![1, 2, 3]);

        let values: Vec<u8> = packet.values().unwrap().copied().collect();
        assert_eq!(values, vec![4, 5, 6]);
    }

    #[test]
    fn outgoing_iters_large() {
        let expected_ids: Vec<usize> = (0..20_usize).into_iter().collect();
        let expected_values: Vec<u8> = (20..40_u8).into_iter().collect();
        let message = OutgoingMessage::new(expected_ids.clone(), expected_values.clone());
        let packet: OutgoingPacket<u8> = message.into();

        let actual_ids: Vec<usize> = packet.stream_ids().cloned().collect();
        assert_eq!(actual_ids, expected_ids);

        let actual_values: Vec<u8> = packet.values().unwrap().copied().collect();
        assert_eq!(actual_values, expected_values);
    }
}
