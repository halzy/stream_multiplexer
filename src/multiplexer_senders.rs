use super::{IdGen, IncrementIdGen, Sender, StreamId};

use std::collections::HashMap;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;

type MultiplexerSender<T> = Sender<T, LengthDelimitedCodec>;

/// Stores MultiplexerSender and provides a generated ID
pub struct MultiplexerSenders<T, I: IdGen = IncrementIdGen> {
    id_gen: I,
    senders: HashMap<StreamId, MultiplexerSender<T>>,
}
impl<T, I> MultiplexerSenders<T, I>
where
    T: std::fmt::Debug,
    I: IdGen,
{
    pub fn get_mut(&mut self, stream_id: StreamId) -> Option<&mut MultiplexerSender<T>> {
        self.senders.get_mut(&stream_id)
    }

    pub fn get(&mut self, stream_id: StreamId) -> Option<&MultiplexerSender<T>> {
        self.senders.get(&stream_id)
    }

    #[tracing::instrument(level = "trace", skip(stream_id))]
    pub fn remove(&mut self, stream_id: &StreamId) -> Option<MultiplexerSender<T>> {
        tracing::trace!(%stream_id, "removing");
        self.senders.remove(stream_id)
    }

    #[tracing::instrument(level = "trace", skip(sender))]
    pub fn insert(&mut self, sender: MultiplexerSender<T>) -> StreamId {
        loop {
            let id = self.id_gen.next();

            if !self.senders.contains_key(&id) {
                tracing::trace!(stream_id=%id, "inserting");
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

impl<T, I> std::fmt::Debug for MultiplexerSenders<T, I>
where
    I: IdGen,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexerSenders").finish()
    }
}
