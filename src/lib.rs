//! This crate provides natural backpressure to classes of streams.
//!
//! Streams are gathered into 'channels' that can be polled via `recv()`. Channels are indpendent
//! of each other and have their own backpressure.
//!
//! ## Example
//!
//! With a TCP server you may have two different classes of connections: Authenticated and
//! Unauthenticated. By grouping each class of connection into it's own channel, you can favor the
//! Authenticated connections over the Unauthenticated. This would provide a better experience for
//! those that have been able to authenticate.
//!
//! ## Code Example
/*!
```
# use stream_multiplexer::*;
# use futures_util::stream::StreamExt;
# use tokio_util::compat::*;
#
# let fut = async move {
const CHANNEL_ONE: usize = 1;
const CHANNEL_TWO: usize = 2;

// Initialize a multiplexer
let mut multiplexer = Multiplexer::new();

// Set up the recognized channels
multiplexer.add_channel(CHANNEL_ONE);
multiplexer.add_channel(CHANNEL_TWO);

// Bind to a random port on localhost
let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
let local_addr = listener.local_addr().unwrap();

// Set up a task to add incoming connections into multiplexer
let mut incoming_multiplexer = multiplexer.clone();
smol::Task::spawn(async move {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let stream = async_io::Async::new(stream).unwrap();
                let codec = tokio_util::codec::LinesCodec::new();
                let framed = tokio_util::codec::Framed::new(stream.compat(), codec);
                let (sink, stream) = framed.split();
                let _stream_id =
                    incoming_multiplexer.add_stream_pair(sink, stream, CHANNEL_ONE);
            }
            Err(_) => unimplemented!(),
        }
    }
})
.detach();

// test clients to put into channels
let mut client_1 = std::net::TcpStream::connect(local_addr).unwrap();
let mut client_2 = std::net::TcpStream::connect(local_addr).unwrap();

let mut multiplexer_ch_1 = multiplexer.clone();

// Simple server that echos the data back to the stream and moves the stream to channel 2.
smol::Task::spawn(async move {
    while let Ok(stream_item) = multiplexer_ch_1.recv(CHANNEL_ONE).await {
        use ItemKind::*;
        match stream_item.item {
            Value(Ok(data)) => {
                // echo the data back and move it to channel 2
                multiplexer_ch_1.send(vec![stream_item.id], data).await;
                multiplexer_ch_1
                    .change_stream_channel(stream_item.id, CHANNEL_TWO)
                    .unwrap();
            }
            Value(Err(_err)) => {
                // Stream error
            }
            Connected => {
                // stream connected
            }
            Disconnected => {
                // stream disconnected
            }
            ChannelChange => {
                // stream changed to another channel
            }
        }
    }
})
.detach();
# };
#
# smol::block_on(fut);
```
*/

#![forbid(unsafe_code)]
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
    unused_qualifications,
    rust_2018_idioms
)]

mod channel;

mod error;
mod stream_dropper;

use channel::Channel;
pub use error::MultiplexerError;
use stream_dropper::*;

use async_mutex::Mutex;
use dashmap::DashMap;
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{FuturesUnordered, Stream, StreamExt};
use parking_lot::RwLock;
use sharded_slab::Slab;

use std::marker::PhantomData;
use std::sync::Arc;

/// A value returned by `Multiplexer` when a stream pair is added.
pub type StreamId = usize;

/// Used when registering channels with `Multiplexer`.
pub type ChannelId = usize;

struct ChannelChange<St> {
    pub(crate) next_channel_id: ChannelId,
    pub(crate) stream_id: StreamId,
    pub(crate) stream: St,
}

type AddStreamToChannelTx<St> = async_channel::Sender<StreamHolder<St>>;

#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub enum ItemKind<T> {
    Value(T),
    Connected,
    Disconnected,
    ChannelChange,
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct StreamItem<T> {
    pub id: StreamId,
    pub item: ItemKind<T>,
}

type Map<T> = Arc<DashMap<ChannelId, T>>;
type StreamCount = usize;

#[derive(Debug)]
struct ChannelInfo<St> {
    add_stream_channel: AddStreamToChannelTx<St>,
    channel: Arc<Mutex<Channel<St>>>,
    stream_count: StreamCount,
}

impl<St> Clone for ChannelInfo<St> {
    fn clone(&self) -> Self {
        Self {
            add_stream_channel: self.add_stream_channel.clone(),
            channel: Arc::clone(&self.channel),
            stream_count: self.stream_count,
        }
    }
}

impl<St> ChannelInfo<St> {
    fn new(
        add_stream_channel: AddStreamToChannelTx<St>,
        channel: Arc<Mutex<Channel<St>>>,
        stream_count: StreamCount,
    ) -> Self {
        Self {
            add_stream_channel,
            channel,
            stream_count,
        }
    }
}

///
#[derive(Debug)]
pub struct Multiplexer<St, Si, Item>
where
    St: 'static,
{
    stream_controls: Arc<DashMap<StreamId, StreamHolderControl>>,
    sinks: Arc<RwLock<Slab<Arc<Mutex<Si>>>>>,
    channels: Map<ChannelInfo<St>>,
    _marker: PhantomData<Item>,
}

impl<St, Si, Item> Clone for Multiplexer<St, Si, Item> {
    fn clone(&self) -> Self {
        let stream_controls = Arc::clone(&self.stream_controls);
        let sinks = Arc::clone(&self.sinks);
        let channels = Arc::clone(&self.channels);

        Self {
            stream_controls,
            sinks,
            channels,
            _marker: PhantomData,
        }
    }
}

impl<St, Si, Item> Multiplexer<St, Si, Item>
where
    St: Stream + Unpin,
    Si: Sink<Item> + Unpin,
    Si::Error: std::fmt::Debug,
    Item: Clone,
{
    /// Creates a Multiplexer
    pub fn new() -> Self {
        Self {
            channels: Default::default(),
            sinks: Default::default(),
            stream_controls: Default::default(),
            _marker: PhantomData,
        }
    }

    /// Adds a channel id to the internal table used for validation checks.
    ///
    /// Returns an error if `channel` already exists.
    pub fn add_channel(
        &mut self,
        channel_id: ChannelId,
    ) -> Result<(), MultiplexerError<Si::Error>> {
        // Ensure that the channel does not already exist
        if self.has_channel(channel_id) {
            return Err(MultiplexerError::DuplicateChannel(channel_id));
        }

        // Create the channel and store it
        let (add_tx, channel) = Channel::new(channel_id);
        let channel = Mutex::new(channel);
        // set the stream count to zero
        self.channels
            .insert(channel_id, ChannelInfo::new(add_tx, Arc::new(channel), 0));

        Ok(())
    }

    /// Returns true if the channel id exists.
    #[inline]
    pub fn has_channel(&self, channel: ChannelId) -> bool {
        self.channels.contains_key(&channel)
    }

    /// Removes a channel.
    ///
    /// It is an error to remove a channel that has streams.
    pub fn remove_channel(
        &mut self,
        channel_id: ChannelId,
    ) -> Result<(), MultiplexerError<Si::Error>> {
        self.channels
            .remove_if_take(&channel_id, |_channel_id, channel_info| {
                0 == channel_info.stream_count
            })
            .map(|_| ())
            .ok_or_else(|| MultiplexerError::ChannelNotEmpty)
    }

    /// Sends `data` to a set of `stream_ids`, waiting for them to be sent.
    pub async fn send(
        &self,
        stream_ids: impl IntoIterator<Item = StreamId>,
        data: Item,
    ) -> Vec<Result<StreamId, MultiplexerError<Si::Error>>> {
        let futures: FuturesUnordered<_> = stream_ids
            .into_iter()
            .map(|stream_id| {
                // Clone the data before moving it into the async block.
                let data = data.clone();

                // Async block is the return value.
                async move {
                    let sink = {
                        // Fetch the write-half of the stream from the slab
                        let slab_read_guard = self.sinks.read(); // read guard
                        let sink = slab_read_guard.get(stream_id); // shard guard
                        sink.map(|s| Arc::clone(&s))
                    };

                    match sink {
                        Some(sink) => {
                            // Sending data via the stream
                            sink.lock()
                                .await
                                .send(data)
                                .await
                                // to match the return type
                                .map(|()| stream_id)
                                .map_err(|err| MultiplexerError::SendError(stream_id, err))
                        }
                        None => {
                            // It's possible for the stream to not be in the slab, should be a rare case.
                            Err(MultiplexerError::UnknownStream(stream_id))
                        }
                    }
                }
            })
            .collect();

        // Waiting for all of the sends to complete
        futures.collect().await
    }

    /// Adds `stream` to the `channel` and stores `sink`.
    ///
    /// Returns a `StreamId` that represents the pair. It can be used in functions such as `send()` or `change_stream_channel()`.
    pub fn add_stream_pair(
        &mut self,
        sink: Si,
        stream: St,
        channel_id: ChannelId,
    ) -> Result<StreamId, MultiplexerError<Si::Error>> {
        // Check that we have the channel before we commit the stream to the slab.
        if !self.channels.contains_key(&channel_id) {
            return Err(MultiplexerError::UnknownChannel(channel_id));
        }

        // Add the write-half of the stream to the slab, can't return the stream if there isn't room.
        let stream_id = self
            .sinks
            .write()
            .insert(Arc::new(Mutex::new(sink)))
            .ok_or_else(move || MultiplexerError::ChannelFull(channel_id))?;

        // wrap the stream in a dropper so that it can be ejected
        let (stream_control, stream_dropper) = StreamHolderControl::wrap(stream_id, stream);
        self.stream_controls.insert(stream_id, stream_control);
        let mut stream_dropper_mover = Some(stream_dropper);

        if !self
            .channels
            .update(&channel_id, move |_k, channel_info: &ChannelInfo<St>| {
                log::debug!(
                    "Multiplexer::add_stream_pair() sending StreamId({}) to ChannelId({})",
                    stream_id,
                    channel_id
                );

                // Add the read-half of the stream to the channel
                if let Err(_err) = channel_info
                    .add_stream_channel
                    .try_send(stream_dropper_mover.take().unwrap())
                {
                    log::error!("AddTx for Channel({}) does not exist!", channel_id);
                }

                // Increment stream channel count:
                let mut channel_info = channel_info.clone();
                channel_info.stream_count += 1;
                channel_info
            })
        {
            // undo the previous insert
            self.sinks.write().remove(stream_id);
            return Err(MultiplexerError::UnknownChannel(channel_id));
        }

        Ok(stream_id)
    }

    /// Signals to the stream that it should move to a given channel.
    ///
    /// The channel change is not instantaneous. Calling `.recv()` on the stream's current channel
    /// may result in that channel receiving more of the stream's data.
    pub fn change_stream_channel(
        &self,
        stream_id: StreamId,
        channel_id: ChannelId,
    ) -> Result<(), MultiplexerError<Si::Error>> {
        // Ensure that the channel exists
        if !self.has_channel(channel_id) {
            return Err(MultiplexerError::UnknownChannel(channel_id));
        }

        // Increment the next channel's stream count this will double-count the stream until it has
        // left it's existing channel.
        self.channels.update(&channel_id, |_k, channel_info| {
            let mut channel_info = channel_info.clone();
            channel_info.stream_count += 1;
            channel_info
        });

        let control = self
            .stream_controls
            .get(&stream_id)
            .ok_or_else(|| MultiplexerError::UnknownStream(stream_id))?;

        control.change_channel(channel_id);

        Ok(())
    }

    /// Removes the stream from the multiplexer
    pub fn remove_stream(
        &mut self,
        stream_id: StreamId,
    ) -> Result<(), MultiplexerError<Si::Error>> {
        log::trace!("Attempting to remove stream {}", stream_id);

        // Removing it first from the sinks because the sinks are checked when changing channels
        let res = self.sinks.write().take(stream_id);
        log::trace!("Stream {} exists", stream_id);

        // If the stream is changing channels, it may not have a control and will be dropped in process_add_channel
        if let Some(control) = self.stream_controls.get(&stream_id) {
            log::trace!("Stream {} is getting dropped()", stream_id);
            control.drop_stream();
        }

        // the channel's stream count will be updated when the stream drops

        res.map(|_| ())
            .ok_or_else(|| MultiplexerError::UnknownStream(stream_id))
    }

    /// Receives the next packet available from a channel:
    ///
    /// Returns a `StreamItem` or an error.
    pub async fn recv(
        &mut self,
        channel_id: ChannelId,
    ) -> Result<StreamItem<St::Item>, MultiplexerError<Si::Error>> {
        log::trace!("Multiplexer::recv({})", channel_id);
        match self.channels.get(&channel_id) {
            Some(channel_guard) => {
                let channel_guard_value = channel_guard.value();
                let mut channel = channel_guard_value.channel.lock().await;

                let channel_next = channel.next().await;
                Ok(self.process_channel_next(channel_id, channel_next))
            }
            None => Err(MultiplexerError::UnknownChannel(channel_id)),
        }
    }

    fn process_channel_next(
        &mut self,
        channel_id: ChannelId,
        channel_next: channel::ChannelItem<St>,
    ) -> StreamItem<St::Item> {
        // If the stream is leaving this channel, decrement it's counter
        let item = match channel_next.item {
            channel::ChannelKind::Value(value) => ItemKind::Value(value),
            channel::ChannelKind::Connected => ItemKind::Connected,
            channel::ChannelKind::Disconnected => {
                // stream is dropping, decrement stream count in channel
                self.channels.update(&channel_id, |_k, channel_info| {
                    let mut channel_info = channel_info.clone();
                    channel_info.stream_count -= 1;
                    channel_info
                });
                ItemKind::Disconnected
            }
            channel::ChannelKind::ChannelChange(channel_change) => {
                // FIXME: change the channel
                self.process_channel_change(channel_id, channel_change);

                // stream is moving into another channel, decrement stream count in channel
                self.channels.update(&channel_id, |_k, channel_info| {
                    let mut channel_info = channel_info.clone();
                    channel_info.stream_count -= 1;
                    channel_info
                });

                ItemKind::ChannelChange
            }
        };

        StreamItem {
            id: channel_next.id,
            item,
        }
    }

    fn process_channel_change(&mut self, channel_id: ChannelId, channel_change: ChannelChange<St>) {
        let ChannelChange {
            next_channel_id,
            stream_id,
            stream,
        } = channel_change;

        // Channel should exist due to channel_stream_count
        debug_assert!(self.channels.contains_key(&next_channel_id));

        // If the sink has been dropped, don't add this half to a channel
        if self.sinks.read().contains(stream_id) {
            // Wrap the stream in another drop control
            let (stream_control, stream_dropper) = StreamHolderControl::wrap(stream_id, stream);
            self.stream_controls.insert(stream_id, stream_control);

            if let Some(channel) = self.channels.get(&next_channel_id) {
                log::debug!(
                    "process_channel_change sending StreamId({}) on ChannelId({}) to ChannelId({})",
                    stream_id,
                    channel_id,
                    next_channel_id
                );
                if let Err(_err) = channel.add_stream_channel.try_send(stream_dropper) {
                    log::error!(
                        "Channel({}) vanished while moving stream {} into it.",
                        next_channel_id,
                        stream_id
                    );
                }
            }
        } else {
            log::debug!(
                "Stream {} dropped while it was changing channels ({} to {})",
                stream_id,
                channel_id,
                next_channel_id
            );
        }
    }
}
