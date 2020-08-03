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
//!
//! ```
//! use futures_util::stream::StreamExt;
//! use tokio_util::compat::*;
//!
//! smol::block_on(async move {
//!     const CHANNEL_ONE: usize = 1;
//!     const CHANNEL_TWO: usize = 2;
//!
//!     // Initialize a multiplexer
//!     let mut multiplexer = stream_multiplexer::Multiplexer::new();
//!
//!     // Set up the recognized channels
//!     multiplexer.add_channel(CHANNEL_ONE);
//!     multiplexer.add_channel(CHANNEL_TWO);
//!
//!     // Bind to a random port on localhost
//!     let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
//!     let local_addr = listener.local_addr().unwrap();
//!
//!     // Set up a task to add incoming connections into multiplexer
//!     let mut incoming_multiplexer = multiplexer.clone();
//!     smol::Task::spawn(async move {
//!         for stream in listener.incoming() {
//!             match stream {
//!                 Ok(stream) => {
//!                     let stream = async_io::Async::new(stream).unwrap();
//!                     let codec = tokio_util::codec::LinesCodec::new();
//!                     let framed = tokio_util::codec::Framed::new(stream.compat(), codec);
//!                     let (sink, stream) = framed.split();
//!                     let _stream_id = incoming_multiplexer.add_stream_pair(sink, stream, CHANNEL_ONE);
//!                 }
//!                 Err(_) => unimplemented!()
//!             }
//!         }
//!     }).detach();
//!
//!     // test clients to put into channels
//!     let mut client_1 = std::net::TcpStream::connect(local_addr).unwrap();
//!     let mut client_2 = std::net::TcpStream::connect(local_addr).unwrap();
//!
//!     let mut multiplexer_ch_1 = multiplexer.clone();
//!
//!     // Simple server that echos the data back to the stream and moves the stream to channel 2.
//!     smol::Task::spawn(async move {
//!         while let Ok((stream_id, message)) = multiplexer_ch_1.recv(CHANNEL_ONE).await {
//!             match message {
//!                 Some(Ok(data)) => {
//!                     // echo the data back and move it to channel 2
//!                     multiplexer_ch_1.send(vec![stream_id], data);
//!                     multiplexer_ch_1.change_stream_channel(stream_id, CHANNEL_TWO);
//!                 }
//!                 Some(Err(err)) => {
//!                     // the stream had an error
//!                 }
//!                 None => {
//!                     // stream_id has been dropped
//!                 }
//!             }
//!         }
//!     }).detach();
//! });
//! ```

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

use crate::stream_dropper::*;
use channel::Channel;
pub use error::MultiplexerError;

use async_mutex::Mutex;
use dashmap::DashMap;
use futures_util::future::FutureExt;
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{FuturesUnordered, StreamExt, TryStream};
use parking_lot::RwLock;
use sharded_slab::Slab;

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

type AddStreamToChannelTx<St> = async_channel::Sender<StreamDropper<St>>;

///
#[derive(Debug)]
pub struct Multiplexer<St, Si>
where
    St: 'static,
{
    stream_controls: Arc<DashMap<StreamId, StreamDropControl>>,
    sinks: Arc<RwLock<Slab<Arc<Mutex<Si>>>>>,
    channels: Arc<DashMap<ChannelId, (AddStreamToChannelTx<St>, Mutex<Channel<St>>)>>,
    channel_change_rx: async_channel::Receiver<ChannelChange<St>>,
    channel_change_tx: async_channel::Sender<ChannelChange<St>>,
}

impl<St, Si> Clone for Multiplexer<St, Si> {
    fn clone(&self) -> Self {
        let stream_controls = Arc::clone(&self.stream_controls);
        let sinks = Arc::clone(&self.sinks);
        let channels = Arc::clone(&self.channels);
        let channel_change_rx = self.channel_change_rx.clone();
        let channel_change_tx = self.channel_change_tx.clone();

        Self {
            stream_controls,
            sinks,
            channels,
            channel_change_rx,
            channel_change_tx,
        }
    }
}

impl<St, Si> Multiplexer<St, Si>
where
    St: TryStream + Unpin,
    St::Ok: Clone,
    Si: Sink<St::Ok> + Unpin,
    Si::Error: std::fmt::Debug,
{
    /// Creates a Multiplexer
    pub fn new() -> Self {
        let (channel_change_tx, channel_change_rx) = async_channel::unbounded();

        Self {
            channel_change_rx,
            channel_change_tx,
            channels: Arc::new(DashMap::new()),
            sinks: Arc::new(RwLock::new(Slab::new())),
            stream_controls: Arc::new(DashMap::new()),
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
        self.channels.insert(channel_id, (add_tx, channel));

        Ok(())
    }

    /// Returns true if the channel id exists.
    #[inline]
    pub fn has_channel(&self, channel: ChannelId) -> bool {
        self.channels.contains_key(&channel)
    }

    /// Removes a channel.
    ///
    /// Returns `true` if found, `false` otherwise.
    pub fn remove_channel(&mut self, channel: ChannelId) -> bool {
        // FIXME: What do we do with the sockets in that channel?
        self.channels.remove(&channel)
    }

    /// Sends `data` to a set of `stream_ids`, waiting for them to be sent.
    pub async fn send(
        &self,
        stream_ids: impl IntoIterator<Item = StreamId>,
        data: St::Ok,
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
        let channel = self
            .channels
            .get(&channel_id)
            .ok_or_else(|| MultiplexerError::UnknownChannel(channel_id))?;

        // Add the write-half of the stream to the slab, can't return the stream if there isn't room.
        let stream_id = self
            .sinks
            .write()
            .insert(Arc::new(Mutex::new(sink)))
            .ok_or_else(move || MultiplexerError::ChannelFull(channel_id))?;

        // wrap the stream in a dropper so that it can be ejected
        let (stream_control, stream_dropper) =
            StreamDropControl::wrap(stream_id, stream, self.channel_change_tx.clone());
        self.stream_controls.insert(stream_id, stream_control);

        // Add the read-half of the stream to the channel
        channel
            .0
            .try_send(stream_dropper)
            .map_err(|_err| MultiplexerError::ChannelAdd(stream_id, channel_id))?;

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

        let control = self
            .stream_controls
            .get(&stream_id)
            .ok_or_else(|| MultiplexerError::UnknownStream(stream_id))?;

        control.change_channel(channel_id);

        Ok(())
    }

    /// Removes the stream from the multiplexer
    pub fn remove_stream(&mut self, stream_id: StreamId) -> bool {
        log::trace!("Attempting to remove stream {}", stream_id);

        // Removing it first from the sinks because the sinks are checked when changing channels
        let res = self.sinks.write().take(stream_id).is_some();
        log::trace!("Stream {} exists", stream_id);

        // If the stream is changing channels, it may not have a control and will be dropped in process_add_channel
        if let Some(control) = self.stream_controls.get(&stream_id) {
            log::trace!("Stream {} is getting dropped()", stream_id);
            control.drop_stream();
        }

        res
    }

    /// Receives the next packet available from a channel:
    ///
    /// Returns a tuple of the stream's ID and it's data or an error.
    pub async fn recv(
        &mut self,
        channel_id: ChannelId,
    ) -> Result<(StreamId, Option<St::Item>), MultiplexerError<Si::Error>> {
        log::debug!("recv({})", channel_id);
        match self.channels.get(&channel_id) {
            Some(channel_guard) => {
                log::debug!("recv({}) before loop {{}}", channel_id);
                let mut channel = channel_guard.value().1.lock().await;
                loop {
                    log::debug!("recv({}) awaiting the select", channel_id);
                    futures_util::select! {
                        channel_next = channel.next().fuse() => {
                            return Ok(channel_next);
                        }
                        add_res = self.channel_change_rx.next().fuse() => {
                            log::debug!("recv({}) channel_change has message.", channel_id);
                            if let Some(add_res) = add_res {
                                // If the stream fails to get added to this channel, bail
                                self.process_add_channel(add_res)?;
                            }
                        }
                    }
                }
            }
            None => Err(MultiplexerError::UnknownChannel(channel_id)),
        }
    }

    fn process_add_channel(
        &mut self,
        channel_change: ChannelChange<St>,
    ) -> Result<(), MultiplexerError<Si::Error>> {
        let ChannelChange {
            next_channel_id,
            stream_id,
            stream,
        } = channel_change;

        // Check that we have the channel before we commit the stream to the slab.
        let channel = self
            .channels
            .get(&next_channel_id)
            .ok_or_else(|| MultiplexerError::ChannelAdd(stream_id, next_channel_id))?;

        // If the sink has been dropped, don't add this half to a channel
        if self.sinks.read().contains(stream_id) {
            // Wrap the stream in another drop control
            let (stream_control, stream_dropper) =
                StreamDropControl::wrap(stream_id, stream, self.channel_change_tx.clone());
            self.stream_controls.insert(stream_id, stream_control);

            // FIXME: If a channel is removed while a stream is being transitioned to it why are we
            // returning the error to some other channel's next() call?
            channel
                .0
                .try_send(stream_dropper)
                .map_err(|_err| MultiplexerError::ChannelAdd(stream_id, next_channel_id))?;
        }

        Ok(())
    }
}
