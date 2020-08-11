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
        match stream_item.kind {
            Value(Ok(data)) => {
                // echo the data back and move it to channel 2
                multiplexer_ch_1.send(vec![stream_item.stream_id], vec![data]).collect::<Vec<_>>().await;
                multiplexer_ch_1
                    .change_stream_channel(stream_item.stream_id, CHANNEL_TWO)
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

// #![forbid(unsafe_code)]
// #![warn(
//     missing_docs,
//     missing_debug_implementations,
//     missing_copy_implementations,
//     trivial_casts,
//     trivial_numeric_casts,
//     unreachable_pub,
//     unsafe_code,
//     unstable_features,
//     unused_import_braces,
//     unused_qualifications,
//     rust_2018_idioms
// )]

mod error;
mod notify;
pub use error::MultiplexerError;
use notify::*;

use async_channel::*;
use futures_lite::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub enum ItemKind<T> {
    Value(T),
    Connected,
    Disconnected,
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct StreamItem<T, Id> {
    pub stream_id: Id,
    pub kind: ItemKind<T>,
}

struct Ejection<St, Id>
where
    St: 'static,
{
    landed_streams: HashMap<Id, St>,
    eject_channel_tx: Sender<(Id, St)>,
    eject_channel_rx: Receiver<(Id, St)>,
}

impl<St, Id> Ejection<St, Id>
where
    Id: Eq + std::hash::Hash + Clone,
{
    pub fn new() -> Self {
        let (eject_channel_tx, eject_channel_rx) = async_channel::unbounded();
        Self {
            eject_channel_rx,
            eject_channel_tx,
            landed_streams: Default::default(),
        }
    }

    pub fn channel(&self) -> Sender<(Id, St)> {
        self.eject_channel_tx.clone()
    }

    pub async fn recv(&mut self, stream_id: Id) -> St
    where
        Id: Send + Sync + Clone,
        St: Send + Sync + Unpin + 'static,
    {
        if let Entry::Occupied(entry) = self.landed_streams.entry(stream_id.clone()) {
            let (_id, stream) = entry.remove_entry();
            return stream;
        }

        loop {
            let (id, stream) = self
                .eject_channel_rx
                .recv()
                .await
                .expect("Should not fail, other end is owned too");

            if id == stream_id {
                return stream;
            }

            self.landed_streams
                .insert(id, stream)
                .expect("Should not exist, it was checked above");
        }
    }
}

///
pub struct Multiplexer<St, Id>
where
    St: Stream + 'static,
    Id: std::hash::Hash + Eq + 'static,
{
    stream_controls: HashMap<Id, Notifier>,
    stream_of_items_tx: Sender<StreamItem<St::Item, Id>>,
    stream_of_items_rx: Receiver<StreamItem<St::Item, Id>>,
    ejection: Ejection<St, Id>,
}

impl<St, Id> Multiplexer<St, Id>
where
    St: Stream,
    Id: std::hash::Hash + Eq + Clone + 'static,
{
    /// Creates a Multiplexer
    pub fn new(buffer_size: usize) -> Self {
        // Start the runtime so that we can create tasks later
        smol::run(async move {
            let _: () = future::pending().await;
        });

        let (stream_of_items_tx, stream_of_items_rx) = async_channel::bounded(buffer_size);

        Self {
            stream_controls: Default::default(),
            stream_of_items_tx,
            stream_of_items_rx,
            ejection: Ejection::new(),
        }
    }

    /// Adds `stream` with the given `stream_id`.
    ///
    /// Returns an error if the `stream_id` already exists.
    pub fn add_stream(&mut self, stream_id: Id, stream: St) -> Result<(), MultiplexerError>
    where
        Id: Send + Sync + Clone,
        St: Send + Sync + Unpin + 'static,
        St::Item: Send + Sync + 'static,
    {
        let (notifier, notify) = Notify::new();

        match self.stream_controls.entry(stream_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(notifier);
            }
            Entry::Occupied(_) => {
                return Err(MultiplexerError::DuplicateStream);
            }
        }

        let eject = self.ejection.channel();

        let mut stream_roller =
            StreamRoller::new(stream_id.clone(), stream, self.stream_of_items_tx.clone());

        smol::Task::spawn(async move {
            smol::future::race(stream_roller.roll(), notify).await;

            let stream = stream_roller.into_stream();

            return eject.send((stream_id, stream)).await.unwrap();
        })
        .detach();

        Ok(())
    }

    /// Removes the stream from the multiplexer.
    ///
    /// Will only be able to return stream if `recv()` is being polled.
    pub async fn remove_stream(&mut self, stream_id: Id) -> Result<St, MultiplexerError>
    where
        Id: std::fmt::Debug,
        Id: Send + Sync + Clone,
        St: Send + Sync + Stream + Unpin,
        St::Item: Send + Sync,
    {
        // If the stream is changing channels, it may not have a control and will be dropped in process_add_channel
        let control = self
            .stream_controls
            .remove(&stream_id)
            .ok_or_else(|| MultiplexerError::UnknownStream)?;

        control.notify();

        Ok(self.ejection.recv(stream_id).await)
    }

    /// Receives the next packet available from a channel:
    ///
    /// Returns a `StreamItem` or `MultiplexerError::UnknownChannel` if called with an unknown
    /// `channel_id`.
    pub async fn recv(&mut self) -> StreamItem<St::Item, Id>
    where
        Id: Send + Sync + Clone,
        St: Send + Sync + Stream + Unpin,
        St::Item: Send + Sync,
    {
        self.stream_of_items_rx
            .recv()
            .await
            .expect("The other end of the channel we hold should not drop.")
    }
}

struct StreamRoller<St, Id>
where
    St: Stream,
{
    stream_id: Id,
    stream: St,
    stream_of_items: Sender<StreamItem<St::Item, Id>>,
}

impl<St, Id> StreamRoller<St, Id>
where
    Id: Send + Sync + Clone,
    St: Send + Sync + Stream + Unpin,
    St::Item: Send + Sync,
{
    fn new(stream_id: Id, stream: St, stream_of_items: Sender<StreamItem<St::Item, Id>>) -> Self {
        Self {
            stream_id,
            stream,
            stream_of_items,
        }
    }

    fn into_stream(self) -> St {
        self.stream
    }

    pub async fn roll(&mut self) {
        self.stream_of_items
            .send(StreamItem {
                stream_id: self.stream_id.clone(),
                kind: ItemKind::Connected,
            })
            .await
            .unwrap();
        loop {
            // FIXME: stop looping with disconnect
            let kind = match self.stream.next().await {
                Some(item) => ItemKind::Value(item),
                None => ItemKind::Disconnected,
            };

            self.stream_of_items
                .send(StreamItem {
                    stream_id: self.stream_id.clone(),
                    kind,
                })
                .await
                .unwrap();
        }
    }
}
