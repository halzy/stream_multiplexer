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
/*!
This crate provides stream multiplexing with channels.

Channels have their own backpressure that does not affect other channels.

Incoming streams are by default set to channel 0 and can be moved to other channels via `ControlMessage`s.

```rust
# use std::error::Error;
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use stream_multiplexer::{Multiplexer, HalvesStream, ControlMessage, IncomingPacket, OutgoingPacket};
use futures::stream::StreamExt;

# fn main() -> Result<(), Box<dyn Error>> {
# tokio::runtime::Builder::new().basic_scheduler().enable_all().build().unwrap().block_on(async move {
// 3 channels of incoming streams, 0 is the channel that new streams join.
// Backpressure is per channel. Streams can be moved between channels by
// sending an OutgoingPacket::ChangeChannel message.
let (channel0_tx, mut channel0_rx) = mpsc::channel(32);
let (channel1_tx, mut channel1_rx) = mpsc::channel(32);
let (channel2_tx, mut channel2_rx) = mpsc::channel(32);

// A Stream for outgoing messages.
let (mut outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingPacket<Bytes>>(32);

// Construct the multiplexer, giving it the OutgoingPacket stream, and a vector of incoming
// streams. The backlog controls how much of an internal buffer each WriteHalf (TcpSocket in this example) can have.
let outgoing_streams_backlog = 128;
let multiplexer = Multiplexer::new(
    outgoing_streams_backlog,
    outgoing_rx,
    vec![channel0_tx, channel1_tx, channel2_tx],
);

// Bind to a random port on localhost
let socket = TcpListener::bind("127.0.0.1:0").await?;

let local_addr = socket.local_addr()?;

// Use the HalvesStream utility struct to map the stream of new sockets.
// It will use LengthDelimitedCodec with 2 bytes as the packet size.
let halves = HalvesStream::new(socket, 2);

// Control channel for shutting down the multiplexer
let (control_write, control_read) = mpsc::unbounded_channel();
let mp_joinhandle = tokio::task::spawn(multiplexer.run(halves, control_read));

// Make a test connection:
let mut client = tokio::net::TcpStream::connect(local_addr).await?;

// Listen for the channel join announcement
let message = channel0_rx.recv().await.expect("Should have connected.");
matches::assert_matches!(message, IncomingPacket::StreamConnected(_));

// Send 'a message'
let mut data = Bytes::from("\x00\x09a message");
client.write_buf(&mut data).await?;
client.flush();

// Receive 'a message' on channel 0
let incoming_packet = channel0_rx.recv().await.unwrap();
assert_eq!(
    incoming_packet
        .value()
        .expect("should have a value")
        .as_ref()
        .unwrap(),
    &Bytes::from("a message")
);

// Move the client to channel 1
outgoing_tx
    .send(OutgoingPacket::ChangeChannel(vec![incoming_packet.id()], 1))
    .await?;

// Listen for the channel join announcement
let message = channel1_rx.recv().await.expect("Should have connected.");
matches::assert_matches!(message, IncomingPacket::StreamConnected(_));

// Send 'a message' again, on channel 1 this time.
let mut data = Bytes::from("\x00\x09a message");
client.write_buf(&mut data).await?;
client.flush();

// Receive 'a message' on channel 1
let incoming_packet = channel1_rx.recv().await.unwrap();
assert_eq!(
    incoming_packet
        .value()
        .expect("should have a value")
        .as_ref()
        .unwrap(),
    &Bytes::from("a message")
);

// Move the client to channel 2
outgoing_tx
    .send(OutgoingPacket::ChangeChannel(vec![incoming_packet.id()], 2))
    .await?;

// Listen for the channel join announcement
let message = channel2_rx.recv().await.expect("Should have connected.");
matches::assert_matches!(message, IncomingPacket::StreamConnected(_));

// Send 'a message' again, on channel 2 this time.
let mut data = Bytes::from("\x00\x09a message");
client.write_buf(&mut data).await?;
client.flush();

// Receive 'a message' on channel 2
let incoming_packet = channel2_rx.recv().await.unwrap();
assert_eq!(
    incoming_packet
        .value()
        .expect("should have a value")
        .as_ref()
        .unwrap(),
    &Bytes::from("a message")
);

// Tell multiplexer to shut down
control_write.send(ControlMessage::Shutdown)?;

mp_joinhandle.await.unwrap();
# Ok::<_, Box<dyn Error>>(())
# });
# Ok(())
# }
```
*/
mod error;
mod halt;
mod halves_stream;
mod id_gen;
mod multiplexer;
mod multiplexer_senders;
mod packets;
mod send_all_own;
mod sender;
mod stream_mover;

use futures::prelude::*;

pub use error::*;
use halt::*;
pub use halves_stream::*;
pub use id_gen::*;
pub use multiplexer::*;
use multiplexer_senders::*;
pub use packets::*;
use send_all_own::*;
use sender::*;
use stream_mover::*;

type StreamId = usize;
type ChannelId = usize;

/// Container for an incoming stream's Read and Write halves.
#[derive(Debug)]
pub struct IncomingStream<ReadSt, WriteSi>
where
    ReadSt: Stream + Unpin,
    ReadSt::Item: std::fmt::Debug,
{
    /// Channel that the stream should be placed in.
    channel: ChannelId,

    /// Write half of a connection.
    write_sink: WriteSi,

    /// Read half of a connection.
    read_stream: ReadSt,
}

impl<ReadSt, WriteSi> IncomingStream<ReadSt, WriteSi>
where
    ReadSt: Stream + Unpin,
    ReadSt::Item: std::fmt::Debug,
{
    /// Creates a new IncomingStream
    pub fn new(channel: ChannelId, write_sink: WriteSi, read_stream: ReadSt) -> Self {
        Self {
            channel,
            write_sink,
            read_stream,
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
