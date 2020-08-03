use crate::*;

// FIXME: Remove dependency on thiserror when Futures changes to pin-project-lite
// https://github.com/rust-lang/futures-rs/issues/2170

/// Errors returned by `Multiplexer`.
#[derive(thiserror::Error, Debug)]
pub enum MultiplexerError<SE: std::fmt::Debug> {
    /// `StreamId` could not be added to `ChannelId`
    #[error("Could not add stream {0} to channel {1}.")]
    ChannelAdd(StreamId, ChannelId),

    /// The internal storage for `streams` in a channel is full.
    #[error("Could not add stream to full channel {0}.")]
    ChannelFull(ChannelId),

    /// `ChannelId` already exists and cannot be added again.
    #[error("Channel {0} already exists")]
    DuplicateChannel(ChannelId),

    /// `StreamId` is not recognized.
    #[error("Stream {0} does not exist.")]
    UnknownStream(StreamId),

    /// `ChannelId` is not recognized.
    #[error("Channel {0} is unknown.")]
    UnknownChannel(ChannelId),

    /// `StreamId` could not be sent to.
    #[error("Could not send item to sink {0} due to {1:?}")]
    SendError(StreamId, SE),
}
