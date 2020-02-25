/// A collection of errors that can be returned.
#[derive(thiserror::Error, Debug)]
pub enum MultiplexerError {
    // /// Sending can fail to enqueue a message to a stream.
    // #[error("Could not send to the stream")]
    // Send(#[from] tokio::sync::mpsc::error::TrySendError<Result<OutgoingMessage<OV>, ()>>),
    //
    // FIXME: outgoing error stream ?
    // /// If the stream that is trying to be sent to has gone away
    // #[error("Sending to nonexistent stream {0}")]
    // SendNoStream(StreamId),

    // #[error("Sending to full stream {0}")]
    // StreamFull(StreamId),

    // #[error("Sending to full stream {0}")]
    // StreamClosed(StreamId),
    /// Wrapper around std::io::Error
    #[error("IoError")]
    IoError(#[from] std::io::Error),

    /// Nothing to see here
    #[error("Should never happen")]
    UnitError,
}
