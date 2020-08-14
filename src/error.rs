/// Errors returned by `Multiplexer`.
#[derive(Debug)]
pub enum MultiplexerError {
    /// She stream already exists
    DuplicateStream,

    /// `StreamId` is not recognized.
    UnknownStream,
}

impl std::fmt::Display for MultiplexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use MultiplexerError::*;
        match self {
            DuplicateStream => write!(f, "Stream already exists."),
            UnknownStream => write!(f, "Stream does not exist."),
        }
    }
}

impl std::error::Error for MultiplexerError {}
