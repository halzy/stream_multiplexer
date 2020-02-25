use crate::*;

pub struct Sender<Si> {
    stream_id: Option<StreamId>,
    sink: Option<Si>,
    read_halt: HaltRead,
}
impl<Si> Unpin for Sender<Si> where Si: Unpin {}

impl<Si> std::fmt::Debug for Sender<Si> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl<Si> Sender<Si> {
    #[tracing::instrument(level = "trace", skip(sink, read_halt))]
    pub fn new(sink: Si, read_halt: HaltRead) -> Self {
        Self {
            stream_id: None,
            sink: Some(sink),
            read_halt,
        }
    }

    pub(crate) fn sink(&mut self) -> Option<&mut Si> {
        self.sink.as_mut()
    }

    pub(crate) fn set_stream_id(&mut self, stream_id: StreamId) {
        if let Some(old_id) = self.stream_id.replace(stream_id) {
            panic!("Stream ID was already set to: {}", old_id);
        }
    }

    pub(crate) fn stream_id(&self) -> StreamId {
        self.stream_id.expect("Should have stream ID")
    }
}

impl<Si> Drop for Sender<Si> {
    #[tracing::instrument(level = "trace", skip(self))]
    fn drop(&mut self) {
        tracing::trace!("Sending dropped, halting read.");

        // Can be called many times, as long as Poll::Pending is returned early
        let _ = self.sink.take().expect("Should still have sender.");

        tracing::trace!("Signaling halt!");
        self.read_halt.signal();
    }
}

#[cfg(test)]
mod tests {
    use futures::prelude::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn sender_id() {
        // Create a 'test' stream with Read and Write halves
        let (tx, rx) = mpsc::channel::<()>(10);
        let (mut sender, _reader) = crate::tests::sender_reader(tx, rx);
        sender.set_stream_id(234);
        assert_eq!(234, sender.stream_id());
    }

    #[tokio::test]
    async fn message() {
        //crate::tests::init_logging();

        // Create a 'test' stream with Read and Write halves
        let (tx, rx) = mpsc::channel::<()>(10);
        let (sender, mut reader) = crate::tests::sender_reader(tx, rx);

        // Check that shutdown return the stream, and that we can read from it
        drop(sender);

        assert!(reader.next().await.is_none());
    }
}
