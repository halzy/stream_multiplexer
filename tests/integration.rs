use stream_multiplexer::*;

use async_io::*;

use futures_util::io::{AsyncReadExt, ReadHalf, WriteHalf};
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;

use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project_lite::pin_project! {
    struct ByteStream<T> {
        #[pin]
        inner: T,
    }
}
impl<T> ByteStream<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> futures_util::stream::Stream for ByteStream<T>
where
    T: futures_util::io::AsyncRead,
{
    type Item = Result<u8, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let mut buf = [0_u8; 1];
        futures_util::ready!(this.inner.poll_read(cx, &mut buf))?;
        Poll::Ready(Some(Ok(buf[0])))
    }
}

pin_project_lite::pin_project! {
    struct ByteSink<T> {
        #[pin]
        inner: T,
        data: Option<u8>
    }
}
impl<T> ByteSink<T> {
    pub fn new(inner: T) -> Self {
        Self { inner, data: None }
    }
}

impl<T> futures_util::sink::Sink<u8> for ByteSink<T>
where
    T: futures_util::io::AsyncWrite,
{
    type Error = std::io::Error;
    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        log::info!("SINK: poll_ready()");
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, item: u8) -> Result<(), Self::Error> {
        log::info!("SINK: start_send: {}", item);
        let this = self.project();

        this.data.replace(item);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        log::info!("SINK: poll_flush()");
        let this = self.project();

        match Option::take(this.data) {
            Some(data) => {
                log::info!("SINK: poll_flush() with Some {} ", data);
                this.inner.poll_write(cx, &[data]).map_ok(|_| ())
            }
            None => {
                log::info!("SINK: poll_flush() with None ");
                this.inner.poll_flush(cx)
            }
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        log::info!("SINK: poll_close()");
        let this = self.project();
        this.inner.poll_flush(cx)
    }
}

fn create_byte_stream_pair() -> (
    ByteSink<WriteHalf<Async<UnixStream>>>,
    ByteStream<ReadHalf<Async<UnixStream>>>,
    ByteSink<WriteHalf<Async<UnixStream>>>,
    ByteStream<ReadHalf<Async<UnixStream>>>,
) {
    // Set up the Sink/Stream pairs
    let (left, right) = Async::<UnixStream>::pair().unwrap();

    let (right_rx, right_tx) = right.split();
    let right_stream = ByteStream::new(right_rx);
    let right_sink = ByteSink::new(right_tx);

    let (left_rx, left_tx) = left.split();
    let left_stream = ByteStream::new(left_rx);
    let left_sink = ByteSink::new(left_tx);

    (left_sink, left_stream, right_sink, right_stream)
}

#[test]
fn create_and_simple_messages() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (mut left_sink_1, _left_stream, right_sink_1, right_stream_1) =
            create_byte_stream_pair();
        let (mut left_sink_2, _left_stream, right_sink_2, right_stream_2) =
            create_byte_stream_pair();

        // Start the test:
        let channel_id = 3;

        let mut mp = Multiplexer::new();
        mp.add_channel(channel_id).unwrap();
        let first_stream_id = mp
            .add_stream_pair(right_sink_1, right_stream_1, channel_id)
            .unwrap();
        let second_stream_id = mp
            .add_stream_pair(right_sink_2, right_stream_2, channel_id)
            .unwrap();

        let connected = mp.recv(channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(first_stream_id, connected.id);

        let connected = mp.recv(channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(second_stream_id, connected.id);

        left_sink_1.send(42).await.unwrap();

        // from stream_1
        let item_1 = mp.recv(channel_id).await.unwrap();
        assert!(matches!(&item_1.item, ItemKind::Value(Ok(42))));

        left_sink_2.send(24).await.unwrap();

        // from stream_2
        let item_2 = mp.recv(channel_id).await.unwrap();
        assert!(matches!(item_2.item, ItemKind::Value(Ok(24))));

        assert_ne!(item_1.id, item_2.id);
    });
}

#[test]
fn channel_change() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (mut left_sink_1, _left_stream, right_sink_1, right_stream_1) =
            create_byte_stream_pair();

        // Start the test:
        let first_channel_id = 3;
        let second_channel_id = 4;

        let mut mp = Multiplexer::new();
        mp.add_channel(first_channel_id).unwrap();
        mp.add_channel(second_channel_id).unwrap();
        let first_stream_id = mp
            .add_stream_pair(right_sink_1, right_stream_1, first_channel_id)
            .unwrap();

        let connected = mp.recv(first_channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(first_stream_id, connected.id);

        left_sink_1.send(42).await.unwrap();

        // from first channel
        let item_1 = mp.recv(first_channel_id).await.unwrap();
        assert!(matches!(item_1.item, ItemKind::Value(Ok(42))));
        assert_eq!(first_stream_id, item_1.id);

        mp.change_stream_channel(first_stream_id, second_channel_id)
            .unwrap();

        // When the stream leaves the channel, None is returned
        let item = mp.recv(first_channel_id).await.unwrap();

        assert!(matches!(item.item, ItemKind::ChannelChange));

        assert_eq!(first_stream_id, item.id);

        // Send another message and check the next channel
        left_sink_1.send(24).await.unwrap();

        // from second channel
        let connected = mp.recv(second_channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(first_stream_id, connected.id);

        let item_2 = mp.recv(second_channel_id).await.unwrap();
        assert!(matches!(item_2.item, ItemKind::Value(Ok(24))));
        assert_eq!(first_stream_id, item_2.id);
    });
}

#[test]
fn stream_drop() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (mut left_sink, _left_stream, right_sink, right_stream) = create_byte_stream_pair();

        // Start the test:
        let channel_id = 3;

        let mut mp = Multiplexer::new();
        mp.add_channel(channel_id).unwrap();

        let stream_id_1 = mp
            .add_stream_pair(right_sink, right_stream, channel_id)
            .unwrap();

        let connected = mp.recv(channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(stream_id_1, connected.id);

        // Verify that the stream is in the channel
        left_sink.send(42).await.unwrap();
        let item_1 = mp.recv(channel_id).await.unwrap();
        assert!(matches!(item_1.item, ItemKind::Value(Ok(42))));
        assert_eq!(item_1.id, stream_id_1);

        // drop the stream
        assert!(matches!(mp.remove_stream(stream_id_1), Ok(())));

        // Get a None when it is removed
        let item_2 = mp.recv(channel_id).await.unwrap();
        assert!(matches!(item_2.item, ItemKind::Disconnected));
        assert_eq!(item_2.id, stream_id_1);
    });
}

#[test]
fn errors() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (_left_sink, _left_stream, right_sink, right_stream) = create_byte_stream_pair();

        // Start the test:
        let channel_id = 3;

        let mut mp = Multiplexer::new();

        // Should not be able to add a stream to a channel that does not exist
        let res = mp.add_stream_pair(right_sink, right_stream, channel_id);
        assert!(res.is_err());

        // removing non-existent channel should return false
        assert!(matches!(
            mp.remove_channel(234),
            Err(MultiplexerError::ChannelNotEmpty)
        ));

        // add channel should fail if the channel ID already exists
        mp.add_channel(43).unwrap();
        assert!(matches!(
            mp.add_channel(43),
            Err(MultiplexerError::DuplicateChannel(43))
        ));

        assert!(mp.has_channel(43));

        // Sending to non-existent streams
        let results = mp.send(vec![8], 88_u8).await;
        assert!(matches!(
            results[0],
            Err(MultiplexerError::UnknownStream(8))
        ));

        // test channel not existing
        let res = mp.change_stream_channel(0, 0);
        assert!(matches!(res, Err(MultiplexerError::UnknownChannel(0))));

        // check stream not existing
        let res = mp.change_stream_channel(0, 43);
        assert!(matches!(res, Err(MultiplexerError::UnknownStream(0))));

        // should fail to remove non-existent stream
        assert!(matches!(
            mp.remove_stream(0),
            Err(MultiplexerError::UnknownStream(0))
        ));

        // should fail to recv for non-exstent channel
        let res = mp.recv(678).await;
        assert!(matches!(res, Err(MultiplexerError::UnknownChannel(678))));
    });
}

#[test]
fn clones() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (mut left_sink_1, mut left_stream_1, right_sink_1, right_stream_1) =
            create_byte_stream_pair();
        let (mut left_sink_2, mut left_stream_2, right_sink_2, right_stream_2) =
            create_byte_stream_pair();

        // Start the test:
        let channel_id = 3;

        let mut mp = Multiplexer::new();
        mp.add_channel(channel_id).unwrap();

        let stream_id_1 = mp
            .add_stream_pair(right_sink_1, right_stream_1, channel_id)
            .unwrap();

        let stream_id_2 = mp
            .add_stream_pair(right_sink_2, right_stream_2, channel_id)
            .unwrap();

        let connected = mp.recv(channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(stream_id_1, connected.id);

        let connected = mp.recv(channel_id).await.unwrap();
        assert!(matches!(connected.item, ItemKind::Connected));
        assert_eq!(stream_id_2, connected.id);

        smol::Task::spawn(async move {
            loop {
                left_sink_1
                    .send(left_stream_1.next().await.unwrap().unwrap())
                    .await
                    .unwrap();
            }
        })
        .detach();
        smol::Task::spawn(async move {
            loop {
                left_sink_2
                    .send(left_stream_2.next().await.unwrap().unwrap())
                    .await
                    .unwrap();
            }
        })
        .detach();

        let mp1: Multiplexer<_, _, _> = mp.clone();
        smol::Task::spawn(async move {
            let streams = vec![stream_id_1];
            mp1.send(streams, 33_u8).await;
        })
        .detach();

        let mp2 = mp.clone();
        smol::Task::spawn(async move {
            let streams = vec![stream_id_2];
            mp2.send(streams, 22_u8).await;
        })
        .detach();

        let res1 = mp.recv(channel_id).await.unwrap();
        let res2 = mp.recv(channel_id).await.unwrap();

        if res1.id == stream_id_1 {
            assert!(matches!(res1.item, ItemKind::Value(Ok(33))));
            assert!(matches!(res2.item, ItemKind::Value(Ok(22))));
        } else {
            assert_eq!(res2.id, stream_id_2);
            assert!(matches!(res1.item, ItemKind::Value(Ok(22))));
            assert!(matches!(res2.item, ItemKind::Value(Ok(33))));
        }
    });
}
