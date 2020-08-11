use stream_multiplexer::*;

use async_io::*;

use futures_util::future::FutureExt;
use futures_util::io::{AsyncReadExt, ReadHalf, WriteHalf};
use futures_util::sink::SinkExt;

use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::task::{Context, Poll};

type Item = StreamItem<Result<u8, std::io::Error>, usize>;

pin_project_lite::pin_project! {
    #[derive(Debug)]
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
    #[derive(Debug)]
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

        let first_stream_id = 1;
        let second_stream_id = 2;

        let mut mp1 = Multiplexer::new();
        mp1.add_stream(first_stream_id, right_stream_1).unwrap();
        mp1.add_stream(second_stream_id, right_stream_2).unwrap();

        let connected: Item = mp1.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(first_stream_id, connected.stream_id);

        let connected: Item = mp1.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(second_stream_id, connected.stream_id);

        left_sink_1.send(42).await.unwrap();

        // from stream_1
        let item_1: Item = mp1.recv().await.unwrap();
        assert!(matches!(&item_1.kind, ItemKind::Value(Ok(42))));

        left_sink_2.send(24).await.unwrap();

        // from stream_2
        let item_2: Item = mp1.recv().await.unwrap();
        assert!(matches!(item_2.kind, ItemKind::Value(Ok(24))));

        assert_ne!(item_1.stream_id, item_2.stream_id);
    });
}

#[test]
fn channel_change() {
    smol::block_on(async move {
        let _ = alto_logger::init_term_logger();

        let (mut left_sink_1, _left_stream, _right_sink_1, right_stream_1) =
            create_byte_stream_pair();

        let stream_id = 7432;

        // Start the test:
        let mut mp1 = Multiplexer::new();
        mp1.add_stream(stream_id, right_stream_1).unwrap();

        let connected: Item = mp1.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(stream_id, connected.stream_id);

        left_sink_1.send(42).await.unwrap();

        // from first channel
        let item_1: Item = mp1.recv().await.unwrap();
        assert!(matches!(item_1.kind, ItemKind::Value(Ok(42))));
        assert_eq!(stream_id, item_1.stream_id);

        log::error!("A:AA");
        let recv_fuse = mp1.recv().fuse();
        let remove_fuse = mp1.remove_stream(stream_id).fuse();

        futures_util::pin_mut!(recv_fuse);
        futures_util::pin_mut!(remove_fuse);

        let stream = futures_util::select! {
            _nothing = recv_fuse => { panic!("Should have chosen the other branch."); },
            stream = remove_fuse => { stream.unwrap() }
        };

        log::error!("BBB");

        let mut mp2 = Multiplexer::new();
        mp2.add_stream(stream_id, stream).unwrap();

        // Send another message and check the next channel
        left_sink_1.send(24).await.unwrap();

        // from second channel
        let connected: Item = mp2.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(stream_id, connected.stream_id);

        let item_2: Item = mp2.recv().await.unwrap();
        assert!(matches!(item_2.kind, ItemKind::Value(Ok(24))));
        assert_eq!(stream_id, item_2.stream_id);
    });
}

#[test]
fn stream_drop() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (mut left_sink, left_stream, right_sink, right_stream) = create_byte_stream_pair();

        let stream_id = 83;

        // Start the test:
        let mut mp = Multiplexer::new();

        mp.add_stream(stream_id, right_stream).unwrap();

        let connected: Item = mp.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(stream_id, connected.stream_id);

        // Verify that the stream is in the channel
        left_sink.send(42).await.unwrap();
        let item_1: Item = mp.recv().await.unwrap();
        assert!(matches!(item_1.kind, ItemKind::Value(Ok(42))));
        assert_eq!(stream_id, item_1.stream_id);

        // drop the stream
        drop(left_sink);
        drop(left_stream);

        // Get a None when it is removed
        let item_2 = mp.recv().await.unwrap();
        assert!(matches!(item_2.kind, ItemKind::Disconnected));
        assert_eq!(stream_id, item_2.stream_id);
    });
}

#[test]
fn errors() {
    smol::block_on(async move {
        // let _ = alto_logger::init_term_logger();

        let (_left_sink, _left_stream, right_sink, right_stream) = create_byte_stream_pair();

        let mut mp = Multiplexer::<ByteStream<ReadHalf<Async<UnixStream>>>, usize>::new();

        // should fail to remove non-existent stream
        assert!(matches!(
            mp.remove_stream(0_usize).await,
            Err(MultiplexerError::UnknownStream)
        ));
    });
}

/*
#[test]
fn clones() {
    smol::run(async move {
        let _ = alto_logger::init_term_logger();

        let (mut left_sink_1, mut left_stream_1, right_sink_1, right_stream_1) =
            create_byte_stream_pair();

        let (mut left_sink_2, mut left_stream_2, right_sink_2, right_stream_2) =
            create_byte_stream_pair();

        // Start the test:
        let channel_id = 3;

        let mut mp = Multiplexer::new();
        mp.add_channel(channel_id).unwrap();

        let stream_id_1 = mp
            .add_stream(right_sink_1, right_stream_1, channel_id)
            .unwrap();

        let stream_id_2 = mp
            .add_stream(right_sink_2, right_stream_2, channel_id)
            .unwrap();

        let connected = mp.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(stream_id_1, connected.stream_id);

        let connected = mp.recv().await.unwrap();
        assert!(matches!(connected.kind, ItemKind::Connected));
        assert_eq!(stream_id_2, connected.stream_id);

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
            mp1.send(Some(stream_id_1), Some(33_u8))
                .for_each(|_| async move { () })
                .await;
        })
        .detach();

        let mp2 = mp.clone();
        smol::Task::spawn(async move {
            mp2.send(Some(stream_id_2), Some(22_u8))
                .for_each(|_| async move { () })
                .await;
        })
        .detach();

        let res1 = mp.recv().await.unwrap();
        let res2 = mp.recv().await.unwrap();

        if res1.stream_id == stream_id_1 {
            assert!(matches!(res1.kind, ItemKind::Value(Ok(33))));
            assert!(matches!(res2.kind, ItemKind::Value(Ok(22))));
        } else {
            assert_eq!(res2.stream_id, stream_id_2);
            assert!(matches!(res1.kind, ItemKind::Value(Ok(22))));
            assert!(matches!(res2.kind, ItemKind::Value(Ok(33))));
        }
    });
}
*/
