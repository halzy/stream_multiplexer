/**
 * Todo:
 * Add tracing
 * linkdead support
 **/
use tcp_streams::*;

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc as channel;

use std::io::Result as IoResult;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct SharedIdGen<T: IdGen = IncrementIdGen> {
    inner: Arc<Mutex<T>>,
    id_rx: Arc<Mutex<channel::UnboundedReceiver<usize>>>,
    id_tx: channel::UnboundedSender<usize>,
}
impl<T: IdGen> IdGen for SharedIdGen<T> {
    fn next(&mut self) -> usize {
        let id = self.inner.lock().unwrap().next();
        self.id_tx
            .send(id)
            .expect("should be able to send updated id");
        id
    }

    fn id(&self) -> usize {
        self.inner.lock().unwrap().id()
    }

    fn seed(&mut self, seed: usize) {
        self.inner.lock().unwrap().seed(seed)
    }
}
impl<T: IdGen> Default for SharedIdGen<T> {
    fn default() -> Self {
        let (id_tx, id_rx) = channel::unbounded_channel();
        Self {
            inner: Default::default(),
            id_tx,
            id_rx: Arc::new(Mutex::new(id_rx)),
        }
    }
}
impl<T: IdGen> SharedIdGen<T> {
    pub async fn wait_for_next_id(&self) -> usize {
        self.id_rx.lock().unwrap().recv().await.unwrap()
    }
}

pub async fn bind() -> IoResult<TcpListener> {
    tracing::info!("Starting");
    let addrs = "127.0.0.1:0".to_string();
    tracing::info!("Binding {:?}", &addrs);
    TcpListener::bind(&addrs).await
}

#[tokio::test(basic_scheduler)]
async fn shutdown() {
    let mut socket = bind().await.unwrap();
    let (control_write, control_read) = channel::unbounded_channel();
    let (_data_write, data_read) = channel::unbounded_channel();
    let mut tcp_streams = PacketMultiplexer::new(data_read);
    let shutdown_status =
        tokio::task::spawn(async move { tcp_streams.run(socket.incoming(), control_read).await });

    control_write.send(ControlMessage::Shutdown).unwrap();
    assert!(shutdown_status.await.is_ok());
}

#[tokio::test(basic_scheduler)]
async fn socket_shutdown() {
    let mut socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();

    let client = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    client.shutdown(std::net::Shutdown::Both).unwrap();

    let (control_write, control_read) = channel::unbounded_channel();
    let (_data_write, data_read) = channel::unbounded_channel();
    let mut tcp_streams = PacketMultiplexer::new(data_read);
    let shutdown_status =
        tokio::task::spawn(async move { tcp_streams.run(socket.incoming(), control_read).await });

    control_write.send(ControlMessage::Shutdown).unwrap();
    assert!(shutdown_status.await.is_ok());
}

#[tokio::test(basic_scheduler)]
async fn write_packets() {
    let mut socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();

    // so that we can signal shutdown
    let (control_write, control_read) = channel::unbounded_channel();

    let (data_write, data_read) = channel::unbounded_channel();

    // sharing the idgen so that we can get the client stream_ids
    let id_gen: SharedIdGen<IncrementIdGen> = SharedIdGen::default();
    let senders = MultiplexerSenders::new(id_gen.clone());
    let mut multiplexer = PacketMultiplexer::with_senders(senders, data_read);

    // start the loop
    let run_loop = async move { multiplexer.run(socket.incoming(), control_read).await };
    let shutdown_status = tokio::task::spawn(run_loop);

    // connect some clients so that we can send a message to them
    let mut client1 = tokio::net::TcpStream::connect(local_addr).await.unwrap();

    let client1_id = id_gen.wait_for_next_id().await;
    let client2 = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    let _client2_id = id_gen.wait_for_next_id().await;

    // send a message
    let data = Bytes::from("a message");
    data_write
        .send(OutgoingPacket::new(vec![client1_id], data.clone()))
        .unwrap();

    eprintln!("about to read");
    let mut read_data = BytesMut::new();
    let _read_res = client1.read_buf(&mut read_data).await.unwrap();
    assert_eq!(read_data, "\0\ta message".as_bytes());
    eprintln!("done with read");

    // cleanup
    client1.shutdown(std::net::Shutdown::Both).unwrap();
    client2.shutdown(std::net::Shutdown::Both).unwrap();
    control_write.send(ControlMessage::Shutdown).unwrap();

    assert!(shutdown_status.await.is_ok());
}
