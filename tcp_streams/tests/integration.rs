/**
 * TODO:
 * linkdeadsupport
 * server id
 **/
use tcp_streams::*;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    let socket = TcpStreamProducer::new(bind().await.unwrap());
    let (control_write, control_read) = channel::unbounded_channel();
    let (_data_write, data_read) = channel::unbounded_channel();
    let (in_data_tx, _in_data_rx) = channel::channel(10);
    let tcp_streams = PacketMultiplexer::new(data_read, vec![in_data_tx]);
    let shutdown_status = tokio::task::spawn(tcp_streams.run(socket, control_read));

    control_write.send(ControlMessage::Shutdown).unwrap();
    assert!(shutdown_status.await.is_ok());
}

#[tokio::test(basic_scheduler)]
async fn socket_shutdown() {
    let socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();
    let socket = TcpStreamProducer::new(socket);

    let client = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    client.shutdown(std::net::Shutdown::Both).unwrap();

    let (control_write, control_read) = channel::unbounded_channel();
    let (_data_write, data_read) = channel::unbounded_channel();
    let (in_data_tx, _in_data_rx) = channel::channel(10);
    let tcp_streams = PacketMultiplexer::new(data_read, vec![in_data_tx]);
    let shutdown_status = tokio::task::spawn(tcp_streams.run(socket, control_read));

    control_write.send(ControlMessage::Shutdown).unwrap();
    assert!(shutdown_status.await.is_ok());
}

#[allow(dead_code)]
fn init_logging() {
    use tracing_subscriber::FmtSubscriber;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[tokio::test(basic_scheduler)]
async fn write_packets() {
    // init_logging();
    let socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();
    let socket = TcpStreamProducer::new(socket);

    // so that we can signal shutdown
    let (control_write, control_read) = channel::unbounded_channel();
    let (data_write, data_read) = channel::unbounded_channel();

    // sharing the idgen so that we can get the client stream_ids
    let id_gen: SharedIdGen<IncrementIdGen> = SharedIdGen::default();
    let senders = MultiplexerSenders::new(id_gen.clone());
    let (in_data_tx, _in_data_rx) = channel::channel(10);
    let multiplexer = PacketMultiplexer::with_senders(senders, data_read, vec![in_data_tx]);

    // start the loop
    let shutdown_status = tokio::task::spawn(multiplexer.run(socket, control_read));

    // connect some clients so that we can send a message to them
    let mut client1 = tokio::net::TcpStream::connect(local_addr).await.unwrap();

    let client1_id = id_gen.wait_for_next_id().await;
    let client2 = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    let _client2_id = id_gen.wait_for_next_id().await;

    // send a message
    let data = Bytes::from("a message");
    data_write
        .send(OutgoingPacket::new(
            vec![client1_id],
            OutgoingMessage::Bytes(data.clone()),
        ))
        .unwrap();

    let mut read_data = BytesMut::new();
    let _read_res = client1.read_buf(&mut read_data).await.unwrap();
    assert_eq!(read_data, "\0\ta message".as_bytes());

    // cleanup
    client1.shutdown(std::net::Shutdown::Both).unwrap();
    client2.shutdown(std::net::Shutdown::Both).unwrap();
    control_write.send(ControlMessage::Shutdown).unwrap();

    assert!(shutdown_status.await.is_ok());
}

#[tokio::test(basic_scheduler)]
async fn read_packets() {
    //init_logging();
    let socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();
    let socket = TcpStreamProducer::new(socket);

    // so that we can signal shutdown
    let (control_write, control_read) = channel::unbounded_channel();
    let (_data_write, data_read) = channel::unbounded_channel();

    // sharing the idgen so that we can get the client stream_ids
    let id_gen: SharedIdGen<IncrementIdGen> = SharedIdGen::default();
    let senders = MultiplexerSenders::new(id_gen.clone());
    let (in_data_tx, mut in_data_rx) = channel::channel(10);
    let multiplexer = PacketMultiplexer::with_senders(senders, data_read, vec![in_data_tx]);

    // start the loop
    let shutdown_status = tokio::task::spawn(multiplexer.run(socket, control_read));

    // connect some clients so that we can send a message to them
    let mut client1 = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    let client1_id = id_gen.wait_for_next_id().await;

    for _ in 0_u8..2 {
        // send a message
        let mut data = Bytes::from("\0\ta message");
        client1.write_buf(&mut data).await.unwrap();

        // read multiplexed data
        let incoming_packet = in_data_rx.recv().await.unwrap();
        assert_eq!(incoming_packet.id(), client1_id);
        assert_eq!(
            incoming_packet.message(),
            &IncomingMessage::Bytes(Bytes::from("a message"))
        );
    }

    // cleanup
    client1.shutdown(std::net::Shutdown::Both).unwrap();
    control_write.send(ControlMessage::Shutdown).unwrap();

    assert!(shutdown_status.await.is_ok());
}

#[tokio::test(basic_scheduler)]
async fn change_channel() {
    init_logging();
    let socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();
    let socket = TcpStreamProducer::new(socket);

    // so that we can signal shutdown
    let (control_write, control_read) = channel::unbounded_channel();
    let (data_write, data_read) = channel::unbounded_channel();

    // sharing the idgen so that we can get the client stream_ids
    let id_gen: SharedIdGen<IncrementIdGen> = SharedIdGen::default();
    let senders = MultiplexerSenders::new(id_gen.clone());
    let (in_data_tx0, mut in_data_rx0) = channel::channel(10);
    let (in_data_tx1, mut in_data_rx1) = channel::channel(10);
    let multiplexer =
        PacketMultiplexer::with_senders(senders, data_read, vec![in_data_tx0, in_data_tx1]);

    // start the loop
    let shutdown_status = tokio::task::spawn(multiplexer.run(socket, control_read));

    // connect some clients so that we can send a message to them
    let mut client1 = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    let client1_id = id_gen.wait_for_next_id().await;

    // read multiplexed data from channel 0
    //
    // send a message
    let mut data = Bytes::from("\0\ta message");
    client1.write_buf(&mut data).await.unwrap();

    let incoming_packet = in_data_rx0.recv().await.unwrap();
    assert_eq!(incoming_packet.id(), client1_id);
    assert_eq!(
        incoming_packet.message(),
        &IncomingMessage::Bytes(Bytes::from("a message"))
    );

    // Switch client1 from channel 0 to channel 1
    let change_channel = OutgoingPacket::new(vec![client1_id], OutgoingMessage::ChangeChannel(1));
    data_write.send(change_channel);

    // read multiplexed data from channel 1
    //
    // send a message
    let mut data = Bytes::from("\0\ta message");
    client1.write_buf(&mut data).await.unwrap();

    let incoming_packet = in_data_rx1.recv().await.unwrap();
    assert_eq!(incoming_packet.id(), client1_id);
    assert_eq!(
        incoming_packet.message(),
        &IncomingMessage::Bytes(Bytes::from("a message"))
    );

    // cleanup
    client1.shutdown(std::net::Shutdown::Both).unwrap();
    control_write.send(ControlMessage::Shutdown).unwrap();

    assert!(shutdown_status.await.is_ok());
}

/* FIXME: PLEASE!
#[tokio::test(basic_scheduler)]
async fn linkdead() {
    //init_logging();
    let socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();
    let socket = TcpStreamProducer::new(socket);

    let (control_write, control_read) = channel::unbounded_channel();
    let (_data_write, data_read) = channel::unbounded_channel();
    let (in_data_tx, _in_data_rx) = channel::channel(10);
    let tcp_streams = PacketMultiplexer::new(data_read, vec![in_data_tx]);

    let shutdown_status = tcp_streams.run(socket, control_read);

    let client = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    client.shutdown(std::net::Shutdown::Both).unwrap();

    control_write.send(ControlMessage::Shutdown).unwrap();
    assert!(shutdown_status.await.await.is_ok());
}
*/
