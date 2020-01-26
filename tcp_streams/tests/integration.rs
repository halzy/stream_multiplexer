use tcp_streams::*;

use tokio::net::TcpListener;
use tokio::sync::mpsc;

use std::io::Result as IoResult;

pub async fn bind() -> IoResult<TcpListener> {
    tracing::info!("Starting");
    let addrs = "127.0.0.1:0".to_string();
    tracing::info!("Binding {:?}", &addrs);
    TcpListener::bind(&addrs).await
}

#[tokio::test(basic_scheduler)]
async fn shutdown() {
    let mut socket = bind().await.unwrap();
    let (control_write, control_read) = mpsc::unbounded_channel();
    let mut tcp_streams = PacketMultiplexer::new();
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

    let (control_write, control_read) = mpsc::unbounded_channel();
    let mut tcp_streams = PacketMultiplexer::new();
    let shutdown_status =
        tokio::task::spawn(async move { tcp_streams.run(socket.incoming(), control_read).await });

    control_write.send(ControlMessage::Shutdown).unwrap();
    assert!(shutdown_status.await.is_ok());
}

#[tokio::test(basic_scheduler)]
async fn write_packets() {
    let mut socket = bind().await.unwrap();
    let local_addr = socket.local_addr().unwrap();

    let (control_write, control_read) = mpsc::unbounded_channel();
    let mut multiplexer = PacketMultiplexer::new();
    let run_loop = async move { multiplexer.run(socket.incoming(), control_read).await };
    let shutdown_status = tokio::task::spawn(run_loop);

    let client_1 = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    let client_2 = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    client_1.shutdown(std::net::Shutdown::Both).unwrap();
    client_2.shutdown(std::net::Shutdown::Both).unwrap();

    control_write.send(ControlMessage::Shutdown).unwrap();

    assert!(shutdown_status.await.is_ok());
}
