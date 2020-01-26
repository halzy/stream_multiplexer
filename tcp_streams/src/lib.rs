use bytes::Bytes;

mod codec;
use codec::*;

mod stream_control;
use stream_control::*;

mod sender;
use sender::*;

mod multiplexer;
pub use multiplexer::*;

/*
fn listen_address() -> impl tokio::net::ToSocketAddrs + std::fmt::Debug {
    if cfg!(test) {
        // the :0 gives us a random port, chosen by the OS
        "127.0.0.1:0".to_string()
    } else {
        env::var("LISTEN_ADDR")
            .expect("LISTEN_ADDR missing from environment.")
            .parse::<String>()
            .expect("LISTEN_ADDR should be a string: 127.0.0.1:12345")
    }
}
*/

#[derive(Clone, PartialEq, Debug)]
pub struct IncomingPacket {
    stream_id: StreamId,
    bytes: Bytes,
}
#[derive(Clone, PartialEq, Debug)]
pub struct OutgoingPacket {
    stream_ids: Vec<StreamId>,
    bytes: Bytes,
}

pub enum ControlMessage {
    Shutdown,
}

type StreamId = usize;

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream;
    use tokio::net::TcpListener;

    pub async fn bind() -> IoResult<TcpListener> {
        tracing::info!("Starting");
        let addrs = "127.0.0.1:0".to_string();
        tracing::info!("Binding to {:?}", &addrs);
        TcpListener::bind(&addrs).await
    }

    #[tokio::test(basic_scheduler)]
    async fn shutdown() {
        let mut socket = bind().await.unwrap();
        let control_stream = stream::once(async { ControlMessage::Shutdown });
        let mut tcp_streams = PacketMultiplexer::new();
        let shutdown_status =
            tokio::task::spawn(
                async move { tcp_streams.run(socket.incoming(), control_stream).await },
            );

        assert!(shutdown_status.await.is_ok());
    }

    #[tokio::test(basic_scheduler)]
    async fn socket_shutdown() {
        let mut socket = bind().await.unwrap();
        let local_addr = socket.local_addr().unwrap();

        let mut stream = tokio::net::TcpStream::connect(local_addr).await.unwrap();
        let (_rx, _tx) = stream.split();
        stream.shutdown(std::net::Shutdown::Both).unwrap();

        let control_stream = stream::once(async { ControlMessage::Shutdown });
        let mut tcp_streams = PacketMultiplexer::new();
        let shutdown_status =
            tokio::task::spawn(
                async move { tcp_streams.run(socket.incoming(), control_stream).await },
            );

        assert!(shutdown_status.await.is_ok());
    }

    #[tokio::test(basic_scheduler)]
    async fn write_packets() {
        let mut socket = bind().await.unwrap();
        let local_addr = socket.local_addr().unwrap();

        let mut stream = tokio::net::TcpStream::connect(local_addr).await.unwrap();
        let (_rx, _tx) = stream.split();
        stream.shutdown(std::net::Shutdown::Both).unwrap();

        let control_stream = stream::once(async { ControlMessage::Shutdown });
        let mut tcp_streams = PacketMultiplexer::new();
        let shutdown_status =
            tokio::task::spawn(
                async move { tcp_streams.run(socket.incoming(), control_stream).await },
            );

        assert!(shutdown_status.await.is_ok());
    }
}
