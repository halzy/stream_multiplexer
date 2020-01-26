use bytes::Bytes;

mod codec;
use codec::*;

mod halt;
use halt::*;

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

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ControlMessage {
    Shutdown,
}

type StreamId = usize;
