use bytes::Bytes;

mod codec;
use codec::*;

mod halt;
use halt::*;

mod sender;
use sender::*;

mod id_gen;
pub use id_gen::*;

mod multiplexer;
pub use multiplexer::*;

mod multiplexer_senders;
pub use multiplexer_senders::*;

/* FIXME: move me!
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
type StreamId = usize;

#[derive(Clone, PartialEq, Debug)]
pub enum IncomingMessage {
    Bytes(Bytes),
    Linkdead,
}

#[derive(Clone, PartialEq, Debug)]
pub struct IncomingPacket {
    id: StreamId,
    message: IncomingMessage,
}

#[derive(Default, Clone, PartialEq, Debug)]
pub struct OutgoingPacket {
    ids: Vec<StreamId>,
    bytes: Bytes,
}
impl OutgoingPacket {
    pub fn new(ids: Vec<StreamId>, bytes: Bytes) -> Self {
        Self { ids, bytes }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ControlMessage {
    Shutdown,
}
