use crate::{ItemKind, StreamItem};

use async_channel::*;
use futures_lite::*;

pub struct StreamRoller<St, Id>
where
    St: Stream,
{
    stream_id: Id,
    stream: St,
    stream_of_items: Sender<StreamItem<St::Item, Id>>,
}

impl<St, Id> StreamRoller<St, Id>
where
    Id: Send + Sync + Clone,
    St: Send + Sync + Stream + Unpin,
    St::Item: Send + Sync + std::fmt::Debug,
{
    pub fn new(
        stream_id: Id,
        stream: St,
        stream_of_items: Sender<StreamItem<St::Item, Id>>,
    ) -> Self {
        Self {
            stream_id,
            stream,
            stream_of_items,
        }
    }

    pub fn into_stream(self) -> St {
        self.stream
    }

    pub async fn roll(&mut self) {
        self.stream_of_items
            .send(StreamItem {
                stream_id: self.stream_id.clone(),
                kind: ItemKind::Connected,
            })
            .await
            .unwrap();

        loop {
            let kind = match self.stream.next().await {
                Some(item) => ItemKind::Value(item),
                None => ItemKind::Disconnected,
            };

            let stop_looping = matches!(kind, ItemKind::Disconnected);

            match self
                .stream_of_items
                .send(StreamItem {
                    stream_id: self.stream_id.clone(),
                    kind,
                })
                .await
            {
                Ok(()) => {}
                Err(_err) => return,
            }

            if stop_looping {
                return;
            }
        }
    }
}
