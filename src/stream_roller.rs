use crate::{ItemKind, StreamItem};

use async_channel::*;
use futures_lite::*;

pub struct StreamRoller<St, Item, Id> {
    stream_id: Id,
    stream: St,
    stream_of_items: Sender<StreamItem<Item, Id>>,
}

impl<St, Item, Id> StreamRoller<St, Item, Id> {
    pub fn new(stream_id: Id, stream: St, stream_of_items: Sender<StreamItem<Item, Id>>) -> Self {
        Self {
            stream_id,
            stream,
            stream_of_items,
        }
    }

    pub fn into_stream(self) -> St {
        self.stream
    }
}

impl<St, Item, Id> StreamRoller<St, Item, Id> {
    pub async fn roll(&mut self)
    where
        Id: Clone,
        St: Stream<Item = Item>,
        St: Unpin,
    {
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
