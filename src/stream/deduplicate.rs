use crate::stream::Stream;
use crate::state::StateStorage;
use crate::state::traits::KeyExtractor;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub struct StatefulDeduplicateStream<S, F, T>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(T) -> T + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    stream: S,
    seen_items: HashMap<String, Instant>,
    storage: Arc<dyn StateStorage + Send + Sync>,
    key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
    f: F,
    ttl: Duration,
    current_item: Option<T>,
    stream_done: bool,
}

impl<S, F, T> StatefulDeduplicateStream<S, F, T>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(T) -> T + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    pub fn new(
        stream: S,
        f: F,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        ttl: Duration,
        storage: Arc<dyn StateStorage + Send + Sync>,
    ) -> Self {
        Self {
            stream,
            seen_items: HashMap::new(),
            storage,
            key_extractor,
            f,
            ttl,
            current_item: None,
            stream_done: false,
        }
    }
}

impl<S, F, T> Stream for StatefulDeduplicateStream<S, F, T>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(T) -> T + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: We never move fields that are !Unpin
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            // If we have a current item to process, handle it first
            if let Some(item) = this.current_item.take() {
                let key = this.key_extractor.extract_key(&item);
                let now = Instant::now();
                let should_emit = if let Some(last_seen) = this.seen_items.get(&key) {
                    now.duration_since(*last_seen) > this.ttl
                } else {
                    true
                };
                if should_emit {
                    this.seen_items.insert(key, now);
                    let transformed = (this.f)(item);
                    return Poll::Ready(Some(transformed));
                } else {
                    continue;
                }
            }
            if this.stream_done {
                return Poll::Ready(None);
            }
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.current_item = Some(item);
                    continue;
                }
                Poll::Ready(None) => {
                    this.stream_done = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
} 