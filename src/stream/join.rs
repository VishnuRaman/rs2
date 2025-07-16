use crate::stream::Stream;
use crate::state::StateStorage;
use crate::state::stream_ext::StateAccess;
use crate::state::traits::KeyExtractor;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::collections::HashSet;

#[derive(Clone)]
struct LeftItemWithTime<T> {
    item: T,
    timestamp: u64,
    key: String,
}

#[derive(Clone)]
struct RightItemWithTime<U> {
    item: U,
    timestamp: u64,
    key: String,
}

pub struct StatefulJoinStream<S1, S2, F, T, U, R>
where
    S1: Stream<Item = T> + Send + Unpin,
    S2: Stream<Item = U> + Send + Unpin,
    F: FnMut(T, U, StateAccess) -> Pin<Box<dyn std::future::Future<Output = Result<R, crate::state::StateError>> + Send>> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    left_stream: S1,
    right_stream: S2,
    left_buffer: HashMap<String, Vec<LeftItemWithTime<T>>>,
    right_buffer: HashMap<String, Vec<RightItemWithTime<U>>>,
    storage: Arc<dyn StateStorage + Send + Sync>,
    key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
    other_key_extractor: Arc<dyn KeyExtractor<U> + Send + Sync>,
    f: F,
    window_duration: Duration,
    current_future: Option<Pin<Box<dyn std::future::Future<Output = Result<R, crate::state::StateError>> + Send>>>,
    left_done: bool,
    right_done: bool,
    emitted_pairs: HashSet<(String, usize, usize)>,
}

impl<S1, S2, F, T, U, R> StatefulJoinStream<S1, S2, F, T, U, R>
where
    S1: Stream<Item = T> + Send + Unpin,
    S2: Stream<Item = U> + Send + Unpin,
    F: FnMut(T, U, StateAccess) -> Pin<Box<dyn std::future::Future<Output = Result<R, crate::state::StateError>> + Send>> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    pub fn new(
        left_stream: S1,
        right_stream: S2,
        f: F,
        storage: Arc<dyn StateStorage + Send + Sync>,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        other_key_extractor: Arc<dyn KeyExtractor<U> + Send + Sync>,
        window_duration: Duration,
    ) -> Self {
        Self {
            left_stream,
            right_stream,
            left_buffer: HashMap::new(),
            right_buffer: HashMap::new(),
            storage,
            key_extractor,
            other_key_extractor,
            f,
            window_duration,
            current_future: None,
            left_done: false,
            right_done: false,
            emitted_pairs: HashSet::new(),
        }
    }

    fn unix_timestamp_millis() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn find_matches(&mut self) -> Option<(T, U, String)> {
        // Find all possible matches between left and right buffers
        for (key, left_items) in &self.left_buffer {
            if let Some(right_items) = self.right_buffer.get(key) {
                for (li, left_item) in left_items.iter().enumerate() {
                    for (ri, right_item) in right_items.iter().enumerate() {
                        if (left_item.timestamp as i64 - right_item.timestamp as i64).abs() 
                            <= self.window_duration.as_millis() as i64 {
                            let pair_id = (key.clone(), li, ri);
                            if !self.emitted_pairs.contains(&pair_id) {
                                self.emitted_pairs.insert(pair_id);
                                return Some((left_item.item.clone(), right_item.item.clone(), key.clone()));
                            }
                        }
                    }
                }
            }
        }
        None
    }
}

impl<S1, S2, F, T, U, R> Stream for StatefulJoinStream<S1, S2, F, T, U, R>
where
    S1: Stream<Item = T> + Send + Unpin,
    S2: Stream<Item = U> + Send + Unpin,
    F: FnMut(T, U, StateAccess) -> Pin<Box<dyn std::future::Future<Output = Result<R, crate::state::StateError>> + Send>> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    type Item = Result<R, crate::state::StateError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: We never move fields that are !Unpin
        let this = unsafe { self.get_unchecked_mut() };

        // If we have a current future, poll it
        if let Some(ref mut future) = this.current_future {
            match Pin::new(future).poll(cx) {
                Poll::Ready(result) => {
                    this.current_future = None;
                    return Poll::Ready(Some(result));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        // If both streams are done, we're finished
        if this.left_done && this.right_done {
            return Poll::Ready(None);
        }

        // Try to get items from both streams
        let mut left_item = None;
        let mut right_item = None;

        if !this.left_done {
            match Pin::new(&mut this.left_stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    left_item = Some(item);
                }
                Poll::Ready(None) => {
                    this.left_done = true;
                }
                Poll::Pending => {}
            }
        }

        if !this.right_done {
            match Pin::new(&mut this.right_stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    right_item = Some(item);
                }
                Poll::Ready(None) => {
                    this.right_done = true;
                }
                Poll::Pending => {}
            }
        }

        // Check presence before moving
        let left_present = left_item.is_some();
        let right_present = right_item.is_some();

        // Add items to buffers
        if let Some(item) = left_item {
            let key = this.key_extractor.extract_key(&item);
            let now = Self::unix_timestamp_millis();
            this.left_buffer.entry(key.clone()).or_insert_with(Vec::new)
                .push(LeftItemWithTime { item, timestamp: now, key });
        }

        if let Some(item) = right_item {
            let key = this.other_key_extractor.extract_key(&item);
            let now = Self::unix_timestamp_millis();
            this.right_buffer.entry(key.clone()).or_insert_with(Vec::new)
                .push(RightItemWithTime { item, timestamp: now, key });
        }

        // Look for matches
        if let Some((left, right, key)) = this.find_matches() {
            let state_access = StateAccess::new(this.storage.clone(), key);
            let future = (this.f)(left, right, state_access);
            this.current_future = Some(future);
            
            // Poll the future immediately
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            // If both streams are done and no matches found, we're finished
            if this.left_done && this.right_done {
                return Poll::Ready(None);
            }
            
            // If we got items from either stream, continue polling to process them
            if left_present || right_present {
                // Continue polling to get more items or find matches
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                // Both streams are pending and no matches found - wait for more data
                Poll::Pending
            }
        }
    }
} 