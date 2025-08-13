use crate::stream::Stream;
use crate::state::traits::KeyExtractor;
use crate::state::StateStorage;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
struct ThrottleState {
    count: u32,
    window_start: u64, // Store as timestamp for serialization
}

pub struct StatefulThrottleStream<S, F, T>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(T) -> T + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    stream: S,
    storage: Arc<dyn StateStorage + Send + Sync>,
    throttle_states: HashMap<String, ThrottleState>, // Keep in-memory for now
    key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
    f: F,
    rate_limit: u32,
    window_duration: Duration,
    current_item: Option<T>,
    next_emit_time: Option<Instant>,
    stream_done: bool,
}

impl<S, F, T> StatefulThrottleStream<S, F, T>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(T) -> T + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    pub fn new(
        stream: S,
        f: F,
        storage: Arc<dyn StateStorage + Send + Sync>,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        rate_limit: u32,
        window_duration: Duration,
    ) -> Self {
        Self {
            stream,
            storage,
            throttle_states: HashMap::new(),
            key_extractor,
            f,
            rate_limit,
            window_duration,
            current_item: None,
            next_emit_time: None,
            stream_done: false,
        }
    }

    fn unix_timestamp_millis() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

impl<S, F, T> Stream for StatefulThrottleStream<S, F, T>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(T) -> T + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: We never move fields that are !Unpin
        let this = unsafe { self.get_unchecked_mut() };

        // If we have a current item to process, handle it first
        if let Some(item) = this.current_item.take() {
            let key = this.key_extractor.extract_key(&item);
            let now = Instant::now();
            let now_timestamp = Self::unix_timestamp_millis();
            
            let throttle_state = this.throttle_states.entry(key.clone()).or_insert(ThrottleState {
                count: 0,
                window_start: now_timestamp,
            });

            // Reset window if expired
            if now_timestamp - throttle_state.window_start > this.window_duration.as_millis() as u64 {
                throttle_state.count = 0;
                throttle_state.window_start = now_timestamp;
            }

            // Check if we can emit this item
            if throttle_state.count < this.rate_limit {
                throttle_state.count += 1;
                return Poll::Ready(Some((this.f)(item)));
            } else {
                // Rate limit exceeded, delay this item
                let next_window_start = throttle_state.window_start + this.window_duration.as_millis() as u64;
                let delay_until = Instant::now() + Duration::from_millis((next_window_start - now_timestamp) as u64);
                
                if now >= delay_until {
                    // Window has passed, reset and emit
                    throttle_state.count = 1;
                    throttle_state.window_start = now_timestamp;
                    return Poll::Ready(Some((this.f)(item)));
                } else {
                    // Still in current window, delay until next window
                    this.current_item = Some(item);
                    this.next_emit_time = Some(delay_until);
                    
                    // Schedule wake-up at the delay time
                    let waker = cx.waker().clone();
                    let delay_duration = delay_until.duration_since(now);
                    
                    tokio::spawn(async move {
                        tokio::time::sleep(delay_duration).await;
                        waker.wake();
                    });
                    
                    return Poll::Pending;
                }
            }
        }

        // Check if we need to delay before processing next item
        if let Some(emit_time) = this.next_emit_time {
            if Instant::now() < emit_time {
                // Still need to wait
                let waker = cx.waker().clone();
                let delay_duration = emit_time.duration_since(Instant::now());
                
                tokio::spawn(async move {
                    tokio::time::sleep(delay_duration).await;
                    waker.wake();
                });
                
                return Poll::Pending;
            } else {
                // Delay period is over
                this.next_emit_time = None;
            }
        }

        // Try to get next item from stream
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.current_item = Some(item);
                // Continue polling to process this item
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(None) => {
                this.stream_done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
} 