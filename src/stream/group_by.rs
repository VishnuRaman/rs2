//! Custom stream for stateful group-by combinators in rs2.
//! Supports emitting all remaining groups after the input stream ends.

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::future::Future;
use pin_project_lite::pin_project;

use crate::stream::Stream;
use crate::state::stream_ext::StateAccess;
use crate::state::{StateError, KeyExtractor};

/// Custom stream implementation for stateful group-by operations
pub struct StatefulGroupByStream<S, F, T, R>
where
    S: Stream<Item = T> + Send + 'static + Unpin,
    F: FnMut(String, Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
    T: Send + 'static + Unpin,
    R: Send + 'static,
{
    stream: S,
    groups: HashMap<String, Vec<T>>,
    group_timestamps: HashMap<String, Instant>,
    group_fn: F,
    key_extractor: Box<dyn KeyExtractor<T> + Send + Sync>,
    max_group_size: usize,
    group_timeout: Duration,
    state_access: StateAccess,
    pending_groups: Vec<(String, Vec<T>)>,
    current_future: Option<Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>>>,
    stream_done: bool,
}

impl<S, F, T, R> StatefulGroupByStream<S, F, T, R>
where
    S: Stream<Item = T> + Send + 'static + Unpin,
    F: FnMut(String, Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
    T: Send + 'static + Unpin,
    R: Send + 'static,
{
    pub fn new(
        stream: S,
        group_fn: F,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        max_group_size: usize,
        group_timeout: Duration,
        state_access: StateAccess,
    ) -> Self {
        Self {
            stream,
            groups: HashMap::new(),
            group_timestamps: HashMap::new(),
            group_fn,
            key_extractor: Box::new(key_extractor),
            max_group_size,
            group_timeout,
            state_access,
            pending_groups: Vec::new(),
            current_future: None,
            stream_done: false,
        }
    }
}

impl<S, F, T, R> Stream for StatefulGroupByStream<S, F, T, R>
where
    S: Stream<Item = T> + Send + 'static + Unpin,
    F: FnMut(String, Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
    T: Send + 'static + Unpin,
    R: Send + 'static,
{
    type Item = Result<R, StateError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // First, check if we have a pending future to complete
        if let Some(mut future) = this.current_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    return Poll::Ready(Some(result));
                }
                Poll::Pending => {
                    this.current_future = Some(future);
                    return Poll::Pending;
                }
            }
        }

        // If stream is done and no pending groups, we're finished
        if this.stream_done && this.pending_groups.is_empty() {
            return Poll::Ready(None);
        }

        // If stream is done, emit remaining groups
        if this.stream_done {
            if let Some((group_key, items)) = this.pending_groups.pop() {
                let state_access = StateAccess::new(this.state_access.get_storage(), group_key.clone());
                let future = (this.group_fn)(group_key, items, state_access);
                this.current_future = Some(future);
                return self.poll_next(cx);
            }
        }

        // Poll the input stream
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let group_key = this.key_extractor.extract_key(&item);
                
                // Add item to group
                this.groups.entry(group_key.clone()).or_insert_with(Vec::new).push(item);
                this.group_timestamps.entry(group_key.clone()).or_insert_with(Instant::now);

                // Check if group should be emitted
                if let Some(group) = this.groups.get(&group_key) {
                    if group.len() >= this.max_group_size {
                        let items = this.groups.remove(&group_key).unwrap();
                        this.group_timestamps.remove(&group_key);
                        this.pending_groups.push((group_key, items));
                    }
                }

                // Continue polling for more items
                self.poll_next(cx)
            }
            Poll::Ready(None) => {
                // Stream is done, emit all remaining groups
                this.stream_done = true;
                for (group_key, items) in this.groups.drain() {
                    this.pending_groups.push((group_key, items));
                }
                this.group_timestamps.clear();
                
                // Emit the first pending group
                if let Some((group_key, items)) = this.pending_groups.pop() {
                    let state_access = StateAccess::new(this.state_access.get_storage(), group_key.clone());
                    let future = (this.group_fn)(group_key, items, state_access);
                    this.current_future = Some(future);
                    return self.poll_next(cx);
                }
                
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
} 