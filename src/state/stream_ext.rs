use crate::resource_manager::ResourceManager;
use crate::stream::Stream;
use pin_project_lite::pin_project;
use std::task::{Context, Poll};
use std::pin::Pin;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::state::traits::KeyExtractor;
use crate::state::{StateConfig, StateError, StateStorage};
use std::future::Future;
use crate::stream::core::StreamExt as CoreStreamExt;
use std::collections::VecDeque;
use std::task::{RawWaker, RawWakerVTable, Waker};

// Memory management constants
const MAX_HASHMAP_KEYS: usize = 10_000;
const MAX_GROUP_SIZE: usize = 10_000; // Max items per group
const MAX_PATTERN_SIZE: usize = 1_000; // Max items per pattern
const CLEANUP_INTERVAL: u64 = 1000; // Cleanup every 1000 items (increased from 100)
const RESOURCE_TRACKING_INTERVAL: u64 = 100; // Track resources every 100 items
const DEFAULT_BUFFER_SIZE: usize = 1024;

// LRU eviction helper
fn evict_oldest_entries<K, V>(map: &mut HashMap<K, V>, max_keys: usize)
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Display + std::cmp::Ord,
    V: Clone,
{
    if map.len() > max_keys {
        let mut entries: Vec<_> = map.iter().map(|(k, _)| k.clone()).collect();
        entries.sort(); // Simple eviction strategy - could be improved with proper LRU
        let to_remove = entries.len() - max_keys;
        for key in entries.into_iter().take(to_remove) {
            map.remove(&key);
        }
    }
}

// Optimized resource tracking - batch operations
async fn track_resource_batch(
    resource_manager: &Arc<ResourceManager>,
    allocations: u64,
    deallocations: u64,
    buffer_overflows: u64,
) {
    if allocations > 0 {
        resource_manager.track_memory_allocation(allocations).await.ok();
    }
    if deallocations > 0 {
        resource_manager.track_memory_deallocation(deallocations).await;
    }
    for _ in 0..buffer_overflows {
        resource_manager.track_buffer_overflow().await.ok();
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct ThrottleState {
    count: u32,
    window_start: u64, // UNIX timestamp in milliseconds
}

#[derive(Serialize, Deserialize, Clone)]
struct SessionState {
    last_activity: u64, // UNIX timestamp
    is_new_session: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct LeftItemWithTime<T> {
    item: T,
    timestamp: u64,
    key: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct RightItemWithTime<U> {
    item: U,
    timestamp: u64,
    key: String,
}

// Custom stream implementations to replace async_stream::stream

/// Stateful map stream combinator
pub struct StatefulMap<S, F, R, T> {
    stream: S,
    f: F,
    storage: Arc<dyn StateStorage + Send + Sync>,
    key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
    current_future: Option<Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>>>,
    _phantom: std::marker::PhantomData<(R, T)>,
}

impl<S, F, R, T> StatefulMap<S, F, R, T>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
    F: FnMut(T, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
    R: Unpin,
{
    fn new(stream: S, f: F, storage: Arc<dyn StateStorage + Send + Sync>, key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>) -> Self {
        Self {
            stream,
            f,
            storage,
            key_extractor,
            current_future: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, F, R, T> Stream for StatefulMap<S, F, R, T>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
    F: FnMut(T, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
    R: Unpin,
{
    type Item = Result<R, StateError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        
        // First, poll the current future if we have one
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
        
        // If no current future, poll the stream for the next item
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let key = this.key_extractor.extract_key(&item);
                let state_access = StateAccess::new(this.storage.clone(), key);
                let future = (this.f)(item, state_access);
                this.current_future = Some(future);
                
                // Poll the future immediately
                if let Some(mut future) = this.current_future.take() {
                    match future.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            Poll::Ready(Some(result))
                        }
                        Poll::Pending => {
                            this.current_future = Some(future);
                            Poll::Pending
                        }
                    }
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stateful filter stream combinator
pub struct StatefulFilter<S, F, T> {
    stream: S,
    f: F,
    storage: Arc<dyn StateStorage + Send + Sync>,
    key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
    current_future: Option<Pin<Box<dyn Future<Output = Result<bool, StateError>> + Send>>>,
    current_item: Option<T>,
}

impl<S, F, T> StatefulFilter<S, F, T>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
    F: FnMut(&T, StateAccess) -> Pin<Box<dyn Future<Output = Result<bool, StateError>> + Send>> + Send + Sync + 'static,
{
    fn new(stream: S, f: F, storage: Arc<dyn StateStorage + Send + Sync>, key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>) -> Self {
        Self {
            stream,
            f,
            storage,
            key_extractor,
            current_future: None,
            current_item: None,
        }
    }
}

impl<S, F, T> Stream for StatefulFilter<S, F, T>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
    F: FnMut(&T, StateAccess) -> Pin<Box<dyn Future<Output = Result<bool, StateError>> + Send>> + Send + Sync + 'static,
{
    type Item = Result<T, StateError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        
        // First, poll the current future if we have one
        if let Some(mut future) = this.current_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Ready(Ok(should_include)) => {
                    let item = this.current_item.take().unwrap();
                    if should_include {
                        Poll::Ready(Some(Ok(item)))
                    } else {
                        // Continue to next item
                        self.poll_next(cx)
                    }
                }
                Poll::Ready(Err(e)) => {
                    this.current_item.take(); // Clear the item
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => {
                    this.current_future = Some(future);
                    Poll::Pending
                }
            }
        } else {
            // If no current future, poll the stream for the next item
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let key = this.key_extractor.extract_key(&item);
                    let state_access = StateAccess::new(this.storage.clone(), key);
                    let future = (this.f)(&item, state_access);
                    this.current_future = Some(future);
                    this.current_item = Some(item);
                    
                    // Poll the future immediately
                    if let Some(mut future) = this.current_future.take() {
                        match future.as_mut().poll(cx) {
                            Poll::Ready(Ok(should_include)) => {
                                let item = this.current_item.take().unwrap();
                                if should_include {
                                    Poll::Ready(Some(Ok(item)))
                                } else {
                                    // Continue to next item
                                    self.poll_next(cx)
                                }
                            }
                            Poll::Ready(Err(e)) => {
                                this.current_item.take(); // Clear the item
                                Poll::Ready(Some(Err(e)))
                            }
                            Poll::Pending => {
                                this.current_future = Some(future);
                                Poll::Pending
                            }
                        }
                    } else {
                        Poll::Pending
                    }
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

pin_project_lite::pin_project! {
    pub struct StatefulWindow<S, F, T, R> {
        #[pin]
        stream: S,
        f: F,
        storage: Arc<dyn StateStorage + Send + Sync>,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        window_size: usize,
        window_buffers: std::collections::HashMap<String, Vec<T>>,
        current_future: Option<Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>>>,
        result_queue: VecDeque<Result<R, StateError>>,
        _phantom: std::marker::PhantomData<(R, T)>,
    }
}

impl<S, F, T, R> StatefulWindow<S, F, T, R>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
    F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
    R: Unpin + 'static,
{
    fn new(
        stream: S,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_size: usize,
        f: F,
    ) -> Self {
        Self {
            stream,
            f,
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            window_size,
            window_buffers: std::collections::HashMap::new(),
            current_future: None,
            result_queue: VecDeque::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, F, T, R> Stream for StatefulWindow<S, F, T, R>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin,
    F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
    R: Unpin + 'static,
{
    type Item = Result<R, StateError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // First, emit any queued results
        if let Some(result) = this.result_queue.pop_front() {
            println!("StatefulWindow: emitting queued result");
            return Poll::Ready(Some(result));
        }
        
        // If we have a future in progress, poll it
        if let Some(mut future) = this.current_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    println!("StatefulWindow: window future ready, emitting result");
                    return Poll::Ready(Some(result));
                }
                Poll::Pending => {
                    *this.current_future = Some(future);
                    return Poll::Pending;
                }
            }
        }
        
        // Poll the source stream and process items
        loop {
            println!("StatefulWindow: polling source stream");
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let key = this.key_extractor.extract_key(&item);
                    let buffer = this.window_buffers.entry(key.clone()).or_insert_with(Vec::new);
                    buffer.push(item);
                    if buffer.len() >= *this.window_size {
                        let items: Vec<T> = buffer.drain(..*this.window_size).collect();
                        let state_access = StateAccess::new(this.storage.clone(), key.clone());
                        let future = (this.f)(items, state_access);
                        println!("StatefulWindow: emitting window for key {}", key);
                        *this.current_future = Some(Box::pin(future));
                        // Poll the future immediately to completion
                        let mut pinned = this.current_future.as_mut().unwrap();
                        match pinned.as_mut().poll(cx) {
                            Poll::Ready(result) => {
                                *this.current_future = None;
                                return Poll::Ready(Some(result));
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    // Source stream is exhausted, emit only full windows (no partial windows)
                    if !this.window_buffers.is_empty() {
                        for (key, mut buffer) in this.window_buffers.drain() {
                            if buffer.len() >= *this.window_size {
                                let items: Vec<T> = buffer.drain(..*this.window_size).collect();
                                let state_access = StateAccess::new(this.storage.clone(), key.clone());
                                let future = (this.f)(items, state_access);
                                *this.current_future = Some(Box::pin(future));
                                // Poll the future to completion synchronously
                                let waker = noop_waker();
                                let mut cx = std::task::Context::from_waker(&waker);
                                let mut pinned = this.current_future.as_mut().unwrap();
                                match pinned.as_mut().poll(&mut cx) {
                                    Poll::Ready(result) => {
                                        this.result_queue.push_back(result);
                                    }
                                    Poll::Pending => {
                                        this.result_queue.push_back(Err(StateError::Validation("Window future did not complete synchronously at end of stream".to_string())));
                                    }
                                }
                                *this.current_future = None;
                            }
                        }
                        if let Some(result) = this.result_queue.pop_front() {
                            return Poll::Ready(Some(result));
                        }
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Extension trait for adding stateful operations to streams
pub trait StatefulStreamExt<T>: Stream<Item = T> + Send + Sync + Sized + Unpin + 'static
where
    Self: 'static,
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Unpin + 'static,
{
    /// Apply a stateful map operation
    fn stateful_map_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        f: F,
    ) -> StatefulMap<Self, F, R, T>
    where
        F: FnMut(
                T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + Unpin + 'static,
        Self: Sized + Unpin,
    {
        let storage = config.create_storage_arc();
        StatefulMap::new(self, f, storage, Arc::new(key_extractor))
    }

    /// Apply a stateful filter operation
    fn stateful_filter_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        f: F,
    ) -> StatefulFilter<Self, F, T>
    where
        F: FnMut(
                &T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<bool, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        Self: Sized + Unpin,
    {
        let storage = config.create_storage_arc();
        StatefulFilter::new(self, f, storage, Arc::new(key_extractor))
    }

    /// Apply a stateful fold operation
    fn stateful_fold_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        initial: R,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(
                R,
                T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct FoldState<S, R, F, T> {
            stream: S,
            aggregates: std::collections::HashMap<String, R>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            initial: R,
        }
        let state = FoldState {
            stream: self,
            aggregates: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            initial,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let current_acc = state.aggregates.entry(key.clone()).or_insert_with(|| state.initial.clone());
                    let state_access = StateAccess::new(state.storage.clone(), key.clone());
                    match (state.f)(current_acc.clone(), item, state_access).await {
                        Ok(new_acc) => {
                            *current_acc = new_acc.clone();
                            Some((Ok(new_acc), state))
                        }
                        Err(e) => Some((Err(e), state)),
                    }
                }
                None => None,
            }
        })
    }

    /// Apply a stateful reduce operation
    fn stateful_reduce_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        initial: Option<R>,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(
                R,
                T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct ReduceState<S, R, F, T> {
            stream: S,
            acc: Option<R>,
            done: bool,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
        }
        let state = ReduceState {
            stream: self,
            acc: initial,
            done: false,
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
        };
        unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    if let Some(acc) = state.acc.take() {
                        let key = state.key_extractor.extract_key(&item);
                        let state_access = StateAccess::new(state.storage.clone(), key);
                        match (state.f)(acc, item, state_access).await {
                            Ok(new_acc) => {
                                state.acc = Some(new_acc.clone());
                                Some((Ok(new_acc), state))
                            }
                            Err(e) => {
                                state.done = true;
                                Some((Err(e), state))
                            }
                        }
                    } else {
                        // No initial accumulator provided
                        state.done = true;
                        Some((Err(StateError::Validation("No initial accumulator provided and cannot infer from first item".to_string())), state))
                    }
                }
                None => {
                    state.done = true;
                    if let Some(acc) = state.acc.take() {
                        Some((Ok(acc), state))
                    } else {
                        None
                    }
                }
            }
        })
    }

    /// Apply a stateful group by operation
    fn stateful_group_by_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        group_timeout: Option<Duration>,
        max_group_size: Option<usize>,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(
                String,
                Vec<T>,
                StateAccess,
            ) -> Pin<
                Box<dyn Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct GroupByState<S, F, T> {
            stream: S,
            groups: std::collections::HashMap<String, Vec<T>>,
            group_timestamps: std::collections::HashMap<String, std::time::Instant>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            max_size: usize,
            timeout: std::time::Duration,
        }
        let state = GroupByState {
            stream: self,
            groups: std::collections::HashMap::new(),
            group_timestamps: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            max_size: max_group_size.unwrap_or(100),
            timeout: group_timeout.unwrap_or(std::time::Duration::from_secs(60)),
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let now = std::time::Instant::now();
                    
                    // Add item to group
                    let group = state.groups.entry(key.clone()).or_insert_with(Vec::new);
                    group.push(item);
                    
                    // Update timestamp
                    state.group_timestamps.insert(key.clone(), now);
                    
                    // Check if group should be emitted
                    let should_emit = group.len() >= state.max_size;
                    
                    if should_emit {
                        let items = group.drain(..).collect();
                        state.group_timestamps.remove(&key); // Clean up timestamp when group is emitted
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(key, items, state_access).await {
                            Ok(result) => Some((Ok(result), state)),
                            Err(e) => Some((Err(e), state)),
                        }
                    } else {
                        // Continue collecting - return None to continue to next item
                        None
                    }
                }
                None => {
                    // Emit remaining groups
                    let mut results = Vec::new();
                    for (key, items) in state.groups.drain() {
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(key, items, state_access).await {
                            Ok(result) => results.push(Ok(result)),
                            Err(e) => results.push(Err(e)),
                        }
                    }
                    
                    if let Some(result) = results.pop() {
                        Some((result, state))
                    } else {
                        None
                    }
                }
            }
        })
    }

    /// Apply a stateful group by operation with advanced configuration
    fn stateful_group_by_advanced_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        group_timeout: Option<std::time::Duration>,
        max_group_size: Option<usize>,
        emit_on_key_change: bool,
        emit_on_group_change: bool,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(
                String,
                Vec<T>,
                StateAccess,
            ) -> Pin<
                Box<dyn Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct GroupByAdvancedState<S, F, T> {
            stream: S,
            groups: std::collections::HashMap<String, Vec<T>>,
            group_timestamps: std::collections::HashMap<String, std::time::Instant>,
            last_key: Option<String>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            max_size: usize,
            timeout: std::time::Duration,
            emit_on_key_change: bool,
            emit_on_group_change: bool,
        }
        let state = GroupByAdvancedState {
            stream: self,
            groups: std::collections::HashMap::new(),
            group_timestamps: std::collections::HashMap::new(),
            last_key: None,
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            max_size: max_group_size.unwrap_or(100),
            timeout: group_timeout.unwrap_or(std::time::Duration::from_secs(60)),
            emit_on_key_change,
            emit_on_group_change,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let now = std::time::Instant::now();
                    
                    // Check for key change
                    let key_changed = state.last_key.as_ref() != Some(&key);
                    if key_changed && state.emit_on_key_change {
                        // Emit previous group if it exists
                        if let Some(prev_key) = state.last_key.take() {
                            if let Some(items) = state.groups.remove(&prev_key) {
                                state.group_timestamps.remove(&prev_key); // Clean up timestamp
                                let state_access = StateAccess::new(state.storage.clone(), prev_key.clone());
                                match (state.f)(prev_key, items, state_access).await {
                                    Ok(result) => return Some((Ok(result), state)),
                                    Err(e) => return Some((Err(e), state)),
                                }
                            }
                        }
                    }
                    
                    // Add item to group
                    let group = state.groups.entry(key.clone()).or_insert_with(Vec::new);
                    group.push(item);
                    state.last_key = Some(key.clone());
                    
                    // Update timestamp
                    state.group_timestamps.insert(key.clone(), now);
                    
                    // Check if group should be emitted
                    let should_emit = group.len() >= state.max_size || 
                                    (state.emit_on_group_change && group.len() > 1) ||
                                    (now.duration_since(*state.group_timestamps.get(&key).unwrap_or(&now)) > state.timeout);
                    
                    if should_emit {
                        let items = group.drain(..).collect();
                        state.group_timestamps.remove(&key); // Clean up timestamp when group is emitted
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(key, items, state_access).await {
                            Ok(result) => Some((Ok(result), state)),
                            Err(e) => Some((Err(e), state)),
                        }
                    } else {
                        // Continue collecting - return None to continue to next item
                        None
                    }
                }
                None => {
                    // Emit remaining groups
                    let mut results = Vec::new();
                    for (key, items) in state.groups.drain() {
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(key, items, state_access).await {
                            Ok(result) => results.push(Ok(result)),
                            Err(e) => results.push(Err(e)),
                        }
                    }
                    
                    if let Some(result) = results.pop() {
                        Some((result, state))
                    } else {
                        None
                    }
                }
            }
        })
    }

    /// Apply a stateful deduplication operation
    fn stateful_deduplicate_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        ttl: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = Result<T, StateError>> + Send + 'static
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct DedupState<S, F, T> {
            stream: S,
            seen_items: std::collections::HashMap<String, std::time::Instant>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            ttl: std::time::Duration,
        }
        let state = DedupState {
            stream: self,
            seen_items: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            ttl,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let now = std::time::Instant::now();
                    let should_emit = if let Some(last_seen) = state.seen_items.get(&key) {
                        now.duration_since(*last_seen) > state.ttl
                    } else {
                        true
                    };
                    if should_emit {
                        state.seen_items.insert(key, now);
                        let transformed = (state.f)(item);
                        Some((Ok(transformed), state))
                    } else {
                        // Skip duplicate - return None to continue to next item
                        None
                    }
                }
                None => None,
            }
        })
    }

    /// Apply a stateful throttle operation
    fn stateful_throttle_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        rate_limit: u32,
        window_duration: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = Result<T, StateError>> + Send + 'static
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct ThrottleStateStruct<S, F, T> {
            stream: S,
            throttle_states: std::collections::HashMap<String, ThrottleState>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            rate_limit: u32,
            window_duration: std::time::Duration,
        }
        let state = ThrottleStateStruct {
            stream: self,
            throttle_states: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            rate_limit,
            window_duration,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let now = unix_timestamp_millis();
                    let throttle_state = state.throttle_states.entry(key.clone()).or_insert(ThrottleState {
                        count: 0,
                        window_start: now,
                    });
                    if now - throttle_state.window_start > state.window_duration.as_millis() as u64 {
                        throttle_state.count = 0;
                        throttle_state.window_start = now;
                    }
                    if throttle_state.count < state.rate_limit {
                        throttle_state.count += 1;
                        let transformed = (state.f)(item);
                        Some((Ok(transformed), state))
                    } else {
                        // Throttled - return None to continue to next item
                        None
                    }
                }
                None => None,
            }
        })
    }

    /// Apply a stateful session operation
    fn stateful_session_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        session_timeout: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = Result<T, StateError>> + Send + 'static
    where
        F: FnMut(T, bool) -> T + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct SessionStateStruct<S, F, T> {
            stream: S,
            session_states: std::collections::HashMap<String, SessionState>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            session_timeout: std::time::Duration,
        }
        let state = SessionStateStruct {
            stream: self,
            session_states: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            session_timeout,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let now = unix_timestamp_millis();
                    let session_state = state.session_states.entry(key.clone()).or_insert(SessionState {
                        last_activity: now,
                        is_new_session: true,
                    });
                    let is_new_session = if now - session_state.last_activity > state.session_timeout.as_millis() as u64 {
                        session_state.is_new_session = true;
                        true
                    } else {
                        session_state.is_new_session = false;
                        false
                    };
                    session_state.last_activity = now;
                    let transformed = (state.f)(item, is_new_session);
                    Some((Ok(transformed), state))
                }
                None => None,
            }
        })
    }

    /// Apply a stateful pattern operation
    fn stateful_pattern_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        pattern_size: usize,
        f: F,
    ) -> impl Stream<Item = Result<Option<String>, StateError>> + Send + 'static
    where
        F: FnMut(Vec<T>, StateAccess) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<String>, StateError>> + Send>> + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct PatternStateStruct<S, F, T> {
            stream: S,
            pattern_buffers: std::collections::HashMap<String, Vec<T>>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            pattern_size: usize,
        }
        let state = PatternStateStruct {
            stream: self,
            pattern_buffers: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            pattern_size,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let buffer = state.pattern_buffers.entry(key.clone()).or_insert_with(Vec::new);
                    buffer.push(item);
                    if buffer.len() >= state.pattern_size {
                        let items: Vec<T> = buffer.drain(..state.pattern_size).collect();
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(items, state_access).await {
                            Ok(pattern) => Some((Ok(pattern), state)),
                            Err(e) => Some((Err(e), state)),
                        }
                    } else {
                        Some((Ok(None), state))
                    }
                }
                None => None,
            }
        })
    }

    /// Join two streams based on keys with time-based windows
    fn stateful_join_rs2<U, F, R>(
        self,
        other: impl Stream<Item = U> + Send + Unpin + 'static,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        other_key_extractor: impl KeyExtractor<U> + Send + Sync + 'static,
        window_duration: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(T, U, StateAccess) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        U: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct JoinState<S, O, F, T, U> {
            stream: S,
            other: O,
            left_buffer: std::collections::HashMap<String, Vec<LeftItemWithTime<T>>>,
            right_buffer: std::collections::HashMap<String, Vec<RightItemWithTime<U>>>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            other_key_extractor: Arc<dyn KeyExtractor<U> + Send + Sync>,
            f: F,
            window_duration: std::time::Duration,
        }
        let state = JoinState {
            stream: self,
            other,
            left_buffer: std::collections::HashMap::new(),
            right_buffer: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            other_key_extractor: Arc::new(other_key_extractor),
            f,
            window_duration,
        };
        unfold(state, |mut state| async move {
            let left_item = CoreStreamExt::next(&mut state.stream).await;
            let right_item = CoreStreamExt::next(&mut state.other).await;
            match (left_item, right_item) {
                (Some(left), Some(right)) => {
                    let left_key = state.key_extractor.extract_key(&left);
                    let right_key = state.other_key_extractor.extract_key(&right);
                    let now = unix_timestamp_millis();
                    state.left_buffer.entry(left_key.clone()).or_insert_with(Vec::new)
                        .push(LeftItemWithTime { item: left, timestamp: now, key: left_key.clone() });
                    state.right_buffer.entry(right_key.clone()).or_insert_with(Vec::new)
                        .push(RightItemWithTime { item: right, timestamp: now, key: right_key.clone() });
                    if left_key == right_key {
                        if let (Some(left_items), Some(right_items)) = (state.left_buffer.get(&left_key), state.right_buffer.get(&right_key)) {
                            for left_item in left_items {
                                for right_item in right_items {
                                    if (left_item.timestamp as i64 - right_item.timestamp as i64).abs() <= state.window_duration.as_millis() as i64 {
                                        let state_access = StateAccess::new(state.storage.clone(), left_key.clone());
                                        match (state.f)(left_item.item.clone(), right_item.item.clone(), state_access).await {
                                            Ok(result) => return Some((Ok(result), state)),
                                            Err(e) => return Some((Err(e), state)),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // No match found - return None to continue to next items
                    None
                }
                (Some(left), None) => {
                    let key = state.key_extractor.extract_key(&left);
                    state.left_buffer.entry(key.clone()).or_insert_with(Vec::new)
                        .push(LeftItemWithTime { item: left, timestamp: unix_timestamp_millis(), key });
                    // Continue collecting - return None to continue to next items
                    None
                }
                (None, Some(right)) => {
                    let key = state.other_key_extractor.extract_key(&right);
                    state.right_buffer.entry(key.clone()).or_insert_with(Vec::new)
                        .push(RightItemWithTime { item: right, timestamp: unix_timestamp_millis(), key });
                    // Continue collecting - return None to continue to next items
                    None
                }
                (None, None) => None,
            }
        })
    }

    /// Apply a stateful window operation
    fn stateful_window_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_size: usize,
        f: F,
    ) -> StatefulWindow<Self, F, T, R>
    where
        F: FnMut(Vec<T>, StateAccess) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + Unpin + 'static,
        Self: Sized + Unpin,
    {
        StatefulWindow::new(
            self,
            config,
            key_extractor,
            window_size,
            f,
        )
    }

    /// Apply a stateful window operation with sliding window support
    fn stateful_window_rs2_advanced<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_size: usize,
        slide_size: Option<usize>,
        emit_partial: bool,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(Vec<T>, StateAccess) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct WindowAdvStateStruct<S, F, T> {
            stream: S,
            window_buffers: std::collections::HashMap<String, Vec<T>>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            window_size: usize,
            slide: usize,
            emit_partial: bool,
        }
        let state = WindowAdvStateStruct {
            stream: self,
            window_buffers: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            window_size,
            slide: slide_size.unwrap_or(1),
            emit_partial,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let buffer = state.window_buffers.entry(key.clone()).or_insert_with(Vec::new);
                    buffer.push(item);
                    let should_emit = if state.emit_partial {
                        buffer.len() >= state.slide
                    } else {
                        buffer.len() >= state.window_size
                    };
                    if should_emit {
                        let items: Vec<T> = if buffer.len() >= state.window_size {
                            buffer.drain(..state.window_size).collect()
                        } else {
                            buffer.drain(..).collect()
                        };
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(items, state_access).await {
                            Ok(result) => Some((Ok(result), state)),
                            Err(e) => Some((Err(e), state)),
                        }
                    } else {
                        // Window not ready yet - return None to continue to next item
                        None
                    }
                }
                None => None,
            }
        })
    }

    /// Apply a stateful aggregate operation
    fn stateful_aggregate_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        initial: R,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(R, T, StateAccess) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct AggStateStruct<S, F, T, R> {
            stream: S,
            aggregates: std::collections::HashMap<String, R>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            initial: R,
        }
        let state = AggStateStruct {
            stream: self,
            aggregates: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            initial,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let current_agg = state.aggregates.entry(key.clone()).or_insert_with(|| state.initial.clone());
                    let state_access = StateAccess::new(state.storage.clone(), key.clone());
                    match (state.f)(current_agg.clone(), item, state_access).await {
                        Ok(new_agg) => {
                            *current_agg = new_agg.clone();
                            Some((Ok(new_agg), state))
                        }
                        Err(e) => Some((Err(e), state)),
                    }
                }
                None => None,
            }
        })
    }

    /// Apply a stateful time window operation
    fn stateful_time_window_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_duration: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(Vec<T>, StateAccess) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct TimeWindowStateStruct<S, F, T> {
            stream: S,
            time_windows: std::collections::HashMap<String, (Vec<T>, std::time::Instant)>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            window_duration: std::time::Duration,
        }
        let state = TimeWindowStateStruct {
            stream: self,
            time_windows: std::collections::HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
            window_duration,
        };
        unfold(state, |mut state| async move {
            let next = CoreStreamExt::next(&mut state.stream).await;
            match next {
                Some(item) => {
                    let key = state.key_extractor.extract_key(&item);
                    let now = std::time::Instant::now();
                    let (buffer, window_start) = state.time_windows.entry(key.clone()).or_insert_with(|| {
                        (Vec::new(), now)
                    });
                    if now.duration_since(*window_start) > state.window_duration {
                        if !buffer.is_empty() {
                            let items = buffer.drain(..).collect();
                            let state_access = StateAccess::new(state.storage.clone(), key.clone());
                            match (state.f)(items, state_access).await {
                                Ok(result) => return Some((Ok(result), state)),
                                Err(e) => return Some((Err(e), state)),
                            }
                        }
                        *window_start = now;
                    }
                    buffer.push(item);
                    // Continue collecting - return None to continue to next item
                    None
                }
                None => {
                    // Emit remaining groups
                    let mut results = Vec::new();
                    for (key, (buffer, _)) in state.time_windows.drain() {
                        if !buffer.is_empty() {
                            let items = buffer;
                            let state_access = StateAccess::new(state.storage.clone(), key.clone());
                            match (state.f)(items, state_access).await {
                                Ok(result) => results.push(Ok(result)),
                                Err(e) => results.push(Err(e)),
                            }
                        }
                    }
                    
                    if let Some(result) = results.pop() {
                        Some((result, state))
                    } else {
                        None
                    }
                }
            }
        })
    }
}

pub struct StateAccess {
    storage: Arc<dyn StateStorage + Send + Sync>,
    key: String,
}

impl StateAccess {
    pub fn new(storage: Arc<dyn StateStorage + Send + Sync>, key: String) -> Self {
        Self { storage, key }
    }

    pub async fn get(&self) -> Option<Vec<u8>> {
        self.storage.get(&self.key).await
    }

    pub async fn set(&self, value: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.storage.set(&self.key, value).await
    }
}

impl Clone for StateAccess {
    fn clone(&self) -> Self {
        StateAccess {
            storage: self.storage.clone(),
            key: self.key.clone(),
        }
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn noop_waker() -> Waker {
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker { noop_raw_waker() }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

impl<T, S> StatefulStreamExt<T> for S
where
    S: Stream<Item = T> + Send + Sync + Unpin + 'static,
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Unpin + 'static,
{
} 