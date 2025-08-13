use crate::resource_manager::{ResourceConfig, ResourceManager};
use crate::stream::Stream;
use crate::rs2_stream_ext::RS2StreamExt;
use std::task::{Context, Poll, Waker, RawWaker, RawWakerVTable};
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
use crate::session::{get_global_state_config, get_global_buffer_config};

// Memory management constants
const MAX_BUFFER_SIZE: usize = 10_000; // Max items per buffer
const MAX_WINDOW_KEYS: usize = 1_000; // Max keys for windowing

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
pin_project_lite::pin_project! {
    pub struct StatefulMap<S, F, R, T> {
        #[pin]
        stream: S,
        f: F,
        storage: Arc<dyn StateStorage + Send + Sync>,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        current_future: Option<Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>>>,
        _phantom: std::marker::PhantomData<(R, T)>,
    }
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // First, poll the current future if we have one
        if let Some(mut future) = this.current_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    return Poll::Ready(Some(result));
                }
                Poll::Pending => {
                    *this.current_future = Some(future);
                    return Poll::Pending;
                }
            }
        }
        
        // If no current future, poll the stream for the next item
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let key = this.key_extractor.extract_key(&item);
                let state_access = StateAccess::new(this.storage.clone(), key);
                let future = (this.f)(item, state_access);
                *this.current_future = Some(future);
                
                // Poll the future immediately
                if let Some(mut future) = this.current_future.take() {
                    match future.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            Poll::Ready(Some(result))
                        }
                        Poll::Pending => {
                            *this.current_future = Some(future);
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
pin_project_lite::pin_project! {
    pub struct StatefulFilter<S, F, T> {
        #[pin]
        stream: S,
        f: F,
        storage: Arc<dyn StateStorage + Send + Sync>,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        current_future: Option<Pin<Box<dyn Future<Output = Result<bool, StateError>> + Send>>>,
        current_item: Option<T>,
    }
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
        loop {
            let mut this = self.as_mut().project();
            
            // First, poll the current future if we have one
            if let Some(mut future) = this.current_future.take() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(should_include)) => {
                        let item = this.current_item.take().unwrap();
                        if should_include {
                            return Poll::Ready(Some(Ok(item)));
                        } else {
                            // Continue to next item by continuing the loop
                            continue;
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        this.current_item.take(); // Clear the item
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        *this.current_future = Some(future);
                        return Poll::Pending;
                    }
                }
            } else {
                // If no current future, poll the stream for the next item
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let key = this.key_extractor.extract_key(&item);
                        let state_access = StateAccess::new(this.storage.clone(), key);
                        let future = (this.f)(&item, state_access);
                        *this.current_future = Some(future);
                        *this.current_item = Some(item);
                        
                        // Poll the future immediately
                        if let Some(mut future) = this.current_future.take() {
                            match future.as_mut().poll(cx) {
                                Poll::Ready(Ok(should_include)) => {
                                    let item = this.current_item.take().unwrap();
                                    if should_include {
                                        return Poll::Ready(Some(Ok(item)));
                                    } else {
                                        // Continue to next item by continuing the loop
                                        continue;
                                    }
                                }
                                Poll::Ready(Err(e)) => {
                                    this.current_item.take(); // Clear the item
                                    return Poll::Ready(Some(Err(e)));
                                }
                                Poll::Pending => {
                                    *this.current_future = Some(future);
                                    return Poll::Pending;
                                }
                            }
                        } else {
                            return Poll::Pending;
                        }
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
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
        window_buffers: HashMap<String, Vec<T>>,
        current_future: Option<Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>>>,
        result_queue: VecDeque<Result<R, StateError>>,
        resource_manager: Arc<ResourceManager>,
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
        resource_manager: Arc<ResourceManager>,
    ) -> Self {
        Self {
            stream,
            f,
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            window_size,
            window_buffers: HashMap::new(),
            current_future: None,
            result_queue: VecDeque::new(),
            resource_manager,
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
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let key = this.key_extractor.extract_key(&item);
                    
                    // Track resource usage when creating new buffer
                    let is_new_buffer = !this.window_buffers.contains_key(&key);
                    if is_new_buffer {
                        if this.window_buffers.len() >= MAX_WINDOW_KEYS {
                            // Track buffer overflow
                            tokio::spawn({
                                let rm = this.resource_manager.clone();
                                async move {
                                    track_resource_batch(&rm, 0, 0, 1).await;
                                }
                            });
                            // Drop oldest key to make room
                            if let Some(oldest_key) = this.window_buffers.keys().next().cloned() {
                                this.window_buffers.remove(&oldest_key);
                            }
                        }
                        // Track new buffer allocation
                        tokio::spawn({
                            let rm = this.resource_manager.clone();
                            async move {
                                track_resource_batch(&rm, 1, 0, 0).await;
                            }
                        });
                    }
                    
                    let buffer = this.window_buffers.entry(key.clone()).or_insert_with(Vec::new);
                    buffer.push(item);
                    
                    // Check for buffer overflow
                    if buffer.len() > MAX_BUFFER_SIZE {
                        // Track buffer overflow
                        tokio::spawn({
                            let rm = this.resource_manager.clone();
                            async move {
                                track_resource_batch(&rm, 0, 0, 1).await;
                            }
                        });
                        // Drop oldest items to maintain size limit
                        buffer.drain(0..buffer.len() - MAX_BUFFER_SIZE);
                    }
                    
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
                        let buffer_count = this.window_buffers.len();
                        for (key, mut buffer) in this.window_buffers.drain() {
                            if buffer.len() >= *this.window_size {
                                let items: Vec<T> = buffer.drain(..*this.window_size).collect();
                                let state_access = StateAccess::new(this.storage.clone(), key.clone());
                                let future = (this.f)(items, state_access);
                                *this.current_future = Some(Box::pin(future));
                                // Poll the future to completion synchronously
                                let waker = noop_waker();
                                let mut cx = Context::from_waker(&waker);
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
                        
                        // Track buffer deallocations
                        tokio::spawn({
                            let rm = this.resource_manager.clone();
                            async move {
                                track_resource_batch(&rm, 0, buffer_count as u64, 0).await;
                            }
                        });
                        
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
            ) -> Pin<
                Box<dyn Future<Output = Result<R, StateError>> + Send>,
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
        f: F
    ) -> StatefulFilter<Self, F, T>
    where
        F: FnMut(
                &T,
                StateAccess,
            ) -> Pin<
                Box<dyn Future<Output = Result<bool, StateError>> + Send>,
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
        f: F
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(
                R,
                T,
                StateAccess,
            ) -> Pin<
                Box<dyn Future<Output = Result<R, StateError>> + Send>,
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
            aggregates: HashMap<String, R>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            initial: R,
        }
        let state = FoldState {
            stream: self,
            aggregates: HashMap::new(),
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
        f: F
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(
                R,
                T,
                StateAccess,
            ) -> Pin<
                Box<dyn Future<Output = Result<R, StateError>> + Send>,
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
            accs: HashMap<String, R>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
        }
        let state = ReduceState {
            stream: self,
            accs: HashMap::new(),
            storage: config.create_storage_arc(),
            key_extractor: Arc::new(key_extractor),
            f,
        };
        let initial_clone = initial.clone();
        unfold(state, move |mut state| {
            let initial = initial_clone.clone();
            async move {
                let next = CoreStreamExt::next(&mut state.stream).await;
                match next {
                    Some(item) => {
                        let key = state.key_extractor.extract_key(&item);
                        let acc = state.accs.entry(key.clone()).or_insert_with(|| {
                            initial.clone().expect("No initial accumulator provided and cannot infer from first item")
                        });
                        let state_access = StateAccess::new(state.storage.clone(), key.clone());
                        match (state.f)(acc.clone(), item, state_access).await {
                            Ok(new_acc) => {
                                *acc = new_acc.clone();
                                Some((Ok(new_acc), state))
                            }
                            Err(e) => Some((Err(e), state)),
                        }
                    }
                    None => None,
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
        f: F
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
            + 'static
            + Unpin,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::group_by::StatefulGroupByStream;
        let storage = config.create_storage_arc();
        let state_access = StateAccess::new(storage.clone(), String::new());
        
        StatefulGroupByStream::new(
            self,
            f,
            key_extractor,
            max_group_size.unwrap_or(100),
            group_timeout.unwrap_or(Duration::from_secs(60)),
            state_access,
        )
    }

    /// Apply a stateful group by operation with advanced configuration
    fn stateful_group_by_advanced_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        group_timeout: Option<Duration>,
        max_group_size: Option<usize>,
        f: F
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
            + 'static
            + Unpin,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::group_by::StatefulGroupByStream;
        let storage = config.create_storage_arc();
        let state_access = StateAccess::new(storage.clone(), String::new());
        
        StatefulGroupByStream::new(
            self,
            f,
            key_extractor,
            max_group_size.unwrap_or(100),
            group_timeout.unwrap_or(Duration::from_secs(60)),
            state_access,
        )
    }

    /// Apply a stateful deduplication operation
    fn stateful_deduplicate_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        ttl: Duration,
        f: F
    ) -> impl Stream<Item = Result<T, StateError>> + Send + 'static
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::deduplicate::StatefulDeduplicateStream;
        use crate::stream::core::StreamExt;
        use std::sync::Arc;
        
        let storage = config.create_storage_arc();
        let key_extractor = Arc::new(key_extractor);
        
        StatefulDeduplicateStream::new(
            self,
            f,
            key_extractor,
            ttl,
            storage,
        )
        .map(|item| Ok(item))
    }

    /// Apply a stateful throttle operation
    fn stateful_throttle_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        rate_limit: u32,
        window_duration: Duration,
        f: F,
    ) -> impl Stream<Item = Result<T, StateError>> + Send + 'static
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::stateful_throttle::StatefulThrottleStream;
        use crate::stream::core::StreamExt;
        use std::sync::Arc;
        
        let storage = config.create_storage_arc();
        let key_extractor = Arc::new(key_extractor);
        
        StatefulThrottleStream::new(
            self,
            f,
            storage,
            key_extractor,
            rate_limit,
            window_duration,
        )
        .map(|item| Ok(item))
    }

    /// Apply a stateful session operation
    fn stateful_session_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        session_timeout: Duration,
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
            session_states: HashMap<String, SessionState>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            session_timeout: Duration,
        }
        let state = SessionStateStruct {
            stream: self,
            session_states: HashMap::new(),
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
        F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<Option<String>, StateError>> + Send>> + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct PatternStateStruct<S, F, T> {
            stream: S,
            pattern_buffers: HashMap<String, Vec<T>>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            pattern_size: usize,
        }
        let state = PatternStateStruct {
            stream: self,
            pattern_buffers: HashMap::new(),
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
        window_duration: Duration,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(T, U, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        U: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::join::StatefulJoinStream;
        use std::sync::Arc;
        
        let key_extractor = Arc::new(key_extractor);
        let other_key_extractor = Arc::new(other_key_extractor);
        
        let storage = config.create_storage_arc();
        
        StatefulJoinStream::new(
            self,
            other,
            f,
            storage,
            key_extractor,
            other_key_extractor,
            window_duration,
        )
    }

    /// Apply a stateful window operation
    fn stateful_window_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_size: usize,
        f: F,
        resource_config: ResourceConfig,
    ) -> StatefulWindow<Self, F, T, R>
    where
        F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + Unpin + 'static,
        Self: Sized + Unpin,
    {
        let resource_manager = Arc::new(ResourceManager::with_config(resource_config));
        StatefulWindow::new(
            self,
            config,
            key_extractor,
            window_size,
            f,
            resource_manager,
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
        F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
        R: Send + Sync + 'static + Unpin,
        Self: Sized + Unpin,
    {
        use crate::stream::window::StatefulWindowStream;
        use std::sync::Arc;
        
        let storage = config.create_storage_arc();
        let key_extractor = Arc::new(key_extractor);
        let slide_size = slide_size.unwrap_or(1);
        
        StatefulWindowStream::new(
            self,
            storage,
            key_extractor,
            f,
            window_size,
            slide_size,
            emit_partial,
        )
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
        F: FnMut(R, T, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct AggStateStruct<S, F, T, R> {
            stream: S,
            aggregates: HashMap<String, R>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            initial: R,
        }
        let state = AggStateStruct {
            stream: self,
            aggregates: HashMap::new(),
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
        window_duration: Duration,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        use crate::stream::constructors::unfold;
        use crate::stream::core::StreamExt as CoreStreamExt;
        use std::sync::Arc;
        struct TimeWindowStateStruct<S, F, T> {
            stream: S,
            time_windows: HashMap<String, (Vec<T>, std::time::Instant)>,
            storage: Arc<dyn StateStorage + Send + Sync>,
            key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
            f: F,
            window_duration: Duration,
        }
        let state = TimeWindowStateStruct {
            stream: self,
            time_windows: HashMap::new(),
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

    /// Stateful map with session-aware configuration
    fn stateful_map_with_session_rs2<F, R>(
        self,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(T, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + Unpin + 'static,
        Self: Sized + Unpin,
    {
        let config = crate::session::get_global_state_config()
            .unwrap_or_else(StateConfig::default);
        self.stateful_map_rs2(config, key_extractor, f)
    }

    /// Stateful filter with session-aware configuration
    fn stateful_filter_with_session_rs2<F>(
        self,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        f: F,
    ) -> impl Stream<Item = Result<T, StateError>> + Send + 'static
    where
        F: FnMut(&T, StateAccess) -> Pin<Box<dyn Future<Output = Result<bool, StateError>> + Send>> + Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        let config = crate::session::get_global_state_config()
            .unwrap_or_else(StateConfig::default);
        self.stateful_filter_rs2(config, key_extractor, f)
    }

    /// Stateful aggregate with session-aware configuration
    fn stateful_aggregate_with_session_rs2<F, R>(
        self,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        initial: R,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(R, T, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized + Unpin,
    {
        let config = crate::session::get_global_state_config()
            .unwrap_or_else(StateConfig::default);
        self.stateful_aggregate_rs2(config, key_extractor, initial, f)
    }

    /// Stateful group by with session-aware configuration
    fn stateful_group_by_with_session_rs2<F, R>(
        self,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        f: F,
    ) -> impl Stream<Item = Result<R, StateError>> + Send + 'static
    where
        F: FnMut(String, Vec<T>, StateAccess) -> Pin<Box<dyn Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
        R: Send + Sync + 'static,
        Self: Sized + Unpin,
    {
        let config = crate::session::get_global_state_config()
            .unwrap_or_else(StateConfig::default);
        self.stateful_group_by_rs2(config, key_extractor, None, None, f)
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

    pub fn get_storage(&self) -> Arc<dyn StateStorage + Send + Sync> {
        self.storage.clone()
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
    // This creates a no-op waker that does nothing when woken
    // It's used in contexts where we need a waker but don't actually want to wake anything
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker { noop_raw_waker() }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    // SAFETY: This is safe because:
    // 1. The RawWaker is created with a null pointer and a valid VTable
    // 2. The VTable functions are all no-ops that don't access the data pointer
    // 3. The waker is only used in contexts where it won't be woken
    unsafe { Waker::from_raw(noop_raw_waker()) }
} 

// Blanket implementation for StatefulStreamExt
impl<T, S> StatefulStreamExt<T> for S
where
    S: Stream<Item = T> + Send + Sync + Sized + Unpin + 'static,
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Unpin + 'static,
{
} 