//! Extension traits for streams

use crate::rs2::{self, BackpressureConfig};
use crate::stream::constructors::ConstructorStreamExt;
use crate::stream::{
    AdvancedStreamExt, SelectStreamExt, SpecializedStreamExt, Stream, StreamExt, UtilityStreamExt,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use crate::stream_performance_metrics::StreamMetrics;
use crate::stream::round_robin::RoundRobinInterleave;

/// Extension trait for streams
pub trait RS2StreamExt: Stream + Sized + Send + 'static {
    /// Map over stream items
    fn map_rs2<F, U>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> U + Send + 'static,
        U: Send + 'static,
    {
        self.map(f)
    }

    /// Filter stream items
    fn filter_rs2<F>(self, f: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: Fn(&Self::Item) -> bool + Send + 'static,
    {
        self.filter(f)
    }

    /// Take n items from the stream
    fn take_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static {
        self.take(n)
    }

    /// Skip n items from the stream
    fn skip_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static {
        self.skip(n)
    }

    fn drop_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static {
        self.skip(n)
    }

    /// Chain with another stream
    fn chain_rs2<U>(self, other: U) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        U: Stream<Item = Self::Item> + Send + 'static,
    {
        self.chain(other)
    }

    /// Zip with another stream
    fn zip_rs2<U>(self, other: U) -> impl Stream<Item = (Self::Item, U::Item)> + Send + 'static
    where
        U: Stream + Send + 'static,
        U::Item: Send + 'static,
        Self::Item: Send + 'static,
    {
        self.zip(other)
    }

    /// Merge with another stream
    fn merge_rs2<U>(self, other: U) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        U: Stream<Item = Self::Item> + Send + 'static,
    {
        self.merge(other)
    }

    /// Collect into a vector
    async fn collect_rs2(self) -> Vec<Self::Item> {
        use crate::stream::StreamExt;
        self.collect().await
    }

    /// Collect all items from the stream into a collection with custom buffer configuration
    ///
    /// This combinator collects all items from the stream into a collection of type B.
    /// It returns a Future that resolves to the collection.
    /// The buffer configuration allows for optimized memory allocation and growth strategies.
    async fn collect_with_config_rs2<B>(self, config: crate::stream_configuration::BufferConfig) -> B
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
        Self: Unpin,
    {
        use crate::stream::StreamExt;
        let mut collection = B::default();
        let mut count = 0;
        let max = config.max_capacity;
        let mut stream = std::pin::pin!(self);
        loop {
            if let Some(max) = max {
                if count >= max {
                    break;
                }
            }
            match StreamExt::next(stream.as_mut().get_mut()).await {
                Some(item) => {
                    collection.extend(std::iter::once(item));
                    count += 1;
                }
                None => break,
            }
        }
        collection
    }

    /// Fold over the stream
    async fn fold_rs2<B, F>(mut self, init: B, f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B + Send + 'static,
        B: Send + 'static + Clone,
    {
        let items = self.collect_rs2().await;
        items.into_iter().fold(init, f)
    }

    /// Reduce the stream using a binary operation
    /// Returns None if the stream is empty, otherwise returns Some with the reduced value
    async fn reduce_rs2<F>(self, f: F) -> Option<Self::Item>
    where
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + 'static,
        Self::Item: Send + 'static,
    {
        let items = self.collect_rs2().await;
        let mut iter = items.into_iter();
        
        if let Some(first) = iter.next() {
            Some(iter.fold(first, f))
        } else {
            None
        }
    }

    /// Count items in the stream
    async fn count_rs2(self) -> usize {
        use crate::stream::StreamExt;
        self.collect::<Vec<_>>().await.len()
    }

    /// Get the first item
    fn first_rs2(self) -> impl std::future::Future<Output = Option<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        self.nth(0)
    }

    /// Get the last item
    async fn last_rs2(self) -> Option<Self::Item> {
        use crate::stream::StreamExt;
        let items = self.collect::<Vec<_>>().await;
        items.into_iter().last()
    }

    /// Find an item matching a predicate
    async fn find_rs2<F>(self, f: F) -> Option<Self::Item>
    where
        F: Fn(&Self::Item) -> bool + Send + 'static,
    {
        use crate::stream::StreamExt;
        let items = self.collect::<Vec<_>>().await;
        items.into_iter().find(f)
    }

    /// Check if any item matches a predicate
    async fn any_rs2<F>(self, f: F) -> bool
    where
        F: Fn(&Self::Item) -> bool + Send + 'static,
    {
        use crate::stream::StreamExt;
        let items = self.collect::<Vec<_>>().await;
        items.iter().any(f)
    }

    /// Check if all items match a predicate
    async fn all_rs2<F>(self, f: F) -> bool
    where
        F: Fn(&Self::Item) -> bool + Send + 'static,
    {
        use crate::stream::StreamExt;
        let items = self.collect::<Vec<_>>().await;
        items.iter().all(f)
    }

    /// Get the nth item
    async fn nth_rs2(self, n: usize) -> Option<Self::Item> {
        use crate::stream::StreamExt;
        let items = self.collect::<Vec<_>>().await;
        items.into_iter().nth(n)
    }

    /// Get the position of an item matching a predicate
    async fn position_rs2<F>(mut self, f: F) -> Option<usize>
    where
        F: Fn(&Self::Item) -> bool + Send + 'static,
        Self: Unpin,
    {
        let mut position = 0;
        let mut stream = std::pin::Pin::new(&mut self);
        while let Some(item) = stream.as_mut().next().await {
            if f(&item) {
                return Some(position);
            }
            position += 1;
        }
        None
    }

    /// Chunk items into vectors
    fn chunks_rs2(self, size: usize) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        self.chunks(size)
    }

    /// Window items with overlap
    fn sliding_window_with_step_rs2(
        self,
        size: usize,
        step: usize,
    ) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self: Sized + 'static,
        Self::Item: Send + 'static + Clone,
    {
        use crate::stream::specialized::SpecializedStreamExt;
        use crate::stream::Either;
        if size == 0 {
            Either::Left(crate::stream::constructors::empty::<Vec<Self::Item>>())
        } else {
            Either::Right(self.sliding_window_with_step(size, step))
        }
    }

    /// Enumerate items
    fn enumerate_rs2(self) -> impl Stream<Item = (usize, Self::Item)> + Send + 'static {
        self.enumerate()
    }

    /// Inspect items without modifying them
    fn inspect_rs2<F>(self, f: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: Fn(&Self::Item) + Send + 'static,
    {
        self.inspect(f)
    }

    /// Peek at the next item without consuming it
    fn peekable_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        self.peekable()
    }

    /// Skip while a predicate is true
    fn skip_while_rs2<F>(self, f: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.skip_while(f)
    }

    /// Drop while a predicate is true (same as skip_while)
    fn drop_while_rs2<F>(self, f: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        crate::rs2::drop_while(self, f)
    }

    /// Take while a predicate is true
    fn take_while_rs2<F>(self, f: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.take_while(f)
    }

    /// Scan over the stream
    fn scan_rs2<St, F, B>(self, initial_state: St, f: F) -> impl Stream<Item = B> + Send + 'static
    where
        F: Fn(&mut St, Self::Item) -> Option<B> + Send + 'static,
        St: Send + 'static,
        B: Send + 'static,
    {
        self.scan(initial_state, f)
    }

    /// Apply a function to each element in the stream
    ///
    /// This combinator applies a function to each element in the stream without accumulating a result.
    /// It returns a Future that completes when the stream is exhausted.
    async fn for_each_rs2<F, Fut>(mut self, mut f: F)
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        Self::Item: Send + 'static,
        Self: Unpin,
    {
        let mut stream = std::pin::Pin::new(&mut self);
        while let Some(item) = stream.as_mut().next().await {
            f(item).await;
        }
    }

    /// Flat map over the stream
    fn flat_map_rs2<U, F>(self, f: F) -> impl Stream<Item = U::Item> + Send + 'static
    where
        U: Stream + Send + 'static,
        F: FnMut(Self::Item) -> U + Send + 'static,
        Self::Item: Send + 'static,
        U::Item: Send + 'static,
    {
        self.flat_map::<U::Item, U, F>(f)
    }

    /// Map elements of the stream with an async function
    fn eval_map_rs2<U, Fut, F>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
        Self::Item: Send + 'static,
    {
        rs2::eval_map(self, f)
    }

    /// Flatten nested streams
    fn flatten_rs2(self) -> impl Stream<Item = <Self::Item as Stream>::Item> + Send + 'static
    where
        Self::Item: Stream + Send + 'static,
        <Self::Item as Stream>::Item: Send + 'static,
    {
        StreamExt::flatten(self)
    }

    /// Filter and map over stream items
    fn filter_map_rs2<U, F>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        U: Send + 'static,
        F: FnMut(Self::Item) -> Option<U> + Send + 'static,
        Self::Item: Send + 'static,
    {
        StreamExt::filter_map(self, f)
    }

    /// Filter and map over stream items with an async function
    fn filter_map_async_rs2<F, Fut, U>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = Option<U>> + Send + 'static,
        U: Send + 'static,
        Self::Item: Send + 'static,
        Self: Unpin,
    {
        use crate::stream::async_combinators::AsyncStreamExt;
        self.filter_map_async(f)
    }

    /// Process items in parallel with bounded concurrency
    fn par_eval_map_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static,
    {
        rs2::par_eval_map(self, concurrency, f)
    }

    /// Apply backpressure with default configuration
    fn auto_backpressure_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::auto_backpressure_block(self, BackpressureConfig::default())
    }

    /// Apply backpressure with custom configuration
    fn auto_backpressure_with_rs2(
        self,
        config: BackpressureConfig,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::auto_backpressure_block(self, config)
    }

    /// Throttle the stream
    fn throttle_rs2(self, duration: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2::throttle(self, duration)
    }

    /// Debounce the stream
    fn debounce_rs2(self, duration: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2::debounce(self, duration)
    }

    /// Sample the stream at regular intervals
    fn sample_rs2(self, interval: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
    {
        rs2::sample(self, interval)
    }

    /// Zip items from multiple streams with a function
    fn zip_with_rs2<U, F, R>(self, other: U, f: F) -> impl Stream<Item = R> + Send + 'static
    where
        U: Stream + Send + 'static,
        F: FnMut(Self::Item, U::Item) -> R + Send + 'static,
        R: Send + 'static,
        Self::Item: Send + 'static,
        U::Item: Send + 'static,
    {
        rs2::zip_with(self, other, f)
    }

    /// Group items by a key function
    fn group_by_rs2<K, F>(
        self,
        key_fn: F,
    ) -> impl Stream<Item = (K, Vec<Self::Item>)> + Send + 'static
    where
        K: Send + 'static + Clone + Eq + std::hash::Hash,
        F: Fn(&Self::Item) -> K + Send + 'static,
        Self::Item: Send + 'static + Clone,
    {
        rs2::group_by(self, key_fn)
    }

    /// Group adjacent items by a key function
    fn group_adjacent_by_rs2<K, F>(
        self,
        key_fn: F,
    ) -> impl Stream<Item = (K, Vec<Self::Item>)> + Send + 'static
    where
        K: Send + 'static + Clone + Eq,
        F: Fn(&Self::Item) -> K + Send + 'static,
        Self::Item: Send + 'static + Clone,
    {
        rs2::group_adjacent_by(self, key_fn)
    }

    /// Create a sliding window of items
    fn sliding_window_rs2(self, size: usize) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        use crate::stream::Either;
        if size == 0 {
            Either::Left(crate::stream::constructors::empty::<Vec<Self::Item>>())
        } else {
            Either::Right(rs2::sliding_window(self, size))
        }
    }

    /// Apply timeout to stream items
    fn timeout_rs2(
        self,
        duration: Duration,
    ) -> impl Stream<Item = crate::error::StreamResult<Self::Item>> + Send + 'static
    where
        Self: Sized + Send + Unpin + 'static,
        Self::Item: Send + 'static,
    {
        rs2::timeout(self, duration)
    }

    /// Apply rate limiting with backpressure
    fn rate_limit_backpressure_rs2(
        self,
        capacity: usize,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::rate_limit_backpressure(self, capacity)
    }

    /// Interleave multiple streams in a round-robin fashion
    fn interleave_rs2(mut self, mut streams: Vec<Self>) -> RoundRobinInterleave<Self>
    where
        Self: Sized + Send + Unpin + 'static,
        Self::Item: Send + 'static,
    {
        streams.insert(0, self);
        RoundRobinInterleave::new(streams)
    }

    /// Concatenate two streams
    fn concat_rs2<U>(self, other: U) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        U: Stream<Item = Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        rs2::concat(self, other)
    }

    /// Take items from either stream
    fn either_rs2<U>(self, other: U) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        U: Stream<Item = Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        rs2::either(self, other)
    }

    /// Remove consecutive duplicate items
    fn distinct_until_changed_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone + PartialEq,
    {
        rs2::distinct_until_changed(self)
    }

    /// Remove consecutive duplicate items with custom equality
    fn distinct_until_changed_by_rs2<F>(
        self,
        eq: F,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: Fn(&Self::Item, &Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static + Clone,
    {
        rs2::distinct_until_changed_by(self, eq)
    }

    /// Emit items at regular intervals
    fn tick_rs2(self, period: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
        Self: Clone,
    {
        AdvancedStreamExt::flatten(rs2::tick(period, self))
    }

    /// Prefetch items for better performance
    fn prefetch_rs2(self, prefetch_count: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::prefetch(self, prefetch_count)
    }

    /// Process items in parallel with unordered results
    fn par_eval_map_unordered_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static,
    {
        rs2::par_eval_map_unordered(self, concurrency, f)
    }

    /// Join parallel streams with concurrency control
    fn par_join_rs2(
        self,
        concurrency: usize,
    ) -> impl Stream<Item = <Self::Item as Stream>::Item> + Send + 'static
    where
        Self::Item: Stream + Send + 'static,
        <Self::Item as Stream>::Item: Send + 'static + Unpin,
    {
        rs2::par_join(self, concurrency)
    }

    /// Apply backpressure with drop oldest strategy
    fn auto_backpressure_drop_oldest_rs2(
        self,
        config: BackpressureConfig,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::auto_backpressure_drop_oldest(self, config)
    }

    /// Apply backpressure with drop newest strategy
    fn auto_backpressure_drop_newest_rs2(
        self,
        config: BackpressureConfig,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::auto_backpressure_drop_newest(self, config)
    }

    /// Apply backpressure with error strategy
    fn auto_backpressure_error_rs2(
        self,
        config: BackpressureConfig,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        rs2::auto_backpressure_error(self, config)
    }

    /// Interrupt stream when a signal is received
    fn interrupt_when_rs2<F>(self, signal: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: Stream + Send + 'static + std::future::Future<Output = ()>,
        F::Item: Send + 'static,
        Self::Item: Send + 'static,
    {
        rs2::interrupt_when(self, signal)
    }

    /// Batch process items
    fn batch_process_rs2<U, F>(
        self,
        batch_size: usize,
        processor: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Vec<Self::Item>) -> Vec<U> + Send + 'static,
        U: Send + 'static + Clone,
        Self::Item: Send + 'static,
    {
        rs2::batch_process(self, batch_size, processor)
    }

    /// Add metrics to the stream
    fn with_metrics_rs2(
        self,
        name: String,
        health_thresholds: crate::stream_performance_metrics::HealthThresholds,
    ) -> (impl Stream<Item = Self::Item> + Send, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
        Self: Unpin,
    {
        rs2::with_metrics(
            self,
            name,
            health_thresholds,
        )
    }

    /// Add metrics with custom configuration
    fn with_metrics_config_rs2(
        self,
        name: String,
        health_thresholds: crate::stream_performance_metrics::HealthThresholds,
        metrics_config: crate::stream_configuration::MetricsConfig,
    ) -> (impl Stream<Item = Self::Item> + Send + 'static, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        rs2::with_metrics_config(self, name, health_thresholds, metrics_config)
    }

    /// Bracket for resource management
    ///
    /// This combinator ensures that a resource is properly released after use.
    /// It takes three parameters:
    /// 1. A future that acquires a resource
    /// 2. A function that uses the resource and returns a stream
    /// 3. A function that releases the resource
    fn bracket_rs2<A, O, St, FAcq, FUse, FRel, R>(
        self,
        acquire: FAcq,
        use_fn: FUse,
        release: FRel,
    ) -> impl Stream<Item = O> + Send + 'static
    where
        FAcq: Future<Output = A> + Send + 'static,
        FUse: FnOnce(A) -> St + Send + 'static,
        St: Stream<Item = O> + Send + 'static,
        FRel: FnOnce(A) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
        O: Send + 'static,
        A: Clone + Send + 'static,
    {
        rs2::bracket(acquire, use_fn, release)
    }

    /// Add simple metrics to the stream
    fn with_metrics_simple_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        let (stream, _metrics) = rs2::with_metrics_simple(
            self,
            "stream".to_string(),
            crate::stream_performance_metrics::HealthThresholds::default(),
        );
        stream
    }

    /// Apply schema validation to stream items
    fn with_schema_validation_rs2<V, T>(
        self,
        validator: V,
    ) -> impl Stream<Item = T> + Send + 'static
    where
        V: crate::schema_validation::SchemaValidator + 'static,
        T: serde::de::DeserializeOwned + serde::Serialize + Send + 'static + Unpin,
        Self: Stream<Item = T> + Send + 'static,
        Self: Unpin,
    {
        let validator = std::sync::Arc::new(validator);
        self.filter_map_async_rs2(move |item| {
            let validator = validator.clone();
            async move {
                let bytes = match serde_json::to_vec(&item) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("Schema validation: failed to serialize item: {}", e);
                        return None;
                    }
                };
                match validator.validate(&bytes).await {
                    Ok(()) => Some(item),
                    Err(e) => {
                        log::warn!("Schema validation failed: {}", e);
                        None
                    }
                }
            }
        })
    }

    /// Parallel map for CPU-bound operations
    fn map_parallel_rs2<F, O>(self, f: F) -> impl Stream<Item = O> + Send + 'static
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        O: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelStreamExt;
        let concurrency = num_cpus::get();
        self.par_eval_map(concurrency, move |x| {
            let f = f.clone();
            async move { f(x) }
        })
    }

    /// Parallel map with concurrency control
    fn map_parallel_with_concurrency_rs2<F, O>(
        self,
        concurrency: usize,
        f: F,
    ) -> impl Stream<Item = O> + Send + 'static
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        O: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelStreamExt;
        self.par_eval_map(concurrency, move |x| {
            let f = f.clone();
            async move { f(x) }
        })
    }

    /// Chunk items into vectors of specified size
    fn chunk_rs2(self, size: usize) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2::chunk(self, size)
    }

    /// Sample every nth item
    fn sample_every_nth_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        use crate::stream::rate::RateStreamExt;
        self.sample_every_nth(n)
    }
}

// Blanket impl for all streams, including unsized
impl<S> RS2StreamExt for S where S: Stream + Send + 'static {}
