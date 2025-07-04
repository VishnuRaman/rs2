use std::future::Future;
use std::time::Duration;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;

use crate::stream::{
    Stream, StreamExt, AdvancedStreamExt, SpecializedStreamExt,
};
use crate::stream_performance_metrics::{HealthThresholds, StreamMetrics};
use crate::stream_configuration::{BufferConfig, GrowthStrategy};
use crate::schema_validation::SchemaValidator;
use serde::{Serialize, Deserialize};
use crate::rs2_new::{self, RS2Stream};
use crate::stream::constructors::ConstructorStreamExt;
use crate::stream::rate::RateStreamExt;



/// Extension trait providing RS2-like combinators on rs2_new Streams
/// This provides the same API surface as RS2StreamExt but for rs2_new implementation
pub trait RS2StreamExt: Stream + Sized + Send + 'static {
    /// Apply automatic backpressure with default configuration
    fn auto_backpressure_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::auto_backpressure(self, rs2_new::BackpressureConfig::default())
    }

    /// Apply automatic backpressure with custom configuration
    fn auto_backpressure_with_rs2(self, config: rs2_new::BackpressureConfig) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::auto_backpressure(self, config)
    }

    /// Apply automatic backpressure with default configuration - for cloneable items
    fn auto_backpressure_clone_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
    {
        rs2_new::auto_backpressure_with_clone(self, rs2_new::BackpressureConfig::default())
    }

    /// Apply automatic backpressure with custom configuration - for cloneable items
    fn auto_backpressure_clone_with_rs2(self, config: rs2_new::BackpressureConfig) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
    {
        rs2_new::auto_backpressure_with_clone(self, config)
    }

    /// Map elements of the stream with a function
    fn map_rs2<U, F>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> U + Send + 'static,
        U: Send + 'static,
    {
        self.map(f)
    }

    /// Transforms each element of the stream in parallel using all available CPU cores
    fn map_parallel_rs2<O, F>(self, f: F) -> impl Stream<Item = O> + Send + 'static
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static,
        O: Send + 'static + Unpin,
    {
        let concurrency = num_cpus::get();
        self.map_parallel_with_concurrency_rs2(concurrency, f)
    }

    /// Transforms each element of the stream in parallel with custom concurrency control
    fn map_parallel_with_concurrency_rs2<O, F>(self, concurrency: usize, f: F) -> impl Stream<Item = O> + Send + 'static
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static,
        O: Send + 'static + Unpin,
    {
        // Use parallel processing with proper Unpin handling
        rs2_new::par_eval_map(self, concurrency, move |item| {
            let f = f.clone();
            async move { f(item) }
        })
    }

    /// Filter elements of the stream with a predicate
    fn filter_rs2<F>(self, f: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.filter(f)
    }

    /// Flat map elements of the stream with a function that returns a stream
    fn flat_map_rs2<U, St, F>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> St + Send + 'static,
        St: Stream<Item = U> + Send + 'static,
        U: Send + 'static,
    {
        self.flat_map::<U, St, F>(f)
    }

    /// Map elements of the stream with an async function
    fn eval_map_rs2<U, Fut, F>(self, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static + Clone,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
        Self::Item: Send + 'static,
    {
        self.then(move |item| {
            let mut f = f.clone();
            async move {
                f(item).await
            }
        })
    }

    /// Merge this stream with another stream
    fn merge_rs2(self, other: RS2Stream<Self::Item>) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::merge(self, other)
    }

    /// Zip this stream with another stream
    fn zip_rs2<U>(self, other: RS2Stream<U>) -> impl Stream<Item = (Self::Item, U)> + Send + 'static
    where
        Self::Item: Send + 'static,
        U: Send + 'static,
    {
        self.zip(other)
    }

    /// Zip this stream with another stream, applying a function to each pair
    fn zip_with_rs2<U, O, F>(self, other: RS2Stream<U>, f: F) -> impl Stream<Item = O> + Send + 'static
    where
        Self::Item: Send + 'static,
        U: Send + 'static,
        O: Send + 'static,
        F: FnMut(Self::Item, U) -> O + Send + 'static,
    {
        rs2_new::zip_with(self, other, f)
    }

    /// Throttle this stream to emit at most one element per duration
    fn throttle_rs2(self, duration: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::throttle(self, duration)
    }

    /// Debounce this stream, only emitting an element after a specified quiet period
    fn debounce_rs2(self, duration: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::debounce(self, duration)
    }

    /// Sample this stream at regular intervals, emitting the most recent value
    fn sample_rs2(self, interval: Duration) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
    {
        rs2_new::sample(self, interval)
    }

    /// Process elements in parallel with bounded concurrency, preserving order
    fn par_eval_map_rs2<U, Fut, F>(self, concurrency: usize, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static,
    {
        rs2_new::par_eval_map(self, concurrency, f)
    }

    /// Add timeout to stream operations
    fn timeout_rs2(self, duration: Duration) -> impl Stream<Item = crate::error::StreamResult<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::timeout(self, duration)
    }

    /// Filter out consecutive duplicate elements from this stream
    fn distinct_until_changed_rs2(self) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Clone + Send + PartialEq + 'static,
    {
        rs2_new::distinct_until_changed(self)
    }

    /// Interrupt this stream when a signal is received
    fn interrupt_when_rs2<F>(self, signal: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        rs2_new::interrupt_when(self, signal)
    }

    /// Take elements from this stream while a predicate returns true
    fn take_while_rs2<F>(self, predicate: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        rs2_new::take_while(self, predicate)
    }

    /// Skip elements from this stream while a predicate returns true
    fn drop_while_rs2<F>(self, predicate: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        rs2_new::drop_while(self, predicate)
    }

    /// Group consecutive elements that share a common key
    fn group_adjacent_by_rs2<K, F>(self, key_fn: F) -> impl Stream<Item = (K, Vec<Self::Item>)> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
        K: Eq + Clone + Send + 'static,
        F: FnMut(&Self::Item) -> K + Send + 'static,
    {
        rs2_new::group_adjacent_by(self, key_fn)
    }

    /// Group consecutive elements that share a common key
    fn group_by_rs2<K, F>(self, key_fn: F) -> impl Stream<Item = (K, Vec<Self::Item>)> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
        K: Eq + Clone + Send + 'static,
        F: FnMut(&Self::Item) -> K + Send + 'static,
    {
        rs2_new::group_by(self, key_fn)
    }

    /// Fold operation that accumulates a value over a stream
    fn fold_rs2<A, F, Fut>(self, init: A, f: F) -> impl Future<Output = A>
    where
        F: FnMut(A, Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = A> + Send + 'static,
        Self::Item: Send + 'static,
        A: Clone + Send + 'static,
    {
        rs2_new::fold(self, init, f)
    }

    /// Scan operation that applies a function to each element and emits intermediate values
    fn scan_rs2<U, F>(self, init: U, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(&mut U, Self::Item) -> Option<U> + Send + 'static,
        Self::Item: Send + 'static,
        U: Clone + Send + 'static,
    {
        rs2_new::scan(self, init, f)
    }

    /// Apply a function to each element in the stream
    fn for_each_rs2<F, Fut>(self, f: F) -> impl Future<Output = ()> + Send + 'static
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.for_each(f)
    }

    /// Take the first n elements from the stream
    fn take_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::take(self, n)
    }

    /// Drop the first n elements from the stream
    fn drop_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::drop(self, n)
    }

    /// Skip the first n elements from the stream
    fn skip_rs2(self, n: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::drop(self, n)
    }

    /// Select between this stream and another stream based on which produces a value first
    fn either_rs2(self, other: RS2Stream<Self::Item>) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::either(self, other)
    }

    /// Collect all items from the stream into a collection
    fn collect_rs2<B>(self) -> impl Future<Output = B> + Send + 'static
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.collect()
    }

    /// Collect all items from the stream into a collection with custom buffer configuration
    fn collect_with_config_rs2<B>(self, config: BufferConfig) -> impl Future<Output = B> + Send + 'static
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        // Use config settings for backpressure and limits
        let initial_capacity = config.initial_capacity;
        let max_capacity = config.max_capacity.unwrap_or(usize::MAX);
        
        // Apply backpressure based on initial capacity
        let stream = self.backpressure(initial_capacity);
        
        // Take at most max_capacity items
        let limited_stream = if max_capacity == usize::MAX {
            stream.take(max_capacity)
        } else {
            stream.take(max_capacity)
        };
        
        // Collect into the target collection
        limited_stream.collect()
    }

    /// Create a sliding window of elements from the stream
    fn sliding_window_rs2(self, size: usize) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
    {
        rs2_new::sliding_window(self, size)
    }

    /// Process items in batches for better throughput
    fn batch_process_rs2<U, F>(self, batch_size: usize, processor: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Vec<Self::Item>) -> Vec<U> + Send + 'static,
        Self::Item: Send + 'static,
        U: Clone + Send + 'static,
    {
        rs2_new::batch_process(self, batch_size, processor)
    }

    /// Collect metrics while processing the stream
    fn with_metrics_rs2(
        self,
        name: String,
        thresholds: HealthThresholds,
    ) -> (impl Stream<Item = Self::Item> + Send + 'static, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        rs2_new::with_metrics(self, name, thresholds)
    }

    /// Collect metrics while processing the stream with custom config
    fn with_metrics_config_rs2(
        self,
        name: String,
        thresholds: HealthThresholds,
        metrics_config: crate::stream_configuration::MetricsConfig,
    ) -> (impl Stream<Item = Self::Item> + Send + 'static, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        rs2_new::with_metrics_config(self, name, thresholds, metrics_config)
    }

    /// Interleave this stream with other streams in a round-robin fashion
    fn interleave_rs2(self, other: RS2Stream<Self::Item>) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::interleave(self, other)
    }

    /// Chunk the stream into vectors of the specified size
    fn chunk_rs2(self, size: usize) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::chunk(self, size)
    }



    /// Process elements in parallel with bounded concurrency, unordered output
    fn par_eval_map_unordered_rs2<U, Fut, F>(self, concurrency: usize, f: F) -> impl Stream<Item = U> + Send + 'static
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static,
    {
        rs2_new::par_eval_map_unordered(self, concurrency, f)
    }

    /// Join multiple streams in parallel with bounded concurrency
    fn par_join_rs2<S, O>(self, concurrency: usize) -> impl Stream<Item = O> + Send + 'static
    where
        Self::Item: Stream<Item = O> + Send + 'static,
        S: Stream<Item = O> + Send + 'static,
        O: Send + 'static,
    {
        rs2_new::par_join(self, concurrency)
    }

    /// Prefetch items for better performance
    fn prefetch_rs2(self, prefetch_count: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::prefetch(self, prefetch_count)
    }

    /// Filter out consecutive duplicate elements with custom equality function
    fn distinct_until_changed_by_rs2<F>(self, eq: F) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
        F: FnMut(&Self::Item, &Self::Item) -> bool + Send + 'static,
    {
        rs2_new::distinct_until_changed_by(self, eq)
    }

    /// Rate limiting with backpressure
    fn rate_limit_backpressure_rs2(self, capacity: usize) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static,
    {
        rs2_new::rate_limit_backpressure(self, capacity)
    }

    /// Schema validation for stream items
    fn with_schema_validation_rs2<V, T>(
        self,
        validator: V,
    ) -> impl Stream<Item = crate::error::StreamResult<T>> + Send + 'static
    where
        V: SchemaValidator + 'static,
        T: serde::de::DeserializeOwned + Serialize + Send + 'static,
        Self: Stream<Item = T> + Send + 'static,
    {
        let validator = Arc::new(validator);
        self.then(move |item| {
            let validator = validator.clone();
            async move {
                let bytes = match serde_json::to_vec(&item) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("Schema validation: failed to serialize item: {}", e);
                        return Err(crate::error::StreamError::ValidationError(format!("Serialization error: {}", e)));
                    }
                };
                match validator.validate(&bytes).await {
                    Ok(()) => Ok(item),
                    Err(e) => {
                        log::warn!("Schema validation failed: {}", e);
                        Err(crate::error::StreamError::ValidationError(format!("Validation error: {}", e)))
                    }
                }
            }
        })
    }
}

impl<S> RS2StreamExt for S where S: Stream + Sized + Send + 'static {}

/// Static methods for RS2NewStreamExt
pub struct RS2Static;

impl RS2Static {
    /// Create a stream that emits values at a fixed rate
    pub fn tick_rs2<O>(period: Duration, item: O) -> RS2Stream<O>
    where
        O: Clone + Send + 'static,
    {
        Box::new(rs2_new::tick(period, item))
    }

    /// Bracket for resource management
    pub fn bracket_rs2<A, O, St, FAcq, FUse, FRel, R>(
        acquire: FAcq,
        use_fn: FUse,
        release: FRel,
    ) -> RS2Stream<O>
    where
        FAcq: Future<Output = A> + Send + 'static,
        FUse: FnOnce(A) -> St + Send + 'static,
        St: Stream<Item = O> + Send + 'static,
        FRel: FnOnce(A) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
        O: Send + 'static,
        A: Clone + Send + 'static,
    {
        Box::new(rs2_new::bracket(acquire, use_fn, release))
    }
} 