//! Extension traits for streams

use crate::rs2::{self, BackpressureConfig};
use crate::stream::constructors::from_iter;
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

    /// Collect all items from the stream into a collection using session buffer configuration
    ///
    /// This combinator collects all items from the stream into a collection of type B.
    /// It automatically uses the buffer configuration from the global session.
    async fn collect_with_session_rs2<B>(self) -> B
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
        Self: Unpin,
    {
        use crate::session::get_global_buffer_config;
        let config = get_global_buffer_config().unwrap_or_default();
        self.collect_with_config_rs2(config).await
    }

    /// Fold over the stream
    async fn fold_rs2<B, F>(self, init: B, f: F) -> B
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

    /// Growing then sliding window - starts with small windows and grows to full size
    fn growing_then_sliding_window_rs2(
        self,
        size: usize,
    ) -> impl Stream<Item = Vec<Self::Item>> + Send + 'static
    where
        Self: Sized + 'static,
        Self::Item: Send + 'static + Clone,
    {
        use crate::stream::Either;
        if size == 0 {
            Either::Left(crate::stream::constructors::empty::<Vec<Self::Item>>())
        } else {
            Either::Right(growing_then_sliding_window(self, size))
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
        Self: Unpin,
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
        concurrency: Option<usize>,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParEvalMap;
        use crate::session::get_global_parallel_config;
        
        // Try to get concurrency from session config, fall back to explicit parameter
        let effective_concurrency = concurrency.unwrap_or_else(|| {
            get_global_parallel_config()
                .map(|config| config.concurrency)
                .unwrap_or(4) // Default fallback
        });
        
        let config = crate::stream::parallel::ParallelConfig {
            concurrency: effective_concurrency,
            max_buffer_size: get_global_parallel_config()
                .map(|config| config.max_buffer_size)
                .unwrap_or(1024),
            timeout: get_global_parallel_config()
                .map(|config| config.timeout)
                .unwrap_or(std::time::Duration::MAX),
            sequence_timeout: get_global_parallel_config()
                .map(|config| config.sequence_timeout)
                .unwrap_or(std::time::Duration::MAX),
            task_timeout: get_global_parallel_config()
                .map(|config| config.task_timeout)
                .unwrap_or(std::time::Duration::MAX),
        };
        
        ParEvalMap::with_config(self, f, config)
    }

    /// Parallel map with explicit concurrency - Preserves order
    /// Backward compatibility method
    fn par_eval_map_with_concurrency_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        self.par_eval_map_rs2(Some(concurrency), f)
    }

    /// Parallel map using session configuration - Preserves order
    fn par_eval_map_with_session_rs2<F, Fut, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        self.par_eval_map_rs2(None, f)
    }

    /// Parallel map with concurrency control - Does not preserve order
    fn par_eval_map_unordered_rs2<F, Fut, U>(
        self,
        concurrency: Option<usize>,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParEvalMapUnordered;
        use crate::session::get_global_parallel_config;
        
        // Try to get concurrency from session config, fall back to explicit parameter
        let effective_concurrency = concurrency.unwrap_or_else(|| {
            get_global_parallel_config()
                .map(|config| config.concurrency)
                .unwrap_or(4) // Default fallback
        });
        
        let config = crate::stream::parallel::ParallelConfig {
            concurrency: effective_concurrency,
            max_buffer_size: get_global_parallel_config()
                .map(|config| config.max_buffer_size)
                .unwrap_or(1024),
            timeout: get_global_parallel_config()
                .map(|config| config.timeout)
                .unwrap_or(std::time::Duration::MAX),
            sequence_timeout: get_global_parallel_config()
                .map(|config| config.sequence_timeout)
                .unwrap_or(std::time::Duration::MAX),
            task_timeout: get_global_parallel_config()
                .map(|config| config.task_timeout)
                .unwrap_or(std::time::Duration::MAX),
        };
        
        ParEvalMapUnordered::with_config(self, f, config)
    }

    /// Parallel map unordered with explicit concurrency - Does not preserve order
    fn par_eval_map_unordered_with_concurrency_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        self.par_eval_map_unordered_rs2(Some(concurrency), f)
    }

    /// Parallel map unordered using session configuration - Does not preserve order
    fn par_eval_map_unordered_with_session_rs2<F, Fut, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        self.par_eval_map_unordered_rs2(None, f)
    }

    /// Parallel map using session configuration
    fn map_parallel_with_session_rs2<F, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> U + Send + Sync + Clone + 'static + Unpin,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        let concurrency = crate::session::get_global_parallel_config()
            .map(|config| config.concurrency)
            .unwrap_or(4);
        self.map_parallel_rs2(Some(concurrency), f)
    }

    /// Parallel join using session configuration
    fn par_join_with_session_rs2(self) -> impl Stream<Item = <Self::Item as Stream>::Item> + Send + 'static
    where
        Self: Send + 'static + Unpin,
        Self::Item: Stream + Send + 'static + Unpin,
        <Self::Item as Stream>::Item: Send + 'static + Unpin + Clone,
    {
        let concurrency = crate::session::get_global_parallel_config()
            .map(|config| config.concurrency)
            .unwrap_or(4);
        self.par_join_rs2(concurrency)
    }

    /// Parallel map optimized for small workloads (4 workers, 256 buffer)
    fn par_eval_map_small_workloads_rs2<F, Fut, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelConfig;
        let config = ParallelConfig::for_small_workloads();
        self.par_eval_map_with_config_rs2(config, f)
    }

    /// Parallel map optimized for large workloads (CPU cores, 4096 buffer)
    fn par_eval_map_large_workloads_rs2<F, Fut, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelConfig;
        let config = ParallelConfig::for_large_workloads();
        self.par_eval_map_with_config_rs2(config, f)
    }

    /// Parallel map with adaptive configuration based on expected workload
    fn par_eval_map_adaptive_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        expected_items: usize,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelConfig;
        let config = ParallelConfig::adaptive(concurrency, expected_items);
        self.par_eval_map_with_config_rs2(config, f)
    }

    /// Parallel map with task timeout (drops items that take too long)
    fn par_eval_map_with_timeout_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        task_timeout: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelConfig;
        let mut config = ParallelConfig::default();
        config.concurrency = concurrency;
        config.task_timeout = task_timeout;
        self.par_eval_map_with_config_rs2(config, f)
    }

    /// Parallel map with sequence timeout (skips missing items in ordered mode)
    fn par_eval_map_with_sequence_timeout_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        sequence_timeout: std::time::Duration,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelConfig;
        let mut config = ParallelConfig::default();
        config.concurrency = concurrency;
        config.sequence_timeout = sequence_timeout;
        self.par_eval_map_with_config_rs2(config, f)
    }

    /// Parallel map with custom buffer size
    fn par_eval_map_with_buffer_size_rs2<F, Fut, U>(
        self,
        concurrency: usize,
        max_buffer_size: usize,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParallelConfig;
        let mut config = ParallelConfig::default();
        config.concurrency = concurrency;
        config.max_buffer_size = max_buffer_size;
        self.par_eval_map_with_config_rs2(config, f)
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

    /// Apply backpressure using session configuration
    fn auto_backpressure_with_session_rs2(
        self,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        use crate::session::get_global_backpressure_config;
        let config = get_global_backpressure_config().unwrap_or_default();
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
    fn interleave_rs2(self, mut streams: Vec<Self>) -> RoundRobinInterleave<Self>
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



    /// Join parallel streams with concurrency control
    fn par_join_rs2(
        self,
        concurrency: usize,
    ) -> impl Stream<Item = <Self::Item as Stream>::Item> + Send + 'static
    where
        Self::Item: Stream + Send + 'static + Unpin,
        <Self::Item as Stream>::Item: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
    {
        // Use par_eval_map_rs2 to process streams with the given concurrency
        self.par_eval_map_rs2(Some(concurrency), |stream| async move {
            // Collect all items from the stream
            let items: Vec<_> = stream.collect().await;
            items
        })
        .flat_map_rs2(|items| from_iter(items))
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

    /// Apply backpressure with drop oldest strategy using session configuration
    fn auto_backpressure_drop_oldest_with_session_rs2(
        self,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        use crate::session::get_global_backpressure_config;
        let config = get_global_backpressure_config().unwrap_or_default();
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

    /// Apply backpressure with drop newest strategy using session configuration
    fn auto_backpressure_drop_newest_with_session_rs2(
        self,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        use crate::session::get_global_backpressure_config;
        let config = get_global_backpressure_config().unwrap_or_default();
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

    /// Apply backpressure with error strategy using session configuration
    fn auto_backpressure_error_with_session_rs2(
        self,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        use crate::session::get_global_backpressure_config;
        let config = get_global_backpressure_config().unwrap_or_default();
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

    /// Add metrics using session configuration
    fn with_metrics_with_session_rs2(
        self,
        name: String,
        health_thresholds: crate::stream_performance_metrics::HealthThresholds,
    ) -> (impl Stream<Item = Self::Item> + Send + 'static, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        use crate::session::get_global_metrics_config;
        let metrics_config = get_global_metrics_config().unwrap_or_default();
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
    fn map_parallel_rs2<F, U>(
        self,
        concurrency: Option<usize>,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> U + Send + Sync + Clone + 'static + Unpin,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParMap;
        use crate::session::get_global_parallel_config;
        
        // Try to get concurrency from session config, fall back to explicit parameter
        let effective_concurrency = concurrency.unwrap_or_else(|| {
            get_global_parallel_config()
                .map(|config| config.concurrency)
                .unwrap_or(4) // Default fallback
        });
        
        let config = crate::stream::parallel::ParallelConfig {
            concurrency: effective_concurrency,
            max_buffer_size: get_global_parallel_config()
                .map(|config| config.max_buffer_size)
                .unwrap_or(1024),
            timeout: get_global_parallel_config()
                .map(|config| config.timeout)
                .unwrap_or(std::time::Duration::MAX),
            sequence_timeout: get_global_parallel_config()
                .map(|config| config.sequence_timeout)
                .unwrap_or(std::time::Duration::MAX),
            task_timeout: get_global_parallel_config()
                .map(|config| config.task_timeout)
                .unwrap_or(std::time::Duration::MAX),
        };
        
        ParMap::with_config(self, f, config)
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
        O: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
    {
        
        self.par_eval_map_rs2(Some(concurrency), move |x| {
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

    // ================================
    // Advanced Parallel Operations with Configuration
    // ================================

    /// Parallel map with full configuration control
    fn par_eval_map_with_config_rs2<F, Fut, U>(
        self,
        config: crate::stream::parallel::ParallelConfig,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParEvalMap;
        ParEvalMap::with_config(self, f, config)
    }

    /// Parallel map using session configuration
    fn par_eval_map_with_session_config_rs2<F, Fut, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::session::get_global_parallel_config;
        let config = get_global_parallel_config().unwrap_or_default();
        self.par_eval_map_with_config_rs2(config, f)
    }

    /// Parallel map unordered with full configuration control
    fn par_eval_map_unordered_with_config_rs2<F, Fut, U>(
        self,
        config: crate::stream::parallel::ParallelConfig,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::stream::parallel::ParEvalMapUnordered;
        ParEvalMapUnordered::with_config(self, f, config)
    }

    /// Parallel map unordered using session configuration
    fn par_eval_map_unordered_with_session_config_rs2<F, Fut, U>(
        self,
        f: F,
    ) -> impl Stream<Item = U> + Send + 'static
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin + Clone,
        Self: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
    {
        use crate::session::get_global_parallel_config;
        let config = get_global_parallel_config().unwrap_or_default();
        self.par_eval_map_unordered_with_config_rs2(config, f)
    }
}

/// Growing then sliding window implementation
/// Starts with small windows and grows to full size, then slides
pub fn growing_then_sliding_window<S, T>(
    stream: S,
    size: usize,
) -> impl Stream<Item = Vec<T>> + Send + 'static
where
    S: Stream<Item = T> + Send + 'static,
    T: Clone + Send + 'static,
{
    
    
    stream.scan(Vec::<T>::new(), move |window, item| {
        window.push(item);
        if window.len() > size {
            window.remove(0);
        }
        Some(window.clone())
    })
}

// Blanket impl for all streams, including unsized
impl<S> RS2StreamExt for S where S: Stream + Send + 'static {}
