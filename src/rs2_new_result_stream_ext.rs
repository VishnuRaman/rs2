use std::future::Future;
use std::time::Duration;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;

use crate::stream::{
    Stream, StreamExt, AdvancedStreamExt, SpecializedStreamExt, SelectStreamExt,
    empty, once, repeat, from_iter, pending, repeat_with, once_with, unfold,
    UtilityStreamExt
};
use crate::stream::rate::RateStreamExt;
use crate::error::{StreamResult, RetryPolicy};
use crate::stream_performance_metrics::{HealthThresholds, StreamMetrics};
use crate::stream_configuration::BufferConfig;
use crate::rs2_new::{self, RS2Stream, BackpressureConfig};

/// Extension trait for streams that produce `Result` values
/// This provides the same API surface as RS2ResultStreamExt but for rs2_new implementation
pub trait RS2ResultStreamExt<T, E>: Stream<Item = Result<T, E>> + Sized + Send + 'static 
where
    T: Unpin,
    E: Unpin + From<crate::error::StreamError>,
{
    /// Filter and unwrap successful results, dropping errors
    fn successes_rs2(self) -> RS2Stream<T>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Box::new(StreamExt::filter_map(self, |result| result.ok()))
    }

    /// Filter and unwrap errors, dropping successful results
    fn errors_rs2(self) -> RS2Stream<E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Box::new(StreamExt::filter_map(self, |result| result.err()))
    }

    /// Map successful results with a function, preserving errors
    fn map_successes_rs2<U, F>(self, mut f: F) -> RS2Stream<Result<U, E>>
    where
        F: FnMut(T) -> U + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        U: Send + 'static,
    {
        Box::new(self.map(move |result| result.map(|v| f(v))))
    }

    /// Map errors with a function, preserving successful results
    fn map_errors_rs2<F, E2>(self, mut f: F) -> RS2Stream<Result<T, E2>>
    where
        F: FnMut(E) -> E2 + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        E2: Send + 'static,
    {
        Box::new(self.map(move |result| result.map_err(|e| f(e))))
    }

    /// Transform successful results with an async function
    fn eval_map_successes_rs2<U, F, Fut>(self, f: F) -> RS2Stream<Result<U, E>>
    where
        F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = U> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        U: Send + 'static,
    {
        Box::new(self.then(move |result| {
            let f = f.clone();
            async move {
                match result {
                    Ok(value) => Ok(f(value).await),
                    Err(err) => Err(err),
                }
            }
        }))
    }

    /// Transform successful results with an async function that can fail
    fn eval_map_try_rs2<U, F, Fut, E2>(self, f: F) -> RS2Stream<Result<U, E2>>
    where
        F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<U, E2>> + Send + 'static,
        T: Send + 'static,
        E: Into<E2> + Send + 'static,
        E2: Send + 'static,
        U: Send + 'static,
    {
        Box::new(self.then(move |result| {
            let f = f.clone();
            async move {
                match result {
                    Ok(value) => f(value).await,
                    Err(err) => Err(err.into()),
                }
            }
        }))
    }

    /// Apply a fallback function for errors
    fn or_else_rs2<F, Fut>(self, f: F) -> RS2Stream<Result<T, E>>
    where
        F: Fn(E) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Box::new(self.then(move |result| {
            let f = f.clone();
            async move {
                match result {
                    Ok(value) => Ok(value),
                    Err(err) => Ok(f(err).await),
                }
            }
        }))
    }

    /// Filter results, keeping only those that match a predicate
    fn filter_successes_rs2<F>(self, mut f: F) -> RS2Stream<Result<T, E>>
    where
        F: FnMut(&T) -> bool + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        Box::new(self.filter(move |result| {
            match result {
                Ok(value) => f(value),
                Err(_) => true, // Keep errors
            }
        }))
    }

    /// Retry the stream on first error up to `times`
    fn retry_rs2<F>(self, max_retries: usize, retry_fn: F) -> RS2Stream<Result<T, E>>
    where
        F: FnMut() -> Self + Send + Clone + 'static,
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        use crate::rs2_new::unfold_rs2;
        
        Box::new(unfold_rs2((Some(self), 0, max_retries, retry_fn), move |(current_stream, attempts, max, mut factory)| async move {
            if let Some(stream) = current_stream {
                let mut stream = Box::pin(stream);
                
                // Try to get an item from current stream
                use std::future::poll_fn;
                if let Some(result) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                    match result {
                        Ok(value) => Some((Ok(value), (None, attempts, max, factory))),
                        Err(err) => {
                            // We got an error - should we retry?
                            if attempts < max {
                                // Create new stream and continue
                                let new_stream = factory();
                                Some((Err(err), (Some(new_stream), attempts + 1, max, factory)))
                            } else {
                                // No more retries - return error
                                Some((Err(err), (None, attempts, max, factory)))
                            }
                        }
                    }
                } else {
                    // Stream ended without producing anything
                    None
                }
            } else {
                None
            }
        }))
    }

    /// Retry with policy - Full implementation
    fn retry_with_policy_rs2<F>(
        self,
        policy: crate::error::RetryPolicy,
        factory: F,
    ) -> RS2Stream<Result<T, E>>
    where
        F: FnMut() -> Self + Send + Clone + 'static,
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        use crate::error::RetryPolicy;
        use std::time::Duration;
        
        // Extract retry parameters from policy
        let (max_retries, delay_fn) = match policy {
            RetryPolicy::None => return Box::new(self),
            RetryPolicy::Immediate { max_retries } => {
                (max_retries, Box::new(|_attempt: usize| Duration::ZERO) as Box<dyn Fn(usize) -> Duration + Send + Sync>)
            }
            RetryPolicy::Fixed { max_retries, delay } => {
                (max_retries, Box::new(move |_attempt: usize| delay) as Box<dyn Fn(usize) -> Duration + Send + Sync>)
            }
            RetryPolicy::Exponential { max_retries, initial_delay, multiplier } => {
                let multiplier = multiplier;
                let max_delay = Duration::from_secs(60); // Default max delay
                (max_retries, Box::new(move |attempt: usize| {
                    let delay = initial_delay.mul_f64(multiplier.powi(attempt as i32));
                    delay.min(max_delay)
                }) as Box<dyn Fn(usize) -> Duration + Send + Sync>)
            }
        };

        Box::new(crate::rs2_new::unfold_rs2((Some(self), 0, max_retries, factory, delay_fn), move |(current_stream, attempts, max, mut factory, delay_fn)| async move {
            if let Some(stream) = current_stream {
                let mut stream = Box::pin(stream);
                
                // Try to get an item from current stream
                use std::future::poll_fn;
                if let Some(result) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                    match result {
                        Ok(value) => Some((Ok(value), (None, attempts, max, factory, delay_fn))),
                        Err(err) => {
                            // We got an error - should we retry?
                            if attempts < max {
                                // Wait for the calculated delay
                                let delay = delay_fn(attempts);
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                                
                                // Create new stream and continue
                                let new_stream = factory();
                                Some((Err(err), (Some(new_stream), attempts + 1, max, factory, delay_fn)))
                            } else {
                                // No more retries - return error
                                Some((Err(err), (None, attempts, max, factory, delay_fn)))
                            }
                        }
                    }
                } else {
                    // Stream ended without producing anything
                    None
                }
            } else {
                None
            }
        }))
    }

    /// Collect all successful results, stopping at the first error
    fn collect_successes_rs2<B>(self) -> impl Future<Output = Result<B, E>>
    where
        B: Default + Extend<T> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        async move {
            let mut collection = B::default();
            let mut stream = Box::pin(self);
            
            use std::task::{Context, Poll};
            use std::future::poll_fn;
            
            loop {
                let result = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
                match result {
                    Some(Ok(value)) => collection.extend(std::iter::once(value)),
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
            Ok(collection)
        }
    }

    /// Fold over successful results, stopping at the first error
    fn fold_successes_rs2<A, F, Fut>(self, init: A, mut f: F) -> impl Future<Output = Result<A, E>>
    where
        F: FnMut(A, T) -> Fut + Send + 'static,
        Fut: Future<Output = A> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        A: Send + 'static,
    {
        async move {
            let mut acc = init;
            let mut stream = Box::pin(self);
            
            use std::task::{Context, Poll};
            use std::future::poll_fn;
            
            loop {
                let result = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
                match result {
                    Some(Ok(value)) => acc = f(acc, value).await,
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
            Ok(acc)
        }
    }

    /// Apply automatic backpressure with default configuration
    fn auto_backpressure_rs2(self) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::auto_backpressure(self, crate::rs2_new::BackpressureConfig::default())
    }

    /// Apply automatic backpressure with custom configuration
    fn auto_backpressure_with_rs2(self, config: crate::rs2_new::BackpressureConfig) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::auto_backpressure(self, config)
    }

    /// Apply automatic backpressure with default configuration - for cloneable items
    fn auto_backpressure_clone_rs2(self) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        rs2_new::auto_backpressure_with_clone(self, crate::rs2_new::BackpressureConfig::default())
    }

    /// Apply automatic backpressure with custom configuration - for cloneable items
    fn auto_backpressure_clone_with_rs2(self, config: crate::rs2_new::BackpressureConfig) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        rs2_new::auto_backpressure_with_clone(self, config)
    }

    /// Throttle result streams
    fn throttle_rs2(self, duration: Duration) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::throttle(self, duration)
    }

    /// Debounce result streams
    fn debounce_rs2(self, duration: Duration) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::debounce(self, duration)
    }

    /// Sample result streams at regular intervals
    fn sample_rs2(self, interval: Duration) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        rs2_new::sample(self, interval)
    }

    /// Merge with another result stream
    fn merge_rs2(self, other: RS2Stream<Result<T, E>>) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::merge(self, other)
    }

    /// Take the first n results
    fn take_rs2(self, n: usize) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::take(self, n)
    }

    /// Drop the first n results
    fn drop_rs2(self, n: usize) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::drop(self, n)
    }

    /// Skip the first n results (alias for drop)
    fn skip_rs2(self, n: usize) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::drop(self, n)
    }

    /// Timeout result stream operations
    fn timeout_rs2(self, duration: Duration) -> impl Stream<Item = StreamResult<Result<T, E>>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::timeout(self, duration)
    }

    /// Chunk result streams
    fn chunk_rs2(self, size: usize) -> impl Stream<Item = Vec<Result<T, E>>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::chunk(self, size)
    }

    /// Collect metrics while processing the stream
    fn with_metrics_rs2(
        self,
        name: String,
        health_thresholds: HealthThresholds,
    ) -> (impl Stream<Item = Self::Item> + Send + 'static, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        let (stream, metrics) = rs2_new::with_metrics_simple(self, name, health_thresholds);
        (stream, metrics)
    }

    /// Collect metrics while processing the stream with custom config
    fn with_metrics_config_rs2(
        self,
        name: String,
        health_thresholds: HealthThresholds,
        metrics_config: crate::stream_configuration::MetricsConfig,
    ) -> (impl Stream<Item = Self::Item> + Send + 'static, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        let (stream, metrics) = rs2_new::with_metrics_config(self, name, health_thresholds, metrics_config);
        (stream, metrics)
    }

    /// Interrupt when a signal is received
    fn interrupt_when_rs2<F>(self, signal: F) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        rs2_new::interrupt_when(self, signal)
    }

    /// Process results in parallel with bounded concurrency
    fn par_eval_map_rs2<U, F, Fut>(self, concurrency: usize, f: F) -> impl Stream<Item = Result<U, E>> + Send + 'static
    where
        F: FnMut(Result<T, E>) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = Result<U, E>> + Send + 'static,
        T: Send + 'static + Unpin,
        E: Send + 'static + Unpin,
        U: Send + 'static + Unpin,
    {
        rs2_new::par_eval_map(self, concurrency, f)
    }

    /// Convert result stream to either stream
    fn either_rs2(self, other: RS2Stream<Result<T, E>>) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rs2_new::either(self, other)
    }


}

// Implement the trait for all Result streams
impl<S, T, E> RS2ResultStreamExt<T, E> for S 
where 
    S: Stream<Item = Result<T, E>> + Sized + Send + 'static,
    T: Unpin,
    E: Unpin + From<crate::error::StreamError>,
{} 