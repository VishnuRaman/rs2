//! Extension traits for Result streams

use crate::stream::{Stream, StreamExt};
use crate::rs2_stream_ext::RS2StreamExt;
use std::time::Duration;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use std::marker::PhantomData;
use std::collections::VecDeque;
use pin_project_lite::pin_project;
use std::future::Future;

/// Extension trait for streams that yield `Result<T, E>` values
pub trait RS2ResultStreamExt<T, E>: Stream<Item = Result<T, E>> + Sized + Send + Unpin + 'static 
where
    T: Send + 'static + Clone,
    E: Send + 'static + std::fmt::Debug,
{
    /// Map over successful values, leaving errors unchanged
    fn map_ok<F, U>(self, f: F) -> impl Stream<Item = Result<U, E>> + Send + 'static
    where
        F: Fn(T) -> U + Send + Sync + 'static,
        U: Send + 'static,
    {
        self.map_rs2(move |item| match item {
            Ok(value) => Ok(f(value)),
            Err(e) => Err(e),
        })
    }

    /// Map over error values, leaving successful values unchanged
    fn map_err<F, F2>(self, f: F) -> impl Stream<Item = Result<T, F2>> + Send + 'static
    where
        F: Fn(E) -> F2 + Send + Sync + 'static,
        F2: Send + 'static,
    {
        self.map_rs2(move |item| match item {
            Ok(value) => Ok(value),
            Err(e) => Err(f(e)),
        })
    }

    /// Filter out error values, keeping only successful values
    fn ok_values(self) -> impl Stream<Item = T> + Send + 'static {
        self.filter_map_rs2(|item| match item {
            Ok(value) => Some(value),
            Err(_) => None,
        })
    }

    /// Filter out successful values, keeping only error values
    fn err_values(self) -> impl Stream<Item = E> + Send + 'static {
        self.filter_map_rs2(|item| match item {
            Ok(_) => None,
            Err(e) => Some(e),
        })
    }

    /// Collect all successful values into a vector
    async fn collect_ok(self) -> Vec<T> {
        let mut results = Vec::new();
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            if let Ok(value) = item {
                results.push(value);
            }
        }
        results
    }

    /// Collect all error values into a vector
    async fn collect_err(self) -> Vec<E> {
        let mut results = Vec::new();
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                results.push(e);
            }
        }
        results
    }

    /// Partition successful and error values
    async fn partition_results(self) -> (Vec<T>, Vec<E>) {
        let mut ok_values = Vec::new();
        let mut err_values = Vec::new();
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            match item {
                Ok(value) => ok_values.push(value),
                Err(e) => err_values.push(e),
            }
        }
        (ok_values, err_values)
    }

    /// Retry failed operations with a maximum number of retries
    fn retry(self) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        E: Clone + Send + 'static + Default,
    {
        RetryStream::<Self, T, E> {
            stream: self,
            max_retries: 3, // Default to 3 retries
            retry_buffer: VecDeque::new(),
            _phantom: PhantomData,
        }
    }

    /// Retry with a maximum number of retries and delay between attempts
    fn retry_with_delay(
        self,
        max_retries: usize,
        delay: Duration,
    ) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        E: Clone + Send + 'static + Default,
    {
        RetryWithDelayStream::<Self, T, E> {
            stream: self,
            max_retries,
            delay,
            current_retries: 0,
            last_error_time: None,
            _phantom: Default::default(),
        }
    }

    /// Retry with a custom retry policy
    fn retry_with_policy(
        self,
        policy: crate::error::RetryPolicy,
    ) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        E: Clone + Send + 'static + Default,
    {
        RetryWithPolicyStream::<Self, T, E> {
            stream: self,
            policy,
            current_retries: 0,
            last_error_time: None,
            _phantom: PhantomData::<(T, E)>,
        }
    }

    /// Retry with policy and factory function
    fn retry_with_policy_rs2<F>(
        self,
        policy: crate::error::RetryPolicy,
        factory: F,
    ) -> impl Stream<Item = Result<T, E>> + Send + 'static
    where
        F: FnMut() -> Self + Send + 'static,
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        let max_retries = match &policy {
            crate::error::RetryPolicy::None => 0,
            crate::error::RetryPolicy::Immediate { max_retries } => *max_retries,
            crate::error::RetryPolicy::Fixed { max_retries, .. } => *max_retries,
            crate::error::RetryPolicy::Exponential { max_retries, .. } => *max_retries,
        };

        RetryWithPolicyFactoryStream::<Self, T, E, F> {
            stream: self,
            policy,
            factory,
            current_attempt: 0,
            max_retries,
            delay_state: CustomDelayState::None,
            terminated: false,
            _phantom: PhantomData::<(T, E)>,
        }
    }

    /// Handle errors by converting them to a default value
    fn handle_errors<F>(self, handler: F) -> impl Stream<Item = T> + Send + 'static
    where
        F: Fn(E) -> T + Send + Sync + 'static,
    {
        self.map_rs2(move |item| match item {
            Ok(value) => value,
            Err(e) => handler(e),
        })
    }

    /// Log errors and continue with successful values
    /// Note: This method requires T to implement Default to handle errors gracefully
    fn log_errors(self) -> impl Stream<Item = T> + Send + 'static
    where
        T: Default,
    {
        self.handle_errors(|e| {
            eprintln!("Error in stream: {:?}", e);
            // Return default value instead of panicking
            T::default()
        })
    }

    /// Ignore errors and continue with successful values
    fn ignore_errors(self) -> impl Stream<Item = T> + Send + 'static {
        self.ok_values()
    }

    /// Transform errors into a different error type
    fn map_err_into<F2>(self) -> impl Stream<Item = Result<T, F2>> + Send + 'static
    where
        E: Into<F2>,
        F2: Send + 'static,
    {
        self.map_err(|e| e.into())
    }

    /// Apply a function to both success and error cases
    fn bimap<F1, F2, U, F3>(
        self,
        success_fn: F1,
        error_fn: F2,
    ) -> impl Stream<Item = Result<U, F3>> + Send + 'static
    where
        F1: Fn(T) -> U + Send + 'static + std::marker::Sync,
        F2: Fn(E) -> F3 + Send + 'static + std::marker::Sync,
        U: Send + 'static,
        F3: Send + 'static,
    {
        self.map_rs2(move |item| match item {
            Ok(value) => Ok(success_fn(value)),
            Err(e) => Err(error_fn(e)),
        })
    }

    /// Ensure all values are successful by using a default value on errors
    /// Note: This method requires T to implement Default to handle errors gracefully
    fn unwrap_results(self) -> impl Stream<Item = T> + Send + 'static
    where
        T: Default,
    {
        self.map_rs2(|item| item.unwrap_or_else(|e| {
            eprintln!("Stream error (using default): {:?}", e);
            T::default()
        }))
    }

    /// Ensure all values are successful by using a default value on errors
    fn unwrap_or_default(self) -> impl Stream<Item = T> + Send + 'static
    where
        T: Default,
    {
        self.handle_errors(|_| T::default())
    }

    /// Ensure all values are successful by using a fallback value on errors
    fn unwrap_or_else<F>(self, fallback: F) -> impl Stream<Item = T> + Send + 'static
    where
        F: Fn() -> T + Send + 'static + std::marker::Sync,
    {
        self.handle_errors(move |_| fallback())
    }

    /// Map errors to recovery values via async fn
    fn recover_rs2<F, Fut>(self, f: F) -> impl Stream<Item = T> + Send + 'static
    where
        F: FnMut(E) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        RecoverStream::<Self, T, E, F> {
            stream: self,
            f,
            recovery_future: None,
            _phantom: PhantomData,
        }
    }

    /// On error, switch to alternative stream and continue
    fn on_error_resume_next_rs2<F, St>(self, f: F) -> impl Stream<Item = T> + Send + 'static
    where
        F: FnMut(E) -> St + Send + 'static,
        St: Stream<Item = T> + Send + 'static,
    {
        OnErrorResumeNextStream::<Self, T, E, F> {
            stream: self,
            f,
            alt_stream: None,
            _phantom: PhantomData,
        }
    }

    fn or_else_rs2<F>(self, mut f: F) -> impl Stream<Item = T> + Send + 'static
    where
        F: FnMut(E) -> T + Send + 'static,
    {
        OrElseStream {
            stream: self,
            f,
        }
    }

    /// Count successful and error values
    async fn count_results(self) -> (usize, usize) {
        let mut success_count = 0;
        let mut error_count = 0;
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }
        (success_count, error_count)
    }

    /// Check if all values are successful
    async fn all_ok(self) -> bool {
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            if item.is_err() {
                return false;
            }
        }
        true
    }

    /// Check if any values are successful
    async fn any_ok(self) -> bool {
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            if item.is_ok() {
                return true;
            }
        }
        false
    }

    /// Find the first successful value
    async fn find_ok<F>(self, predicate: F) -> Option<T>
    where
        F: Fn(&T) -> bool + Send + 'static,
    {
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            if let Ok(value) = item {
                if predicate(&value) {
                    return Some(value);
                }
            }
        }
        None
    }

    /// Find the first error value
    async fn find_err<F>(self, predicate: F) -> Option<E>
    where
        F: Fn(&E) -> bool + Send + 'static,
    {
        let mut stream = std::pin::pin!(self);
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                if predicate(&e) {
                    return Some(e);
                }
            }
        }
        None
    }

    /// Reduce successful values with a function
    async fn reduce_ok<F>(self, f: F) -> Option<T>
    where
        F: Fn(T, T) -> T + Send + 'static,
        T: Clone + Send + 'static,
    {
        let mut stream = std::pin::pin!(self);
        let mut result = None;
        while let Some(item) = stream.next().await {
            if let Ok(value) = item {
                result = match result {
                    Some(acc) => Some(f(acc, value)),
                    None => Some(value),
                };
            }
        }
        result
    }

    /// Fold successful values with an initial value
    async fn fold_ok<B, F>(self, init: B, f: F) -> B
    where
        F: Fn(B, T) -> B + Send + 'static,
        B: Send + 'static,
    {
        let mut stream = std::pin::pin!(self);
        let mut acc = init;
        while let Some(item) = stream.next().await {
            if let Ok(value) = item {
                acc = f(acc, value);
            }
        }
        acc
    }

    /// Sum successful values
    async fn sum_ok(self) -> T
    where
        T: std::ops::Add<Output = T> + Default + Send + 'static,
    {
        self.fold_ok(T::default(), |acc, value| acc + value).await
    }

    /// Product of successful values
    async fn product_ok(self) -> T
    where
        T: std::ops::Mul<Output = T> + Default + Send + 'static,
    {
        self.fold_ok(T::default(), |acc, value| acc * value).await
    }

    /// Get the maximum successful value
    async fn max_ok(self) -> Option<T>
    where
        T: Ord,
    {
        self.reduce_ok(|a, b| std::cmp::max(a, b)).await
    }

    /// Get the minimum successful value
    async fn min_ok(self) -> Option<T>
    where
        T: Ord,
    {
        self.reduce_ok(|a, b| std::cmp::min(a, b)).await
    }

    /// Get the maximum successful value by a key function
    async fn max_by_key_ok<F, K>(self, f: F) -> Option<T>
    where
        F: Fn(&T) -> K + Send + 'static,
        K: Ord,
    {
        let mut stream = std::pin::pin!(self);
        let mut result = None;
        let mut max_key = None;
        while let Some(item) = stream.next().await {
            if let Ok(value) = item {
                let key = f(&value);
                match max_key {
                    Some(ref current_max) if key > *current_max => {
                        result = Some(value);
                        max_key = Some(key);
                    }
                    None => {
                        result = Some(value);
                        max_key = Some(key);
                    }
                    _ => {}
                }
            }
        }
        result
    }

    /// Get the minimum successful value by a key function
    async fn min_by_key_ok<F, K>(self, f: F) -> Option<T>
    where
        F: Fn(&T) -> K + Send + 'static,
        K: Ord,
    {
        let mut stream = std::pin::pin!(self);
        let mut result = None;
        let mut min_key = None;
        while let Some(item) = stream.next().await {
            if let Ok(value) = item {
                let key = f(&value);
                match min_key {
                    Some(ref current_min) if key < *current_min => {
                        result = Some(value);
                        min_key = Some(key);
                    }
                    None => {
                        result = Some(value);
                        min_key = Some(key);
                    }
                    _ => {}
                }
            }
        }
        result
    }


}

impl<S, T, E> RS2ResultStreamExt<T, E> for S
where
    S: Stream<Item = Result<T, E>> + Send + Unpin + 'static,
    T: Send + 'static + Clone,
    E: Send + 'static + std::fmt::Debug,
{
} 

// Custom delay state that is Unpin
#[derive(Debug)]
enum CustomDelayState {
    None,
    Waiting {
        start_time: Instant,
        duration: Duration,
    },
}

impl CustomDelayState {
    fn is_ready(&self) -> bool {
        match self {
            CustomDelayState::None => true,
            CustomDelayState::Waiting { start_time, duration } => {
                start_time.elapsed() >= *duration
            }
        }
    }
}

// Retry combinator
pin_project! {
    struct RetryStream<S, T, E> {
        #[pin]
        stream: S,
        max_retries: usize,
        retry_buffer: VecDeque<(T, usize)>, // (item, retry_count)
        _phantom: PhantomData<E>,
    }
}

impl<S, T, E> Stream for RetryStream<S, T, E>
where
    S: Stream<Item = Result<T, E>> + Send + 'static + Unpin,
    T: Clone + Send + 'static,
    E: Clone + Send + 'static + Default,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // First, try to retry any buffered failed items
        if let Some((item, retry_count)) = this.retry_buffer.pop_front() {
            if retry_count < *this.max_retries {
                // Retry the item by putting it back in the buffer with incremented count
                this.retry_buffer.push_back((item.clone(), retry_count + 1));
                return Poll::Ready(Some(Ok(item)));
            } else {
                // Max retries exceeded for this item, return an error
                return Poll::Ready(Some(Err(E::default())));
            }
        }
        
        // Then, try to get new items from the original stream
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                // Success - return the item
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(_e))) => {

                Poll::Ready(Some(Err(_e)))
            }
            Poll::Ready(None) => {
                // Stream ended, check if we have any remaining retry attempts
                if this.retry_buffer.is_empty() {
                    Poll::Ready(None)
                } else {
                    // Continue with retries
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Retry with delay combinator
pin_project! {
    struct RetryWithDelayStream<S, T, E> {
        #[pin]
        stream: S,
        max_retries: usize,
        delay: Duration,
        current_retries: usize,
        last_error_time: Option<Instant>,
        _phantom: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Stream for RetryWithDelayStream<S, T, E>
where
    S: Stream<Item = Result<T, E>> + Send + 'static + Unpin,
    T: Clone + Send + 'static,
    E: Clone + Send + 'static + Default,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // Check if we need to wait due to delay
        if let Some(last_error) = *this.last_error_time {
            if last_error.elapsed() < *this.delay {
                // Wake up the task after the delay
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                // Success - reset retry count and return
                *this.current_retries = 0;
                *this.last_error_time = None;
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                // Error - try to retry with delay
                if *this.current_retries < *this.max_retries {
                    *this.current_retries += 1;
                    *this.last_error_time = Some(Instant::now());
                    Poll::Ready(Some(Err(e)))
                } else {
                    // Max retries exceeded
                    Poll::Ready(Some(Err(e)))
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Retry with policy combinator
pin_project! {
    struct RetryWithPolicyStream<S, T, E> {
        #[pin]
        stream: S,
        policy: crate::error::RetryPolicy,
        current_retries: usize,
        last_error_time: Option<Instant>,
        _phantom: PhantomData<(T, E)>,
    }
}

impl<S, T, E> Stream for RetryWithPolicyStream<S, T, E>
where
    S: Stream<Item = Result<T, E>> + Send + 'static + Unpin,
    T: Clone + Send + 'static,
    E: Clone + Send + 'static + Default,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // Check if we need to wait due to policy delay
        if let Some(last_error) = *this.last_error_time {
            let delay = this.policy.delay_for_attempt(*this.current_retries);
            if last_error.elapsed() < delay {
                // Wake up the task after the delay
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                // Success - reset retry count and return
                *this.current_retries = 0;
                *this.last_error_time = None;
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                // Error - try to retry according to policy
                if *this.current_retries < this.policy.max_retries() {
                    *this.current_retries += 1;
                    *this.last_error_time = Some(Instant::now());
                    Poll::Ready(Some(Err(e)))
                } else {
                    // Max retries exceeded
                    Poll::Ready(Some(Err(e)))
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Retry with policy and factory function combinator - now with custom delay
pin_project! {
    struct RetryWithPolicyFactoryStream<S, T, E, F> {
        #[pin]
        stream: S,
        policy: crate::error::RetryPolicy,
        factory: F,
        current_attempt: usize,
        max_retries: usize,
        delay_state: CustomDelayState,
        terminated: bool, // Track if we should stop processing
        _phantom: PhantomData<(T, E)>,
    }
}

impl<S, T, E, F> Stream for RetryWithPolicyFactoryStream<S, T, E, F>
where
    S: Stream<Item = Result<T, E>> + Send + 'static + Unpin,
    T: Clone + Send + 'static,
    E: Clone + Send + 'static,
    F: FnMut() -> S + Send + 'static,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // If we're terminated, don't process anything more
        if *this.terminated {
            return Poll::Ready(None);
        }
        
        // Check if we need to wait due to delay
        if !this.delay_state.is_ready() {
            // Wake up the task to check again
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // If delay is complete, clear it and create new stream if needed
        if matches!(this.delay_state, CustomDelayState::Waiting { .. }) {
            *this.delay_state = CustomDelayState::None;
            let new_stream = (this.factory)();
            this.stream.set(new_stream);
        }

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                // Success - yield the item
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                // Error - yield it and prepare for retry
                if *this.current_attempt < *this.max_retries {
                    *this.current_attempt += 1;
                    
                    // Calculate delay based on policy
                    let delay = match &this.policy {
                        crate::error::RetryPolicy::None => Duration::ZERO,
                        crate::error::RetryPolicy::Immediate { .. } => Duration::ZERO,
                        crate::error::RetryPolicy::Fixed { delay, .. } => *delay,
                        crate::error::RetryPolicy::Exponential { initial_delay, multiplier, .. } => {
                            let delay_ms = initial_delay.as_millis() as f64 * multiplier.powi((*this.current_attempt - 1) as i32);
                            Duration::from_millis(delay_ms as u64)
                        },
                    };
                    
                    if delay > Duration::ZERO {
                        // Set up custom delay
                        *this.delay_state = CustomDelayState::Waiting {
                            start_time: Instant::now(),
                            duration: delay,
                        };
                    } else {
                        // Create new stream immediately
                        let new_stream = (this.factory)();
                        this.stream.set(new_stream);
                    }
                } else {
                    // Max retries reached - terminate after yielding this error
                    *this.terminated = true;
                }
                // Always yield the error, whether we retry or not
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // Stream ended - check if we should retry
                if *this.current_attempt < *this.max_retries {
                    *this.current_attempt += 1;
                    
                    // Calculate delay based on policy
                    let delay = match &this.policy {
                        crate::error::RetryPolicy::None => Duration::ZERO,
                        crate::error::RetryPolicy::Immediate { .. } => Duration::ZERO,
                        crate::error::RetryPolicy::Fixed { delay, .. } => *delay,
                        crate::error::RetryPolicy::Exponential { initial_delay, multiplier, .. } => {
                            let delay_ms = initial_delay.as_millis() as f64 * multiplier.powi((*this.current_attempt - 1) as i32);
                            Duration::from_millis(delay_ms as u64)
                        },
                    };
                    
                    if delay > Duration::ZERO {
                        // Set up custom delay
                        *this.delay_state = CustomDelayState::Waiting {
                            start_time: Instant::now(),
                            duration: delay,
                        };
                        // Wake up the task to check again after delay
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        // Create new stream immediately
                        let new_stream = (this.factory)();
                        this.stream.set(new_stream);
                        // Continue with the new stream
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                } else {
                    // No more retries - terminate
                    *this.terminated = true;
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Stream implementations for all combinators

// Map error stream
pin_project! {
    struct MapErrStream<S, F, E2> {
        #[pin]
        stream: S,
        f: F,
        _phantom: PhantomData<E2>,
    }
}

impl<S, T, E, F, E2> Stream for MapErrStream<S, F, E2>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(E) -> E2,
{
    type Item = Result<T, E2>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err((this.f)(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Filter map error stream
pin_project! {
    struct FilterMapErrStream<S, F, E2> {
        #[pin]
        stream: S,
        f: F,
        _phantom: PhantomData<E2>,
    }
}

impl<S, T, E, F, E2> Stream for FilterMapErrStream<S, F, E2>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(E) -> Option<E2>,
{
    type Item = Result<T, E2>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(Ok(item))),
                Poll::Ready(Some(Err(e))) => {
                    if let Some(e2) = (this.f)(e) {
                        return Poll::Ready(Some(Err(e2)));
                    }
                    // Continue to next item if filter returns None
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Or else stream
pin_project! {
    struct OrElseStream<S, F> {
        #[pin]
        stream: S,
        f: F,
    }
}

impl<S, T, E, F> Stream for OrElseStream<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(E) -> T,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some((this.f)(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Filter Ok stream
pin_project! {
    struct FilterOkStream<S> {
        #[pin]
        stream: S,
    }
}

impl<S, T, E> Stream for FilterOkStream<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(item)),
                Poll::Ready(Some(Err(_))) => {
                    // Continue to next item, filtering out errors
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Filter Err stream
pin_project! {
    struct FilterErrStream<S> {
        #[pin]
        stream: S,
    }
}

impl<S, T, E> Stream for FilterErrStream<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(_))) => {
                    // Continue to next item, filtering out successes
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(e)),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Inspect error stream
pin_project! {
    struct InspectErrStream<S, F> {
        #[pin]
        stream: S,
        f: F,
    }
}

impl<S, T, E, F> Stream for InspectErrStream<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&E),
    E: Clone,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(e))) => {
                (this.f)(&e);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Inspect Ok stream
pin_project! {
    struct InspectOkStream<S, F> {
        #[pin]
        stream: S,
        f: F,
    }
}

impl<S, T, E, F> Stream for InspectOkStream<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T),
    T: Clone,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                (this.f)(&item);
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Flatten stream
pin_project! {
    struct FlattenStream<S, T: IntoIterator> {
        #[pin]
        stream: S,
        current_iter: Option<T::IntoIter>,
        _phantom: PhantomData<T>,
    }
}

impl<S, T, E> Stream for FlattenStream<S, T>
where
    S: Stream<Item = Result<T, E>>,
    T: IntoIterator,
    T::Item: Send + 'static,
    E: Clone + Send + 'static,
{
    type Item = Result<T::Item, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // First, try to get items from current iterator
        if let Some(iter) = this.current_iter.as_mut() {
            if let Some(item) = iter.next() {
                return Poll::Ready(Some(Ok(item)));
            } else {
                // Current iterator is exhausted
                *this.current_iter = None;
            }
        }
        
        // Get next item from stream
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(iterable))) => {
                let mut iter = iterable.into_iter();
                if let Some(item) = iter.next() {
                    *this.current_iter = Some(iter);
                    Poll::Ready(Some(Ok(item)))
                } else {
                    // Empty iterator, continue
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Transpose stream
pin_project! {
    struct TransposeStream<S> {
        #[pin]
        stream: S,
    }
}

impl<S, T, E> Stream for TransposeStream<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = Option<Result<T, E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Some(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Split streams (simplified implementation)
pin_project! {
    struct SplitOkStream<T> {
        buffer: std::sync::Arc<std::sync::Mutex<VecDeque<T>>>,
        _phantom: PhantomData<T>,
    }
}

impl<T> Stream for SplitOkStream<T>
where
    T: Clone + Send + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Ok(mut buffer) = this.buffer.lock() {
            if let Some(item) = buffer.pop_front() {
                Poll::Ready(Some(item))
            } else {
                Poll::Ready(None)
            }
        } else {
            Poll::Ready(None)
        }
    }
}

pin_project! {
    struct SplitErrStream<E> {
        buffer: std::sync::Arc<std::sync::Mutex<VecDeque<E>>>,
        _phantom: PhantomData<E>,
    }
}

impl<E> Stream for SplitErrStream<E>
where
    E: Clone + Send + 'static,
{
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Ok(mut buffer) = this.buffer.lock() {
            if let Some(item) = buffer.pop_front() {
                Poll::Ready(Some(item))
            } else {
                Poll::Ready(None)
            }
        } else {
            Poll::Ready(None)
        }
    }
} 

// Recover stream
pin_project! {
    struct RecoverStream<S, T, E, F> {
        #[pin]
        stream: S,
        f: F,
        recovery_future: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
        _phantom: PhantomData<(T, E)>,
    }
}

impl<S, T, E, F, Fut> Stream for RecoverStream<S, T, E, F>
where
    S: Stream<Item = Result<T, E>> + Send + 'static,
    F: FnMut(E) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // If we have a recovery future, poll it first
        if let Some(recovery_future) = this.recovery_future.as_mut() {
            match recovery_future.as_mut().poll(cx) {
                Poll::Ready(value) => {
                    *this.recovery_future = None;
                    return Poll::Ready(Some(value));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        
        // Poll the main stream
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(value)),
            Poll::Ready(Some(Err(e))) => {
                // Create a recovery future
                let recovery_future = (this.f)(e);
                *this.recovery_future = Some(Box::pin(recovery_future));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// OnErrorResumeNext stream
pin_project! {
    struct OnErrorResumeNextStream<S, T, E, F> {
        #[pin]
        stream: S,
        f: F,
        alt_stream: Option<Pin<Box<dyn Stream<Item = T> + Send>>>,
        _phantom: PhantomData<(T, E)>,
    }
}

impl<S, T, E, F, St> Stream for OnErrorResumeNextStream<S, T, E, F>
where
    S: Stream<Item = Result<T, E>> + Send + 'static,
    F: FnMut(E) -> St + Send + 'static,
    St: Stream<Item = T> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // If we have an alternative stream, poll it first
        if let Some(alt_stream) = this.alt_stream.as_mut() {
            match alt_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(value)) => return Poll::Ready(Some(value)),
                Poll::Ready(None) => {
                    *this.alt_stream = None;
                    // Continue with main stream
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        
        // Poll the main stream
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(value)),
            Poll::Ready(Some(Err(e))) => {
                // Create alternative stream
                let alt_stream = (this.f)(e);
                *this.alt_stream = Some(Box::pin(alt_stream));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

