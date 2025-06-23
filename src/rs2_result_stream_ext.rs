use async_stream::stream;
use futures_core::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

use crate::error::RetryPolicy;
use crate::{bracket_case, rate_limit_backpressure, ExitCase, RS2Stream};

/// Extension trait for streams containing Result types
pub trait RS2ResultStreamExt<T: Send + 'static, E: Send + 'static>:
    Stream<Item = Result<T, E>> + Sized + Unpin + Send + 'static
{
    /// Map errors to recovery values via async fn
    fn recover_rs2<F, Fut>(self, mut f: F) -> RS2Stream<T>
    where
        F: FnMut(E) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let s = self.boxed();
        stream! {
            pin_mut!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(v) => yield v,
                    Err(e) => yield f(e).await,
                }
            }
        }
        .boxed()
    }

    /// On error, switch to alternative rs2_stream and continue
    fn on_error_resume_next_rs2<F, St>(self, mut f: F) -> RS2Stream<T>
    where
        F: FnMut(E) -> St + Send + 'static,
        St: Stream<Item = T> + Send + 'static,
        T: Send + 'static,
    {
        let s = self.boxed();
        stream! {
            pin_mut!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(v) => yield v,
                    Err(e) => {
                        let alt = f(e);
                        pin_mut!(alt);
                        while let Some(u) = alt.next().await {
                            yield u;
                        }
                    }
                }
            }
        }
        .boxed()
    }

    /// Retry the rs2_stream on first error up to `times`
    fn retry_rs2<FAct>(self, times: usize, mut action: FAct) -> RS2Stream<Result<T, E>>
    where
        FAct: FnMut() -> Self + Send + 'static,
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        stream! {
            let mut attempts = 0;
            let mut stream = self;

            loop {
                let mut errored = false;
                let mut boxed_stream = stream.boxed();

                while let Some(item) = boxed_stream.next().await {
                    yield item.clone();
                    if item.is_err() {
                        errored = true;
                        break;
                    }
                }

                if !errored || attempts >= times {
                    break;
                }

                attempts += 1;
                stream = action();
            }
        }
        .boxed()
    }

    /// Map errors to a different type
    fn map_error_rs2<E2, F>(self, mut f: F) -> RS2Stream<Result<T, E2>>
    where
        F: FnMut(E) -> E2 + Send + 'static,
        T: Send + 'static,
        E2: Send + 'static,
    {
        let s = self.boxed();
        stream! {
            pin_mut!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(v) => yield Ok(v),
                    Err(e) => yield Err(f(e)),
                }
            }
        }
        .boxed()
    }

    /// Replace errors with fallback values
    fn or_else_rs2<F>(self, mut f: F) -> RS2Stream<T>
    where
        F: FnMut(E) -> T + Send + 'static,
        T: Send + 'static,
    {
        let s = self.boxed();
        stream! {
            pin_mut!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(v) => yield v,
                    Err(e) => yield f(e),
                }
            }
        }
        .boxed()
    }

    /// Collect only successful values into a Vec
    fn collect_ok_rs2(self) -> RS2Stream<Vec<T>>
    where
        T: Send + 'static,
    {
        let s = self.boxed();
        stream! {
            let mut successes = Vec::new();
            pin_mut!(s);
            while let Some(item) = s.next().await {
                if let Ok(v) = item {
                    successes.push(v);
                }
            }
            yield successes;
        }
        .boxed()
    }

    /// Collect only errors into a Vec
    fn collect_err_rs2(self) -> RS2Stream<Vec<E>>
    where
        E: Send + 'static,
    {
        let s = self.boxed();
        stream! {
            let mut errors = Vec::new();
            pin_mut!(s);
            while let Some(item) = s.next().await {
                if let Err(e) = item {
                    errors.push(e);
                }
            }
            yield errors;
        }
        .boxed()
    }

    /// Retry with policy
    fn retry_with_policy_rs2<F>(
        self,
        policy: RetryPolicy,
        mut factory: F,
    ) -> RS2Stream<Result<T, E>>
    where
        F: FnMut() -> Self + Send + 'static,
        T: Clone + Send + 'static,
        E: Clone + Send + 'static,
    {
        stream! {
            let mut attempts = 0;
            let max_retries = match policy {
                RetryPolicy::None => 0,
                RetryPolicy::Immediate { max_retries } => max_retries,
                RetryPolicy::Fixed { max_retries, .. } => max_retries,
                RetryPolicy::Exponential { max_retries, .. } => max_retries,
            };

            let mut stream = self;

            loop {
                let mut had_error = false;
                let mut boxed_stream = stream.boxed();

                while let Some(item) = boxed_stream.next().await {
                    yield item.clone();
                    if item.is_err() {
                        had_error = true;
                        break;
                    }
                }

                if !had_error || attempts >= max_retries {
                    break;
                }

                match &policy {
                    RetryPolicy::None => {},
                    RetryPolicy::Immediate { .. } => {},
                    RetryPolicy::Fixed { delay, .. } => {
                        sleep(*delay).await;
                    },
                    RetryPolicy::Exponential { initial_delay, multiplier, .. } => {
                        let delay_ms = initial_delay.as_millis() as f64 * multiplier.powi(attempts as i32);
                        let delay = Duration::from_millis(delay_ms as u64);
                        sleep(delay).await;
                    },
                }

                attempts += 1;
                stream = factory();
            }
        }.boxed()
    }

    /// Apply back-pressure-aware rate limiting via bounded channel
    fn rate_limit_backpressure_rs2(self, capacity: usize) -> RS2Stream<Result<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        rate_limit_backpressure(self.boxed(), capacity)
    }

    /// BracketCase with exit case semantics for streams of Result<O,E>
    fn bracket_case_rs2<A, St, FAcq, FUse, FRel, R>(
        self,
        acquire: FAcq,
        use_fn: FUse,
        release: FRel,
    ) -> RS2Stream<Result<T, E>>
    where
        FAcq: Future<Output = A> + Send + 'static,
        FUse: FnOnce(A) -> St + Send + 'static,
        St: Stream<Item = Result<T, E>> + Send + 'static,
        FRel: FnOnce(A, ExitCase<E>) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
        T: Send + 'static,
        E: Clone + Send + 'static,
        A: Clone + Send + 'static,
    {
        bracket_case(acquire, use_fn, release)
    }
}

impl<T, E, S> RS2ResultStreamExt<T, E> for S
where
    S: Stream<Item = Result<T, E>> + Sized + Unpin + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
}
