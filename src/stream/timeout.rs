use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use super::core::Stream;
use crate::error::{StreamError, StreamResult};

pin_project! {
    /// A stream that times out after a specified duration
    pub struct TimeoutStream<S> {
        #[pin]
        stream: S,
        timeout_duration: Duration,
        last_activity: Instant,
    }
}

impl<S> TimeoutStream<S>
where
    S: Stream,
{
    /// Create a new TimeoutStream that will timeout after the specified duration
    pub fn new(stream: S, duration: Duration) -> Self {
        Self {
            stream,
            timeout_duration: duration,
            last_activity: Instant::now(),
        }
    }
}

impl<S> Stream for TimeoutStream<S>
where
    S: Stream,
{
    type Item = StreamResult<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Check if we've exceeded the timeout duration
        if this.last_activity.elapsed() >= *this.timeout_duration {
            // Reset the timer and return timeout error
            *this.last_activity = Instant::now();
            return Poll::Ready(Some(Err(StreamError::Timeout)));
        }

        // Poll the inner stream
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Reset the timeout when we get an item
                *this.last_activity = Instant::now();
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Extension trait for adding timeout functionality to streams
pub trait TimeoutStreamExt: Stream + Sized {
    /// Add a timeout to this stream
    fn timeout(self, duration: Duration) -> TimeoutStream<Self> {
        TimeoutStream::new(self, duration)
    }
}

impl<S> TimeoutStreamExt for S where S: Stream {}
