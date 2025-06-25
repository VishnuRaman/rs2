//! Rate limiting and timing combinators
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::future::Future;
use super::core::Stream;
use tokio::time::{Sleep};

// Throttle - optimized pin projection
pub struct Throttle<S> {
    pub(crate) stream: S,
    pub(crate) duration: Duration,
    pub(crate) sleep: Option<Pin<Box<Sleep>>>,
    pub(crate) last: Option<Instant>,
}

impl<S> Stream for Throttle<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        
        // Poll sleep if present
        if let Some(sleep) = &mut this.sleep {
            match sleep.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    this.sleep = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Poll the stream
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let now = Instant::now();
                
                if let Some(last_time) = this.last {
                    let elapsed = now.duration_since(last_time);
                    if elapsed < this.duration {
                        let remaining = this.duration - elapsed;
                        this.sleep = Some(Box::pin(tokio::time::sleep(remaining)));
                        return Poll::Pending; // Wait for sleep to complete
                    }
                }

                this.last = Some(now);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Debounce - optimized pin projection
pub struct Debounce<S, T> {
    pub(crate) stream: S,
    pub(crate) duration: Duration,
    pub(crate) sleep: Option<Pin<Box<Sleep>>>,
    pub(crate) pending: Option<T>,
}

impl<S, T> Stream for Debounce<S, T>
where
    S: Stream<Item = T>
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        
        // Check if debounce timer expired
        if let Some(sleep) = &mut this.sleep {
            match sleep.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    this.sleep = None;
                    if let Some(item) = this.pending.take() {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Pending => {
                    // Timer still running, check for new items
                }
            }
        }

        // Poll the stream for new items
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // New item arrived, replace pending and reset timer
                this.pending = Some(item);
                this.sleep = Some(Box::pin(tokio::time::sleep(this.duration)));
                Poll::Pending // Wait for timer to expire
            }
            Poll::Ready(None) => {
                // Stream ended, emit pending item if any
                if let Some(item) = this.pending.take() {
                    Poll::Ready(Some(item))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => {
                // No new items available
                if this.sleep.is_none() {
                    Poll::Pending
                } else {
                    // Timer is running, continue polling it
                    Poll::Pending
                }
            }
        }
    }
}

// Timeout - optimized pin projection
pub struct Timeout<S> {
    pub(crate) stream: S,
    pub(crate) sleep: Pin<Box<Sleep>>,
    pub(crate) timed_out: bool,
}

impl<S> Stream for Timeout<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        if this.timed_out {
            return Poll::Ready(None);
        }

        // Poll timeout first
        match this.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => {
                this.timed_out = true;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        // Poll the stream
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        stream.poll_next(cx)
    }
}

// Extension trait
pub trait RateStreamExt: Stream + Sized {
    fn throttle(self, duration: Duration) -> Throttle<Self> {
        assert!(!duration.is_zero(), "throttle: duration must be greater than zero");
        Throttle {
            stream: self,
            duration,
            sleep: None,
            last: None
        }
    }

    fn debounce(self, duration: Duration) -> Debounce<Self, Self::Item> {
        assert!(!duration.is_zero(), "debounce: duration must be greater than zero");
        Debounce {
            stream: self,
            duration,
            sleep: None,
            pending: None
        }
    }

    fn timeout(self, duration: Duration) -> Timeout<Self> {
        assert!(!duration.is_zero(), "timeout: duration must be greater than zero");
        Timeout {
            stream: self,
            sleep: Box::pin(tokio::time::sleep(duration)),
            timed_out: false
        }
    }
}

impl<T> RateStreamExt for T where T: Stream {}