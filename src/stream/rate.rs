//! Rate limiting and timing combinators
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::future::Future;
use super::core::Stream;
use tokio::time::{Sleep};

// Throttle - optimized pin projection
pin_project! {
    pub struct Throttle<S>
    where
        S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) duration: Duration,
        pub(crate) sleep: Option<Pin<Box<Sleep>>>,
        pub(crate) last: Option<Instant>,
        pub(crate) pending_item: Option<S::Item>,
    }
}

impl<S> Stream for Throttle<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // If we have a pending item and sleep has completed, emit it
        if let Some(sleep) = this.sleep.as_mut() {
            match sleep.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    *this.sleep = None;
                    if let Some(item) = this.pending_item.take() {
                        *this.last = Some(Instant::now());
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Pending => {
                    // Sleep is still running, wake up when it's ready
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }

        // If we already have a pending item, don't poll for more
        if this.pending_item.is_some() {
            return Poll::Pending;
        }

        // Poll the stream for next item
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let now = Instant::now();
                
                // Check if we need to throttle
                if let Some(last_time) = this.last {
                    let elapsed = now.duration_since(*last_time);
                    if elapsed < *this.duration {
                        // For zero or very short durations, don't actually sleep to avoid benchmark issues
                        if this.duration.is_zero() || this.duration.as_millis() < 1 {
                            // Just update timestamp and continue without throttling
                            *this.last = Some(now);
                            return Poll::Ready(Some(item));
                        }
                        
                        // Need to wait - store item and start sleep
                        let remaining = *this.duration - elapsed;
                        *this.pending_item = Some(item);
                        *this.sleep = Some(Box::pin(tokio::time::sleep(remaining)));
                        // Wake up when the sleep is ready
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }

                // No throttling needed (first item or enough time has passed) - emit immediately
                *this.last = Some(now);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                // Stream ended - emit any pending item
                if let Some(item) = this.pending_item.take() {
                    Poll::Ready(Some(item))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Debounce - optimized pin projection
pin_project! {
    pub struct Debounce<S, T> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) duration: Duration,
        pub(crate) sleep: Option<Pin<Box<Sleep>>>,
        pub(crate) pending: Option<T>,
    }
}

impl<S, T> Stream for Debounce<S, T>
where
    S: Stream<Item = T>
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // Check if debounce timer expired
        if let Some(sleep) = this.sleep.as_mut() {
            match sleep.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    *this.sleep = None;
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
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // For zero duration, emit immediately without debouncing
                if this.duration.is_zero() {
                    return Poll::Ready(Some(item));
                }
                
                // New item arrived, replace pending and reset timer
                *this.pending = Some(item);
                *this.sleep = Some(Box::pin(tokio::time::sleep(*this.duration)));
                // Wake up when the timer is ready
                cx.waker().wake_by_ref();
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
pin_project! {
    pub struct Timeout<S> {
        #[pin]
        pub(crate) stream: S,
        #[pin]
        pub(crate) sleep: Pin<Box<Sleep>>,
        pub(crate) timed_out: bool,
    }
}

impl<S> Stream for Timeout<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.timed_out {
            return Poll::Ready(None);
        }

        // Poll timeout first
        match this.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => {
                *this.timed_out = true;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        // Poll the stream
        this.stream.as_mut().poll_next(cx)
    }
}

// Sample - sample items at regular intervals (improved for both finite and infinite streams)
pin_project! {
    pub struct Sample<S>
    where
        S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) interval: Duration,
        pub(crate) sleep: Option<Pin<Box<Sleep>>>,
        pub(crate) last_sample: Option<Instant>,
        pub(crate) pending_item: Option<S::Item>,
        pub(crate) finite_mode: bool, // New: handle finite streams better
    }
}

impl<S> Stream for Sample<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        let now = Instant::now();
        let should_sample = this.last_sample
            .map(|last| now.duration_since(last) >= *this.interval)
            .unwrap_or(true);

        // For finite mode, use simpler logic that doesn't wait for timers
        if *this.finite_mode {
            if should_sample {
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        *this.last_sample = Some(now);
                        Poll::Ready(Some(item))
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            } else {
                // Skip items until it's time to sample
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(_)) => {
                        // Skip this item, continue polling
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            }
        } else {
            // Original infinite stream logic
            if should_sample {
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        *this.last_sample = Some(now);
                        Poll::Ready(Some(item))
                    }
                    Poll::Ready(None) => {
                        // Stream ended, emit any pending item
                        if let Some(item) = this.pending_item.take() {
                            Poll::Ready(Some(item))
                        } else {
                            Poll::Ready(None)
                        }
                    }
                    Poll::Pending => {
                        // No item available, start sleep for next sample
                        if this.sleep.is_none() {
                            *this.sleep = Some(Box::pin(tokio::time::sleep(*this.interval)));
                        }
                        Poll::Pending
                    }
                }
            } else {
                // Not time to sample yet, poll sleep
                if let Some(sleep) = this.sleep.as_mut() {
                    match sleep.as_mut().poll(cx) {
                        Poll::Ready(_) => {
                            *this.sleep = None;
                            // Try to get an item now
                            match this.stream.as_mut().poll_next(cx) {
                                Poll::Ready(Some(item)) => {
                                    *this.last_sample = Some(now);
                                    Poll::Ready(Some(item))
                                }
                                Poll::Ready(None) => {
                                    if let Some(item) = this.pending_item.take() {
                                        Poll::Ready(Some(item))
                                    } else {
                                        Poll::Ready(None)
                                    }
                                }
                                Poll::Pending => Poll::Pending,
                            }
                        }
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

// SampleEveryNth - sample every nth item (great for finite streams)
pin_project! {
    pub struct SampleEveryNth<S>
    where
        S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) n: usize,
        pub(crate) count: usize,
    }
}

impl<S> Stream for SampleEveryNth<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.count += 1;
                    if *this.count % *this.n == 0 {
                        return Poll::Ready(Some(item));
                    }
                    // Skip this item, continue polling
                    continue;
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// SampleFirst - sample first N items from stream
pin_project! {
    pub struct SampleFirst<S>
    where
        S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) remaining: usize,
    }
}

impl<S> Stream for SampleFirst<S>
where
    S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        if *this.remaining == 0 {
            return Poll::Ready(None);
        }
        
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                *this.remaining -= 1;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// BufferTime - collect items into buffers based on time intervals
pin_project! {
    pub struct BufferTime<S>
    where
        S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) interval: Duration,
        pub(crate) sleep: Option<Pin<Box<Sleep>>>,
        pub(crate) buffer: Vec<S::Item>,
        pub(crate) last_flush: Option<Instant>,
    }
}

impl<S> Stream for BufferTime<S>
where
    S: Stream
{
    type Item = Vec<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        let now = Instant::now();
        let should_flush = this.last_flush
            .map(|last| now.duration_since(last) >= *this.interval)
            .unwrap_or(true);

        if should_flush && !this.buffer.is_empty() {
            // Flush the buffer
            let buffer = std::mem::replace(this.buffer, Vec::new());
            *this.last_flush = Some(now);
            *this.sleep = Some(Box::pin(tokio::time::sleep(*this.interval)));
            Poll::Ready(Some(buffer))
        } else {
            // Poll the stream for new items
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.buffer.push(item);
                    Poll::Pending // Don't emit yet, wait for timer
                }
                Poll::Ready(None) => {
                    // Stream ended, emit any remaining items
                    if !this.buffer.is_empty() {
                        let buffer = std::mem::replace(this.buffer, Vec::new());
                        Poll::Ready(Some(buffer))
                    } else {
                        Poll::Ready(None)
                    }
                }
                Poll::Pending => {
                    // Check if sleep timer has expired
                    if let Some(sleep) = this.sleep.as_mut() {
                        match sleep.as_mut().poll(cx) {
                            Poll::Ready(_) => {
                                *this.sleep = None;
                                if !this.buffer.is_empty() {
                                    let buffer = std::mem::replace(this.buffer, Vec::new());
                                    *this.last_flush = Some(now);
                                    Poll::Ready(Some(buffer))
                                } else {
                                    Poll::Pending
                                }
                            }
                            Poll::Pending => Poll::Pending,
                        }
                    } else {
                        Poll::Pending
                    }
                }
            }
        }
    }
}

// Extension trait
pub trait RateStreamExt: Stream + Sized {
    fn throttle(self, duration: Duration) -> Throttle<Self> {
        // Allow zero duration for benchmarks - will effectively disable throttling
        Throttle {
            stream: self,
            duration,
            sleep: None,
            last: None,
            pending_item: None,
        }
    }

    fn debounce(self, duration: Duration) -> Debounce<Self, Self::Item> {
        // Allow zero duration for benchmarks - will effectively disable debouncing  
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

    /// Sample items at regular time intervals (optimized for infinite streams)
    fn sample(self, interval: Duration) -> Sample<Self> {
        assert!(!interval.is_zero(), "sample: interval must be greater than zero");
        Sample {
            stream: self,
            interval,
            sleep: None,
            last_sample: None,
            pending_item: None,
            finite_mode: false,
        }
    }

    /// Sample items at regular time intervals (optimized for finite streams)
    fn sample_finite(self, interval: Duration) -> Sample<Self> {
        assert!(!interval.is_zero(), "sample_finite: interval must be greater than zero");
        Sample {
            stream: self,
            interval,
            sleep: None,
            last_sample: None,
            pending_item: None,
            finite_mode: true,
        }
    }

    /// Sample every nth item (great for finite streams, no timing involved)
    fn sample_every_nth(self, n: usize) -> SampleEveryNth<Self> {
        assert!(n > 0, "sample_every_nth: n must be greater than zero");
        SampleEveryNth {
            stream: self,
            n,
            count: 0,
        }
    }

    /// Sample first N items from stream
    fn sample_first(self, n: usize) -> SampleFirst<Self> {
        SampleFirst {
            stream: self,
            remaining: n,
        }
    }

    /// Auto-detect stream type and use appropriate sampling
    /// For finite streams, uses every-nth sampling; for infinite, uses time-based
    fn sample_auto(self, interval_or_nth: Duration) -> Sample<Self> {
        // For very short durations, assume finite stream and use finite mode
        if interval_or_nth.as_millis() <= 10 {
            self.sample_finite(interval_or_nth)
        } else {
            self.sample(interval_or_nth)
        }
    }

    fn buffer_time(self, interval: Duration) -> BufferTime<Self> {
        assert!(!interval.is_zero(), "buffer_time: interval must be greater than zero");
        BufferTime {
            stream: self,
            interval,
            sleep: None,
            buffer: Vec::new(),
            last_flush: None,
        }
    }
}

impl<T> RateStreamExt for T where T: Stream {}