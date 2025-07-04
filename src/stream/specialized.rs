//! Specialized stream combinators: TryStream, chunks, take_until, skip_until, backpressure
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tokio::time::Sleep;
use super::core::Stream;

// ================================
// TryStream Trait & Combinators
// ================================

/// A stream that yields `Result<T, E>` items
pub trait TryStream: Stream<Item = Result<Self::Ok, Self::Error>> {
    type Ok;
    type Error;

    fn try_map<U, F>(self, f: F) -> TryMap<Self, F>
    where F: FnMut(Self::Ok) -> U, Self: Sized;

    fn try_filter<F>(self, f: F) -> TryFilter<Self, F>
    where F: FnMut(&Self::Ok) -> bool, Self: Sized;

    fn try_fold<B, F>(self, init: B, f: F) -> TryFold<Self, B, F>
    where F: FnMut(B, Self::Ok) -> B, B: Default, Self: Sized;

    fn try_for_each<F>(self, f: F) -> TryForEach<Self, F>
    where F: FnMut(Self::Ok) -> Result<(), Self::Error>, Self: Sized;
}

impl<S, T, E> TryStream for S
where S: Stream<Item = Result<T, E>>
{
    type Ok = T;
    type Error = E;

    fn try_map<U, F>(self, f: F) -> TryMap<Self, F>
    where F: FnMut(Self::Ok) -> U
    {
        TryMap { stream: self, f }
    }

    fn try_filter<F>(self, f: F) -> TryFilter<Self, F>
    where F: FnMut(&Self::Ok) -> bool
    {
        TryFilter { stream: self, f }
    }

    fn try_fold<B, F>(self, init: B, f: F) -> TryFold<Self, B, F>
    where F: FnMut(B, Self::Ok) -> B
    {
        TryFold { stream: self, acc: init, f }
    }

    fn try_for_each<F>(self, f: F) -> TryForEach<Self, F>
    where F: FnMut(Self::Ok) -> Result<(), Self::Error>
    {
        TryForEach { stream: self, f }
    }
}

pin_project! {
    pub struct TryMap<S, F>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
    }
}

impl<S, F, U, T, E> Stream for TryMap<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(T) -> U,
{
    type Item = Result<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok((this.f)(item)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    pub struct TryFilter<S, F>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
    }
}

impl<S, F, T, E> Stream for TryFilter<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    if (this.f)(&item) {
                        return Poll::Ready(Some(Ok(item)));
                    }
                    // Continue filtering
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    pub struct TryFold<S, B, F>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) acc: B,
        pub(crate) f: F,
    }
}

impl<S, B, F, T, E> Future for TryFold<S, B, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(B, T) -> B,
    B: Default,
{
    type Output = Result<B, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    *this.acc = (this.f)(std::mem::take(this.acc), item);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(std::mem::take(this.acc))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    pub struct TryForEach<S, F>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
    }
}

impl<S, F, T, E> Future for TryForEach<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(T) -> Result<(), E>,
{
    type Output = Result<(), E>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    // Early termination on first error
                    if let Err(e) = (this.f)(item) {
                        return Poll::Ready(Err(e));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Chunks Combinators
// ================================

pin_project! {
    pub struct Chunks<S>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) size: usize,
        pub(crate) buffer: Vec<S::Item>,
    }
}

impl<S> Stream for Chunks<S>
where S: Stream
{
    type Item = Vec<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Fill buffer until we have enough items
        while this.buffer.len() < *this.size {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.buffer.push(item);
                }
                Poll::Ready(None) => {
                    // Stream ended, return remaining items if any
                    if this.buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(std::mem::take(this.buffer)));
                    }
                }
                Poll::Pending => {
                    // If we have some items but not enough, return what we have
                    if !this.buffer.is_empty() {
                        return Poll::Ready(Some(std::mem::take(this.buffer)));
                    }
                    return Poll::Pending;
                }
            }
        }

        // We have enough items, return a chunk
        let chunk = std::mem::replace(this.buffer, Vec::new());
        Poll::Ready(Some(chunk))
    }
}

pin_project! {
    pub struct ChunksTimeout<S>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) size: usize,
        pub(crate) duration: Duration,
        pub(crate) buffer: Vec<S::Item>,
        pub(crate) timeout: Option<Pin<Box<Sleep>>>,
        pub(crate) last_emit: Option<Instant>,
    }
}

impl<S> Stream for ChunksTimeout<S>
where S: Stream
{
    type Item = Vec<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(to) = this.timeout.as_mut() {
            if to.as_mut().poll(cx).is_ready() {
                *this.timeout = None;
                *this.last_emit = Some(Instant::now());
                if !this.buffer.is_empty() {
                    return Poll::Ready(Some(std::mem::take(this.buffer)));
                }
            }
        }

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.buffer.push(item);
                    if this.buffer.len() >= *this.size {
                        *this.timeout = None;
                        *this.last_emit = Some(Instant::now());
                        return Poll::Ready(Some(std::mem::take(this.buffer)));
                    }
                }
                Poll::Ready(None) => {
                    if this.buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(std::mem::take(this.buffer)));
                    }
                }
                Poll::Pending => {
                    if this.timeout.is_none() {
                        if let Some(last_emit) = this.last_emit {
                            let deadline = *last_emit + *this.duration;
                            *this.timeout = Some(Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))));
                        } else {
                            let deadline = Instant::now() + *this.duration;
                            *this.timeout = Some(Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))));
                        }
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

// ================================
// TakeUntil/SkipUntil Combinators
// ================================

pin_project! {
    pub struct TakeUntil<S, Fut> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) future: Option<Pin<Box<Fut>>>,
        pub(crate) done: bool,
    }
}

impl<S, Fut> Stream for TakeUntil<S, Fut>
where
    S: Stream,
    Fut: Future,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        if let Some(fut) = this.future.as_mut() {
            if fut.as_mut().poll(cx).is_ready() {
                *this.done = true;
                *this.future = None;
                return Poll::Ready(None);
            }
        }

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                *this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    pub struct SkipUntil<S, Fut> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) future: Option<Pin<Box<Fut>>>,
        pub(crate) triggered: bool,
    }
}

impl<S, Fut> Stream for SkipUntil<S, Fut>
where
    S: Stream,
    Fut: Future,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if !*this.triggered {
            if let Some(fut) = this.future.as_mut() {
                if fut.as_mut().poll(cx).is_ready() {
                    *this.triggered = true;
                    *this.future = None;
                } else {
                    // Still skipping, discard items from the stream
                    while let Poll::Ready(Some(_)) = this.stream.as_mut().poll_next(cx) {}
                    return Poll::Pending;
                }
            }
        }

        if *this.triggered {
            this.stream.as_mut().poll_next(cx)
        } else {
            // This should be unreachable if the future is polled correctly
            Poll::Pending
        }
    }
}

// ================================
// Backpressure Combinators
// ================================

pin_project! {
    pub struct Backpressure<S>
    where S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) buffer_size: usize,
        pub(crate) buffer: VecDeque<S::Item>,
        pub(crate) paused: bool,
    }
}

impl<S> Stream for Backpressure<S>
where S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if !this.buffer.is_empty() {
            return Poll::Ready(this.buffer.pop_front());
        }

        if *this.paused {
            return Poll::Pending;
        }

        let res = this.stream.as_mut().poll_next(cx);

        if this.buffer.len() >= *this.buffer_size {
            *this.paused = true;
        }

        res
    }
}

// Extension trait for backpressure control
pub trait BackpressureExt: Stream + Sized {
    fn resume(&mut self);
    fn pause(&mut self);
    fn buffer_size(&self) -> usize;
}

impl<S> BackpressureExt for Backpressure<S>
where S: Stream
{
    fn resume(&mut self) {
        self.paused = false;
    }

    fn pause(&mut self) {
        self.paused = true;
    }

    fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

// ================================
// Extension Traits
// ================================

pub trait SpecializedStreamExt: Stream + Sized {
    fn chunks(self, size: usize) -> Chunks<Self> {
        assert!(size > 0, "chunks: size must be greater than 0");
        Chunks {
            stream: self,
            size,
            buffer: Vec::new(),
        }
    }

    fn chunks_timeout(self, size: usize, duration: Duration) -> ChunksTimeout<Self> {
        assert!(size > 0, "chunks_timeout: size must be greater than 0");
        assert!(!duration.is_zero(), "chunks_timeout: duration must be greater than zero");
        ChunksTimeout {
            stream: self,
            size,
            duration,
            buffer: Vec::new(),
            timeout: None,
            last_emit: None,
        }
    }

    fn take_until<Fut>(self, future: Fut) -> TakeUntil<Self, Fut>
    where Fut: Future
    {
        TakeUntil {
            stream: self,
            future: Some(Box::pin(future)),
            done: false,
        }
    }

    fn skip_until<Fut>(self, future: Fut) -> SkipUntil<Self, Fut>
    where Fut: Future
    {
        SkipUntil {
            stream: self,
            future: Some(Box::pin(future)),
            triggered: false,
        }
    }

    fn backpressure(self, buffer_size: usize) -> Backpressure<Self> {
        assert!(buffer_size > 0, "backpressure: buffer_size must be greater than 0");
        Backpressure {
            stream: self,
            buffer_size,
            buffer: VecDeque::new(),
            paused: false,
        }
    }
}

impl<T> SpecializedStreamExt for T where T: Stream {} 