//! Advanced stream combinators: flat_map, zip, scan, fold, filter_map, select, merge
use std::pin::Pin;
use std::task::{Context, Poll};
use super::core::Stream;

// ================================
// FlatMap
// ================================

pub struct FlatMap<S, F, St>
where
    S: Stream,
    F: FnMut(S::Item) -> St,
    St: Stream,
{
    pub(crate) stream: S,
    pub(crate) f: F,
    pub(crate) inner: Option<St>,
}

impl<S, F, St> Stream for FlatMap<S, F, St>
where
    S: Stream,
    F: FnMut(S::Item) -> St,
    St: Stream,
{
    type Item = St::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        loop {
            // First, try to get an item from current inner stream
            if let Some(inner) = &mut this.inner {
                let mut inner_pin = unsafe { Pin::new_unchecked(inner) };
                match inner_pin.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                    Poll::Ready(None) => {
                        this.inner = None;
                        // Continue to get next inner stream
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Get next inner stream from outer stream
            let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.inner = Some((this.f)(item));
                    // Continue loop to poll the new inner stream
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// FilterMap
// ================================

pub struct FilterMap<S, F> {
    pub(crate) stream: S,
    pub(crate) f: F
}

impl<S, U, F> Stream for FilterMap<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Option<U>
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        loop {
            let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if let Some(mapped) = (this.f)(item) {
                        return Poll::Ready(Some(mapped));
                    }
                    // Continue loop if None returned
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Scan
// ================================

pub struct Scan<S, St, F> {
    pub(crate) stream: S,
    pub(crate) state: St,
    pub(crate) f: F
}

impl<S, St, U, F> Stream for Scan<S, St, F>
where
    S: Stream,
    F: FnMut(&mut St, S::Item) -> Option<U>
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        loop {
            let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if let Some(output) = (this.f)(&mut this.state, item) {
                        return Poll::Ready(Some(output));
                    }
                    // Continue loop if None returned
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Zip - Fixed Implementation
// ================================

pub struct Zip<S1, S2>
where
    S1: Stream,
    S2: Stream,
{
    pub(crate) s1: S1,
    pub(crate) s2: S2,
    pub(crate) item1: Option<S1::Item>,
    pub(crate) item2: Option<S2::Item>,
}

impl<S1, S2> Stream for Zip<S1, S2>
where
    S1: Stream,
    S2: Stream
{
    type Item = (S1::Item, S2::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        // Poll first stream if we don't have an item
        if this.item1.is_none() {
            let mut s1 = unsafe { Pin::new_unchecked(&mut this.s1) };
            match s1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => this.item1 = Some(item),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
        }

        // Poll second stream if we don't have an item
        if this.item2.is_none() {
            let mut s2 = unsafe { Pin::new_unchecked(&mut this.s2) };
            match s2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => this.item2 = Some(item),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
        }

        // If we have both items, return them
        if let (Some(item1), Some(item2)) = (this.item1.take(), this.item2.take()) {
            Poll::Ready(Some((item1, item2)))
        } else {
            Poll::Pending
        }
    }
}

// ================================
// Flatten
// ================================

pub struct Flatten<S>
where
    S: Stream,
    S::Item: Stream,
{
    pub(crate) stream: S,
    pub(crate) inner: Option<S::Item>,
}

impl<S> Stream for Flatten<S>
where
    S: Stream,
    S::Item: Stream,
{
    type Item = <S::Item as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        loop {
            // Try to get item from current inner stream
            if let Some(inner) = &mut this.inner {
                let mut inner_pin = unsafe { Pin::new_unchecked(inner) };
                match inner_pin.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                    Poll::Ready(None) => this.inner = None,
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Get next inner stream
            let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(inner_stream)) => {
                    this.inner = Some(inner_stream);
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Extension Trait (Much Cleaner!)
// ================================

pub trait AdvancedStreamExt: Stream + Sized {
    /// Transform each element into a stream and flatten the results
    fn flat_map<U, St, F>(self, f: F) -> FlatMap<Self, F, St>
    where
        St: Stream,
        F: FnMut(Self::Item) -> St,
    {
        FlatMap { stream: self, f, inner: None }
    }

    /// Filter and map elements in one operation
    fn filter_map<U, F>(self, f: F) -> FilterMap<Self, F>
    where
        F: FnMut(Self::Item) -> Option<U>,
    {
        FilterMap { stream: self, f }
    }

    /// Stateful mapping with an accumulator
    fn scan<St, U, F>(self, initial_state: St, f: F) -> Scan<Self, St, F>
    where
        F: FnMut(&mut St, Self::Item) -> Option<U>,
    {
        Scan { stream: self, state: initial_state, f }
    }

    /// Combine two streams element by element
    fn zip<S2>(self, other: S2) -> Zip<Self, S2>
    where
        S2: Stream,
    {
        Zip {
            s1: self,
            s2: other,
            item1: None,
            item2: None,
        }
    }

    /// Flatten a stream of streams
    fn flatten(self) -> Flatten<Self>
    where
        Self::Item: Stream,
    {
        Flatten { stream: self, inner: None }
    }
}

// ================================
// Automatic Implementation
// ================================

impl<T: Stream + Sized> AdvancedStreamExt for T {}