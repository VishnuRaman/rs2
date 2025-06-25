//! Utility stream combinators: nth, last, all, any, find, position, count, step_by, inspect, enumerate
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use super::core::Stream;

// Nth
pub struct Nth<S> {
    pub(crate) stream: S,
    pub(crate) n: usize,
    pub(crate) current: usize,
}

impl<S> Future for Nth<S>
where
    S: Stream,
{
    type Output = Option<S::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, n, current) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &this.n, &mut this.current)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if *current == *n {
                        return Poll::Ready(Some(item));
                    } else {
                        *current = current.saturating_add(1);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Last
pub struct Last<S>
where
    S: Stream
{
    pub(crate) stream: S,
    pub(crate) last: Option<S::Item>
}

impl<S> Future for Last<S>
where
    S: Stream
{
    type Output = Option<S::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, last) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.last)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *last = Some(item);
                }
                Poll::Ready(None) => return Poll::Ready(last.take()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// All
pub struct All<S, F> {
    pub(crate) stream: S,
    pub(crate) f: F
}

impl<S, F> Future for All<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if !f(&item) {
                        return Poll::Ready(false);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(true),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Any
pub struct Any<S, F> {
    pub(crate) stream: S,
    pub(crate) f: F
}

impl<S, F> Future for Any<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if f(&item) {
                        return Poll::Ready(true);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(false),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Find
pub struct Find<S, F> {
    pub(crate) stream: S,
    pub(crate) f: F
}

impl<S, F> Future for Find<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = Option<S::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if f(&item) {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Position
pub struct Position<S, F> {
    pub(crate) stream: S,
    pub(crate) f: F,
    pub(crate) pos: usize
}

impl<S, F> Future for Position<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = Option<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, f, pos) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f, &mut this.pos)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if f(&item) {
                        return Poll::Ready(Some(*pos));
                    }
                    *pos = pos.saturating_add(1);
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Count
pub struct Count<S> {
    pub(crate) stream: S,
    pub(crate) count: usize
}

impl<S> Future for Count<S>
where
    S: Stream
{
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, count) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.count)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(_)) => {
                    *count = count.saturating_add(1);
                }
                Poll::Ready(None) => return Poll::Ready(*count),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// StepBy
pub struct StepBy<S> {
    pub(crate) stream: S,
    pub(crate) step: usize,
    pub(crate) current: usize,
}

impl<S> StepBy<S> {
    pub fn new(stream: S, step: usize) -> Self {
        assert!(step > 0, "StepBy::new: step parameter must be greater than 0, got {}", step);
        Self { stream, step, current: 0 }
    }
}

impl<S> Stream for StepBy<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, step, current) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &this.step, &mut this.current)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if *current % *step == 0 {
                        *current = current.saturating_add(1);
                        return Poll::Ready(Some(item));
                    } else {
                        *current = current.saturating_add(1);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Inspect
pub struct Inspect<S, F> {
    pub(crate) stream: S,
    pub(crate) f: F,
}

impl<S, F> Stream for Inspect<S, F>
where
    S: Stream,
    F: FnMut(&S::Item),
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };

        match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                f(&item);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Enumerate
pub struct Enumerate<S> {
    pub(crate) stream: S,
    pub(crate) index: usize,
}

impl<S> Stream for Enumerate<S>
where
    S: Stream,
{
    type Item = (usize, S::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, index) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.index)
        };

        match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let idx = *index;
                *index = index.saturating_add(1);
                Poll::Ready(Some((idx, item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Extension trait for these combinators
pub trait UtilityStreamExt: Stream + Sized {
    fn nth(self, n: usize) -> Nth<Self> {
        Nth { stream: self, n, current: 0 }
    }

    fn last(self) -> Last<Self> {
        Last { stream: self, last: None }
    }

    fn all<F>(self, f: F) -> All<Self, F>
    where
        F: FnMut(&Self::Item) -> bool
    {
        All { stream: self, f }
    }

    fn any<F>(self, f: F) -> Any<Self, F>
    where
        F: FnMut(&Self::Item) -> bool
    {
        Any { stream: self, f }
    }

    fn find<F>(self, f: F) -> Find<Self, F>
    where
        F: FnMut(&Self::Item) -> bool
    {
        Find { stream: self, f }
    }

    fn position<F>(self, f: F) -> Position<Self, F>
    where
        F: FnMut(&Self::Item) -> bool
    {
        Position { stream: self, f, pos: 0 }
    }

    fn count(self) -> Count<Self> {
        Count { stream: self, count: 0 }
    }

    fn step_by(self, step: usize) -> StepBy<Self> {
        StepBy::new(self, step)
    }

    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        F: FnMut(&Self::Item),
    {
        Inspect { stream: self, f }
    }

    fn enumerate(self) -> Enumerate<Self> {
        Enumerate { stream: self, index: 0 }
    }

    fn enumerate_from(self, start: usize) -> Enumerate<Self> {
        Enumerate { stream: self, index: start }
    }
}

impl<T> UtilityStreamExt for T where T: Stream + Sized {}