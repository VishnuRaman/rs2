//! Utility stream combinators: nth, last, all, any, find, position, count, step_by, inspect, enumerate
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use super::core::Stream;

// Nth
pin_project! {
    pub struct Nth<S> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) n: usize,
        pub(crate) current: usize,
    }
}

impl<S> Future for Nth<S>
where
    S: Stream,
{
    type Output = Option<S::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if *this.current == *this.n {
                        return Poll::Ready(Some(item));
                    } else {
                        *this.current = this.current.saturating_add(1);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Last
pin_project! {
    pub struct Last<S>
    where
        S: Stream
    {
        #[pin]
        pub(crate) stream: S,
        pub(crate) last: Option<S::Item>
    }
}

impl<S> Future for Last<S>
where
    S: Stream
{
    type Output = Option<S::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.last = Some(item);
                }
                Poll::Ready(None) => return Poll::Ready(this.last.take()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// All
pin_project! {
    pub struct All<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F
    }
}

impl<S, F> Future for All<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if !(this.f)(&item) {
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
pin_project! {
    pub struct Any<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F
    }
}

impl<S, F> Future for Any<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if (this.f)(&item) {
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
pin_project! {
    pub struct Find<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F
    }
}

impl<S, F> Future for Find<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = Option<S::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if (this.f)(&item) {
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
pin_project! {
    pub struct Position<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
        pub(crate) pos: usize
    }
}

impl<S, F> Future for Position<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool
{
    type Output = Option<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if (this.f)(&item) {
                        return Poll::Ready(Some(*this.pos));
                    }
                    *this.pos = this.pos.saturating_add(1);
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Count
pin_project! {
    pub struct Count<S> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) count: usize
    }
}

impl<S> Future for Count<S>
where
    S: Stream
{
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(_)) => {
                    *this.count = this.count.saturating_add(1);
                }
                Poll::Ready(None) => return Poll::Ready(*this.count),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// StepBy
pin_project! {
    pub struct StepBy<S> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) step: usize,
        pub(crate) current: usize,
    }
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if *this.current % *this.step == 0 {
                        *this.current = this.current.saturating_add(1);
                        return Poll::Ready(Some(item));
                    } else {
                        *this.current = this.current.saturating_add(1);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Inspect
pin_project! {
    pub struct Inspect<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
    }
}

impl<S, F> Stream for Inspect<S, F>
where
    S: Stream,
    F: FnMut(&S::Item),
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                (this.f)(&item);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Chain
pin_project! {
    pub struct Chain<S1, S2> {
        #[pin]
        pub(crate) first: S1,
        #[pin]
        pub(crate) second: S2,
        pub(crate) first_done: bool,
    }
}

impl<S1, S2> Stream for Chain<S1, S2>
where
    S1: Stream,
    S2: Stream<Item = S1::Item>,
{
    type Item = S1::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Try to get items from the first stream
        if !*this.first_done {
            match this.first.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    *this.first_done = true;
                    // Fall through to try second stream
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // First stream is done, try second stream
        this.second.as_mut().poll_next(cx)
    }
}

// Enumerate
pin_project! {
    pub struct Enumerate<S> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) index: usize,
    }
}

impl<S> Stream for Enumerate<S>
where
    S: Stream,
{
    type Item = (usize, S::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let idx = *this.index;
                *this.index = this.index.saturating_add(1);
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

    fn chain<S2>(self, other: S2) -> Chain<Self, S2>
    where
        S2: Stream<Item = Self::Item>,
    {
        Chain { first: self, second: other, first_done: false }
    }
}

impl<T> UtilityStreamExt for T where T: Stream + Sized {}