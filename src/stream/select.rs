//! Select/merge/fuse/peekable combinators
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use super::core::Stream;

// Select - Priority-based (S1 has priority)
pin_project! {
    pub struct Select<S1, S2> { 
        #[pin] pub(crate) s1: S1, 
        #[pin] pub(crate) s2: S2, 
        pub(crate) done1: bool, 
        pub(crate) done2: bool 
    }
}

impl<S1, S2> Stream for Select<S1, S2>
where
    S1: Stream,
    S2: Stream<Item = S1::Item>,
{
    type Item = S1::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // Always try S1 first (priority-based)
        if !*this.done1 {
            match this.s1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => *this.done1 = true,
                Poll::Pending => {}
            }
        }
        
        // Only try S2 if S1 is done or pending
        if !*this.done2 {
            match this.s2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => *this.done2 = true,
                Poll::Pending => {}
            }
        }
        
        if *this.done1 && *this.done2 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

// Merge - Fair racing (no priority) - Fixed implementation
pin_project! {
    pub struct Merge<S1, S2> { 
        #[pin] pub(crate) s1: S1, 
        #[pin] pub(crate) s2: S2, 
        pub(crate) done1: bool, 
        pub(crate) done2: bool 
    }
}

impl<S1, S2> Stream for Merge<S1, S2>
where
    S1: Stream,
    S2: Stream<Item = S1::Item>,
{
    type Item = S1::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // Check S1 first
        if !*this.done1 {
            match this.s1.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => *this.done1 = true,
                Poll::Pending => {}
            }
        }
        
        // Check S2 
        if !*this.done2 {
            match this.s2.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => *this.done2 = true,
                Poll::Pending => {}
            }
        }
        
        // If both streams are done, we're done
        if *this.done1 && *this.done2 {
            Poll::Ready(None)
        } else {
            // If either stream is not done but neither is ready, we're pending
            Poll::Pending
        }
    }
}

// Fuse
pin_project! {
    pub struct Fuse<S> { 
        #[pin] pub(crate) stream: S, 
        pub(crate) done: bool 
    }
}

impl<S> Stream for Fuse<S>
where S: Stream {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        if *this.done {
            return Poll::Ready(None);
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

// Peekable
pin_project! {
    pub struct Peekable<S> 
    where S: Stream 
    { 
        #[pin] pub(crate) stream: S, 
        pub(crate) peeked: Option<S::Item> 
    }
}

impl<S> Stream for Peekable<S>
where S: Stream {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        if let Some(item) = this.peeked.take() {
            return Poll::Ready(Some(item));
        }
        
        this.stream.as_mut().poll_next(cx)
    }
}

impl<S> Peekable<S>
where S: Stream {
    pub fn peek(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<&S::Item>> {
        let mut this = self.project();
        
        if this.peeked.is_none() {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.peeked = Some(item);
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        
        Poll::Ready(this.peeked.as_ref())
    }
}

// StreamExt trait for these
pub trait SelectStreamExt: Stream + Sized {
    fn select<S2>(self, other: S2) -> Select<Self, S2> where S2: Stream<Item = Self::Item> { Select { s1: self, s2: other, done1: false, done2: false } }
    fn merge<S2>(self, other: S2) -> Merge<Self, S2> where S2: Stream<Item = Self::Item> { Merge { s1: self, s2: other, done1: false, done2: false } }
    fn fuse(self) -> Fuse<Self> { Fuse { stream: self, done: false } }
    fn peekable(self) -> Peekable<Self> { Peekable { stream: self, peeked: None } }
}

impl<T> SelectStreamExt for T where T: Stream {} 