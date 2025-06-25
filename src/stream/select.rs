//! Select/merge/fuse/peekable combinators
use std::pin::Pin;
use std::task::{Context, Poll};
use super::core::Stream;

// Select - Priority-based (S1 has priority)
pub struct Select<S1, S2> { pub(crate) s1: S1, pub(crate) s2: S2, pub(crate) done1: bool, pub(crate) done2: bool }
impl<S1, S2> Stream for Select<S1, S2>
where
    S1: Stream + Unpin,
    S2: Stream<Item = S1::Item> + Unpin,
{
    type Item = S1::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        
        // Always try S1 first (priority-based)
        if !this.done1 {
            match Pin::new(&mut this.s1).poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => this.done1 = true,
                Poll::Pending => {}
            }
        }
        
        // Only try S2 if S1 is done or pending
        if !this.done2 {
            match Pin::new(&mut this.s2).poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => this.done2 = true,
                Poll::Pending => {}
            }
        }
        
        if this.done1 && this.done2 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

// Merge - Fair racing (no priority)
pub struct Merge<S1, S2> { pub(crate) s1: S1, pub(crate) s2: S2, pub(crate) done1: bool, pub(crate) done2: bool }
impl<S1, S2> Stream for Merge<S1, S2>
where
    S1: Stream + Unpin,
    S2: Stream<Item = S1::Item> + Unpin,
{
    type Item = S1::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        
        // Try both streams fairly (no priority)
        let mut s1_ready = None;
        let mut s2_ready = None;
        
        if !this.done1 {
            match Pin::new(&mut this.s1).poll_next(cx) {
                Poll::Ready(Some(item)) => s1_ready = Some(item),
                Poll::Ready(None) => this.done1 = true,
                Poll::Pending => {}
            }
        }
        
        if !this.done2 {
            match Pin::new(&mut this.s2).poll_next(cx) {
                Poll::Ready(Some(item)) => s2_ready = Some(item),
                Poll::Ready(None) => this.done2 = true,
                Poll::Pending => {}
            }
        }
        
        // Return whichever is ready first (fair racing)
        if let Some(item) = s1_ready {
            return Poll::Ready(Some(item));
        }
        if let Some(item) = s2_ready {
            return Poll::Ready(Some(item));
        }
        
        if this.done1 && this.done2 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

// Fuse
pub struct Fuse<S> { pub(crate) stream: S, pub(crate) done: bool }
impl<S> Stream for Fuse<S>
where S: Stream {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        
        let (stream, done) = unsafe { 
            let this = self.get_unchecked_mut(); 
            (Pin::new_unchecked(&mut this.stream), &mut this.done) 
        };
        
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                *done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Peekable
pub struct Peekable<S> where S: Stream { pub(crate) stream: S, pub(crate) peeked: Option<S::Item> }
impl<S> Stream for Peekable<S>
where S: Stream {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (stream, peeked) = unsafe { 
            let this = self.get_unchecked_mut(); 
            (Pin::new_unchecked(&mut this.stream), &mut this.peeked) 
        };
        
        if let Some(item) = peeked.take() {
            return Poll::Ready(Some(item));
        }
        
        stream.poll_next(cx)
    }
}

impl<S> Peekable<S>
where S: Stream {
    pub fn peek(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<&S::Item>> {
        let (stream, peeked) = unsafe { 
            let this = self.get_unchecked_mut(); 
            (Pin::new_unchecked(&mut this.stream), &mut this.peeked) 
        };
        
        if peeked.is_none() {
            match stream.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *peeked = Some(item);
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        
        Poll::Ready(peeked.as_ref())
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