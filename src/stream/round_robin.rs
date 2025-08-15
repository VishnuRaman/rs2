//! Round-robin interleave stream combinator for N streams.
//! No macros, no boxing, no trait objects. True round-robin.

use std::pin::Pin;
use std::task::{Context, Poll};
use crate::stream::Stream;

/// A stream that interleaves multiple streams in round-robin order.
pub struct RoundRobinInterleave<S> {
    streams: Vec<S>,
    index: usize,
}

impl<S> RoundRobinInterleave<S> {
    /// Create a new round-robin interleaver from a vector of streams.
    pub fn new(streams: Vec<S>) -> Self {
        Self { streams, index: 0 }
    }
}

impl<S, O> Stream for RoundRobinInterleave<S>
where
    S: Stream<Item = O> + Unpin,
    O: Send + 'static,
{
    type Item = O;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.streams.is_empty() {
            return Poll::Ready(None);
        }

        // Try to get an item from the current stream
        let current_index = self.index;
        let stream = &mut self.streams[current_index];
        
        match Pin::new(stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Move to next stream for next poll
                self.index = (self.index + 1) % self.streams.len();
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                // Remove exhausted stream
                self.streams.remove(current_index);
                if self.streams.is_empty() {
                    Poll::Ready(None)
                } else {
                    // Adjust index if needed
                    if self.index >= self.streams.len() {
                        self.index = 0;
                    }
                    // Try again with the next stream
                    self.poll_next(cx)
                }
            }
            Poll::Pending => {
                // Move to next stream and try again
                self.index = (self.index + 1) % self.streams.len();
                self.poll_next(cx)
            }
        }
    }
} 