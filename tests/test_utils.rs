use rs2_stream::stream::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A simple test stream that implements the custom Stream trait
pub struct TestStream<T> {
    items: VecDeque<T>,
}

impl<T> TestStream<T> {
    pub fn new<I>(items: I) -> Self 
    where 
        I: IntoIterator<Item = T>
    {
        Self {
            items: items.into_iter().collect(),
        }
    }
}

impl<T> Stream for TestStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

/// Helper function to create a test stream from an iterator
pub fn test_stream<T, I>(items: I) -> TestStream<T>
where
    I: IntoIterator<Item = T>
{
    TestStream::new(items)
} 