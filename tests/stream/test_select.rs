use rs2_stream::stream::{Stream, StreamExt, select::*};
use std::pin::Pin;
use std::task::{Context, Poll};

struct TestStream {
    values: Vec<i32>,
    index: usize,
}

impl TestStream {
    fn new(values: Vec<i32>) -> Self {
        Self { values, index: 0 }
    }
}

impl Stream for TestStream {
    type Item = i32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.values.len() {
            let value = self.values[self.index];
            self.index += 1;
            Poll::Ready(Some(value))
        } else {
            Poll::Ready(None)
        }
    }
}

impl Clone for TestStream {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            index: 0,
        }
    }
}

#[tokio::test]
async fn test_select() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let selected: Vec<i32> = stream1.select(stream2).collect().await;
    assert_eq!(selected.len(), 6);
    assert!(selected.contains(&1));
    assert!(selected.contains(&2));
    assert!(selected.contains(&3));
    assert!(selected.contains(&4));
    assert!(selected.contains(&5));
    assert!(selected.contains(&6));
}

#[tokio::test]
async fn test_select_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let selected: Vec<i32> = stream1.select(stream2).collect().await;
    assert_eq!(selected, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_select_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let selected: Vec<i32> = stream1.select(stream2).collect().await;
    assert_eq!(selected, vec![]);
}

#[tokio::test]
async fn test_merge() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    assert_eq!(merged.len(), 6);
    assert!(merged.contains(&1));
    assert!(merged.contains(&2));
    assert!(merged.contains(&3));
    assert!(merged.contains(&4));
    assert!(merged.contains(&5));
    assert!(merged.contains(&6));
}

#[tokio::test]
async fn test_merge_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    assert_eq!(merged, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_merge_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    assert_eq!(merged, vec![]);
}

#[tokio::test]
async fn test_fuse() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let fused: Vec<i32> = stream.fuse().collect().await;
    assert_eq!(fused, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_fuse_empty() {
    let stream = TestStream::new(vec![]);
    let fused: Vec<i32> = stream.fuse().collect().await;
    assert_eq!(fused, vec![]);
}

#[tokio::test]
async fn test_peekable() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let peekable: Vec<i32> = stream.peekable().collect().await;
    assert_eq!(peekable, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_peekable_empty() {
    let stream = TestStream::new(vec![]);
    let peekable: Vec<i32> = stream.peekable().collect().await;
    assert_eq!(peekable, vec![]);
} 