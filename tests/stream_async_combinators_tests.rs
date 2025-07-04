use rs2_stream::stream::{Stream, StreamExt, AsyncStreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::sleep;
use std::sync::{Arc, Mutex};

// ================================
// Test Stream Implementation
// ================================

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
            let item = self.values[self.index];
            self.index += 1;
            Poll::Ready(Some(item))
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

// ================================
// ForEach Tests
// ================================

#[tokio::test]
async fn test_for_each() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let mut collected = Vec::new();
    
    stream.for_each(|item| {
        collected.push(item);
        async {}
    }).await;
    
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_for_each_empty() {
    let stream = TestStream::new(Vec::<i32>::new());
    let mut collected = Vec::new();
    
    stream.for_each(|item| {
        collected.push(item);
        async {}
    }).await;
    
    assert_eq!(collected, Vec::<i32>::new());
}

// ================================
// ForEachWithDelay Tests
// ================================

#[tokio::test]
async fn test_for_each_with_delay() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let collected = Arc::new(Mutex::new(Vec::new()));
    
    stream.for_each(|item| {
        let collected = Arc::clone(&collected);
        async move {
            sleep(Duration::from_millis(10)).await;
            let mut vec = collected.lock().unwrap();
            vec.push(item);
        }
    }).await;
    
    let result = collected.lock().unwrap();
    assert_eq!(*result, vec![1, 2, 3]);
}

// ================================
// Empty Stream Tests
// ================================

#[tokio::test]
async fn test_empty_for_each() {
    let stream = TestStream::new(Vec::<i32>::new());
    let mut collected = Vec::new();
    
    stream.for_each(|item| {
        collected.push(item);
        async {}
    }).await;
    
    assert_eq!(collected, Vec::<i32>::new());
} 