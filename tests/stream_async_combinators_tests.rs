use rs2_stream::stream::{Stream, StreamExt};
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
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

#[tokio::test]
async fn test_filter_map_async_large_stream_no_stack_overflow() {
    // This should NOT cause stack overflow after our fix
    let large_data: Vec<i32> = (0..10_000).collect();
    
    let result = from_iter(large_data)
        .filter_map_async_rs2(|x| async move {
            if x % 2 == 0 {
                Some(x * 2)
            } else {
                None  // This will cause the implementation to immediately poll again
            }
        })
        .take_rs2(10) // Only take first 10 to speed up test
        .collect_rs2()
        .await;
    
    assert_eq!(result, vec![0, 4, 8, 12, 16, 20, 24, 28, 32, 36]);
    println!("✅ filter_map_async processed large stream without stack overflow");
}

#[tokio::test]
async fn test_filter_map_async_many_none_results() {
    // Test scenario where many consecutive items return None
    // This was the exact scenario causing stack overflow
    let data: Vec<i32> = (1..5001).step_by(2).collect(); // All odd numbers
    
    let result = from_iter(data)
        .filter_map_async_rs2(|x| async move {
            if x % 2 == 0 {
                Some(x)
            } else {
                None  // All items will return None, causing recursive calls
            }
        })
        .collect_rs2()
        .await;
    
    // Should be empty since all input numbers are odd
    assert_eq!(result, Vec::<i32>::new());
    println!("✅ filter_map_async handled {} None results without stack overflow", 2500);
} 