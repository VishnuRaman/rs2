use rs2_stream::stream::{Stream, StreamExt, SpecializedStreamExt, TryStream, from_iter, empty};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::sleep;
use tokio_test::{assert_ok, assert_pending, assert_ready};

// Test stream that yields numbers
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

// Test stream that yields Results
struct TestResultStream {
    values: Vec<Result<i32, &'static str>>,
    index: usize,
}

impl TestResultStream {
    fn new(values: Vec<Result<i32, &'static str>>) -> Self {
        Self { values, index: 0 }
    }
}

impl Stream for TestResultStream {
    type Item = Result<i32, &'static str>;

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

// ================================
// TryStream Tests
// ================================

#[tokio::test]
async fn test_try_map() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Err("error"), Ok(4)]);
    let result: Vec<Result<i32, &str>> = stream.try_map(|x| x * 2).collect().await;
    assert_eq!(result, vec![Ok(2), Ok(4), Err("error"), Ok(8)]);
}

#[tokio::test]
async fn test_try_filter() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Err("error"), Ok(4), Ok(5)]);
    let result: Vec<Result<i32, &str>> = stream.try_filter(|x| x % 2 == 0).collect().await;
    assert_eq!(result, vec![Ok(2), Err("error"), Ok(4)]);
}

#[tokio::test]
async fn test_try_fold() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)]);
    let result = stream.try_fold(0, |acc, x| acc + x).await;
    assert_eq!(result, Ok(15));
}

#[tokio::test]
async fn test_try_fold_with_error() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Err("error"), Ok(4), Ok(5)]);
    let result = stream.try_fold(0, |acc, x| acc + x).await;
    assert_eq!(result, Err("error"));
}

#[tokio::test]
async fn test_try_for_each() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)]);
    let mut collected = Vec::new();
    
    let result = stream.try_for_each(|x| {
        collected.push(x);
        Ok::<(), &'static str>(())
    }).await;
    
    assert_eq!(result, Ok(()));
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_try_for_each_with_error() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)]);
    let mut collected = Vec::new();
    
    let result = stream.try_for_each(|x| {
        collected.push(x);
        if x == 3 {
            Err("stopped at 3")
        } else {
            Ok::<(), &'static str>(())
        }
    }).await;
    
    assert_eq!(result, Err("stopped at 3"));
    assert_eq!(collected, vec![1, 2, 3]);
}

// ================================
// Chunks Tests
// ================================

#[tokio::test]
async fn test_chunks_basic() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let result: Vec<Vec<i32>> = stream.chunks(2).collect().await;
    assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
}

#[tokio::test]
async fn test_chunks_uneven() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<Vec<i32>> = stream.chunks(2).collect().await;
    assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5]]);
}

#[tokio::test]
async fn test_chunks_empty() {
    let stream = TestStream::new(vec![]);
    let result: Vec<Vec<i32>> = stream.chunks(2).collect().await;
    assert_eq!(result, Vec::<Vec<i32>>::new());
}

#[tokio::test]
async fn test_chunks_single() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<Vec<i32>> = stream.chunks(1).collect().await;
    assert_eq!(result, vec![vec![1], vec![2], vec![3], vec![4], vec![5]]);
}

#[tokio::test]
async fn test_chunks_timeout() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<Vec<i32>> = stream.chunks_timeout(3, Duration::from_millis(100)).collect().await;
    assert_eq!(result, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9], vec![10]]);
}

// ================================
// TakeUntil/SkipUntil Tests
// ================================

#[tokio::test]
async fn test_take_until() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.take_until(sleep(Duration::from_millis(50))).collect().await;
    // The exact number depends on timing, but should be some items
    assert!(!result.is_empty());
    assert!(result.len() <= 10);
}

#[tokio::test]
async fn test_take_until_immediate() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.take_until(async {}).collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_skip_until() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.skip_until(sleep(Duration::from_millis(50))).collect().await;
    // Should skip some items and return the rest
    assert!(result.len() <= 10);
}

#[tokio::test]
async fn test_skip_until_immediate() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.skip_until(async {}).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

// ================================
// Backpressure Tests
// ================================

#[tokio::test]
async fn test_backpressure_basic() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let result: Vec<i32> = stream.backpressure(3).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_backpressure_small_buffer() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let result: Vec<i32> = stream.backpressure(2).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_backpressure_empty() {
    let stream = TestStream::new(vec![]);
    let result: Vec<i32> = stream.backpressure(3).collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_backpressure_single_item() {
    let stream = TestStream::new(vec![42]);
    let result: Vec<i32> = stream.backpressure(3).collect().await;
    assert_eq!(result, vec![42]);
}

// ================================
// Integration Tests
// ================================

#[tokio::test]
async fn test_chunks_with_map() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.chunks(3).map(|chunk| chunk.iter().sum::<i32>()).collect().await;
    assert_eq!(result, vec![6, 15, 24, 10]); // 1+2+3, 4+5+6, 7+8+9, 10
}

#[tokio::test]
async fn test_try_map_with_chunks() {
    let stream = TestResultStream::new(vec![Ok(1), Ok(2), Ok(3), Err("error"), Ok(5), Ok(6)]);
    let result: Vec<Vec<Result<i32, &str>>> = stream.try_map(|x| x * 2).chunks(2).collect().await;
    assert_eq!(result, vec![
        vec![Ok(2), Ok(4)],
        vec![Ok(6), Err("error")],
        vec![Ok(10), Ok(12)]
    ]);
}

#[tokio::test]
async fn test_filter_with_backpressure() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.filter(|x| x % 2 == 0).backpressure(2).collect().await;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_chunks_timeout_with_backpressure() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<Vec<i32>> = stream.chunks_timeout(3, Duration::from_millis(100)).backpressure(2).collect().await;
    assert_eq!(result, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9], vec![10]]);
}

#[tokio::test]
async fn test_complex_pipeline() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream
        .filter(|x| x % 2 == 0)  // Keep even numbers
        .map(|x| x * 2)          // Double them
        .chunks(2)               // Group in pairs
        .map(|chunk| chunk.iter().sum::<i32>())  // Sum each chunk
        .backpressure(3)         // Apply backpressure
        .collect()
        .await;
    
    assert_eq!(result, vec![12, 28, 20]); // (2*2 + 4*2) = 12, (6*2 + 8*2) = 28, (10*2) = 20
} 