use rs2_stream::stream::{Stream, StreamExt, SelectStreamExt, AdvancedStreamExt, UtilityStreamExt, from_iter, empty};
use std::pin::Pin;
use std::task::{Context, Poll};

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

impl Unpin for TestStream {}

// Slow test stream for testing select behavior
struct SlowStream {
    values: Vec<i32>,
    index: usize,
    delay_count: usize,
}

impl SlowStream {
    fn new(values: Vec<i32>, _delay_count: usize) -> Self {
        Self { values, index: 0, delay_count: 0 }
    }
}

impl Stream for SlowStream {
    type Item = i32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.delay_count < 2 {
            self.delay_count += 1;
            return Poll::Pending;
        }
        
        if self.index < self.values.len() {
            let value = self.values[self.index];
            self.index += 1;
            Poll::Ready(Some(value))
        } else {
            Poll::Ready(None)
        }
    }
}

impl Unpin for SlowStream {}

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
    assert_eq!(selected, Vec::<i32>::new());
}

#[tokio::test]
async fn test_merge() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    // Check that all items are present (order doesn't matter for merge)
    for v in [1, 2, 3, 4, 5, 6] {
        assert!(merged.contains(&v));
    }
    assert_eq!(merged.len(), 6);
}

#[tokio::test]
async fn test_merge_with_filter() {
    let stream1 = TestStream::new(vec![1, 3, 5, 7, 9]);
    let stream2 = TestStream::new(vec![2, 4, 6, 8, 10]);
    let result: Vec<i32> = stream1
        .merge(stream2)
        .filter(|x| x % 2 == 0)
        .collect()
        .await;
    // Check that all even items are present
    for v in [2, 4, 6, 8, 10] {
        assert!(result.contains(&v));
    }
    assert_eq!(result.len(), 5);
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
    assert_eq!(merged, Vec::<i32>::new());
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
    assert_eq!(fused, Vec::<i32>::new());
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
    assert_eq!(peekable, Vec::<i32>::new());
}

#[tokio::test]
async fn test_select_with_map() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let result: Vec<i32> = stream1
        .select(stream2)
        .map(|x| x * 2)
        .collect()
        .await;
    assert_eq!(result.len(), 6);
    assert!(result.contains(&2));
    assert!(result.contains(&4));
    assert!(result.contains(&6));
    assert!(result.contains(&8));
    assert!(result.contains(&10));
    assert!(result.contains(&12));
}

#[tokio::test]
async fn test_fuse_with_take() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream
        .fuse()
        .take(3)
        .collect()
        .await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_peekable_with_skip() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream
        .peekable()
        .skip(2)
        .collect()
        .await;
    assert_eq!(result, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_select_chain() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![3, 4]);
    let stream3 = TestStream::new(vec![5, 6]);
    let result: Vec<i32> = stream1
        .select(stream2)
        .chain(stream3)
        .collect()
        .await;
    assert_eq!(result.len(), 6);
    assert!(result.contains(&1));
    assert!(result.contains(&2));
    assert!(result.contains(&3));
    assert!(result.contains(&4));
    assert!(result.contains(&5));
    assert!(result.contains(&6));
}

#[tokio::test]
async fn test_merge_zip() {
    let stream1 = SlowStream::new(vec![1, 3, 5], 2);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let stream3 = TestStream::new(vec![10, 20, 30]);
    let result: Vec<(i32, i32)> = stream1
        .merge(stream2)
        .zip(stream3)
        .collect()
        .await;
    // The zipped result should have length 3 and all zipped pairs should have the second element from stream3
    assert_eq!(result.len(), 3);
    let right: Vec<i32> = result.iter().map(|(_, r)| *r).collect();
    let mut right_sorted = right.clone();
    right_sorted.sort();
    assert_eq!(right_sorted, vec![10, 20, 30]);
}

#[tokio::test]
async fn test_complex_select_pipeline() {
    let stream1 = TestStream::new(vec![1, 3, 5, 7, 9]);
    let stream2 = TestStream::new(vec![2, 4, 6, 8, 10]);
    let result: Vec<i32> = stream1
        .select(stream2)
        .filter(|x| x % 2 == 0)  // Keep even numbers
        .map(|x| x * 2)          // Double them
        .take(3)                 // Take first 3
        .collect()
        .await;
    assert_eq!(result, vec![4, 8, 12]); // (2*2), (4*2), (6*2)
}

#[tokio::test]
async fn test_empty_select_pipeline() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let result: Vec<i32> = stream1
        .select(stream2)
        .map(|x| x * 2)
        .filter(|x| x % 2 == 0)
        .take(5)
        .collect()
        .await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_single_item_select_pipeline() {
    let stream1 = TestStream::new(vec![42]);
    let stream2 = TestStream::new(vec![]);
    let result: Vec<i32> = stream1
        .select(stream2)
        .map(|x| x * 2)
        .filter(|x| x % 2 == 0)
        .take(5)
        .collect()
        .await;
    assert_eq!(result, vec![84]);
}

#[tokio::test]
async fn test_select_with_pending() {
    let stream1 = SlowStream::new(vec![1, 2, 3], 2);
    let stream2 = TestStream::new(vec![10, 20, 30]);
    let collected: Vec<i32> = stream1.select(stream2).collect().await;
    
    // Should have items from both streams
    assert!(collected.contains(&1));
    assert!(collected.contains(&10));
    assert!(collected.len() >= 6);
}

#[tokio::test]
async fn test_merge_with_pending() {
    let stream1 = SlowStream::new(vec![1, 2, 3], 1);
    let stream2 = TestStream::new(vec![10, 20, 30]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    // Check that all items are present
    for v in [1, 2, 3, 10, 20, 30] {
        assert!(merged.contains(&v));
    }
    assert_eq!(merged.len(), 6);
}

#[tokio::test]
async fn test_select_three_streams() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![10, 20]);
    let stream3 = TestStream::new(vec![100, 200]);
    
    let collected: Vec<i32> = stream1.select(stream2).select(stream3).collect().await;
    
    // Should have items from all three streams
    assert!(collected.contains(&1));
    assert!(collected.contains(&10));
    assert!(collected.contains(&100));
    assert_eq!(collected.len(), 6);
}

#[tokio::test]
async fn test_merge_three_streams() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![10, 20]);
    let stream3 = TestStream::new(vec![100, 200]);
    let merged: Vec<i32> = stream1.merge(stream2).merge(stream3).collect().await;
    // Check that all items are present
    for v in [1, 2, 10, 20, 100, 200] {
        assert!(merged.contains(&v));
    }
    assert_eq!(merged.len(), 6);
}

#[tokio::test]
async fn test_fuse_with_chain() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let result: Vec<i32> = stream1.chain(stream2).fuse().collect().await;
    
    // Should get all items in order
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_select_with_filter() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4, 5]);
    let stream2 = TestStream::new(vec![10, 20, 30, 40, 50]);
    let collected: Vec<i32> = stream1.select(stream2).filter(|x| x % 2 == 0).collect().await;
    
    // Should only have even numbers from both streams
    for item in &collected {
        assert!(item % 2 == 0);
    }
    assert!(collected.contains(&2));
    assert!(collected.contains(&4));
    assert!(collected.contains(&20));
    assert!(collected.contains(&40));
}

#[tokio::test]
async fn test_fuse_with_peekable() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let result: Vec<i32> = stream.fuse().peekable().collect().await;
    
    // Should get all items in order
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_test_streams_work() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    
    let result1: Vec<i32> = stream1.collect().await;
    let result2: Vec<i32> = stream2.collect().await;
    
    println!("Stream1 result: {:?}", result1);
    println!("Stream2 result: {:?}", result2);
    
    assert_eq!(result1, vec![1, 3, 5]);
    assert_eq!(result2, vec![2, 4, 6]);
} 