use rs2_stream::stream::{Stream, StreamExt, UtilityStreamExt, from_iter, empty, once, repeat, pending};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_basic_stream_operations() {
    // Test from_iter
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    
    // Test map
    let mapped = stream.map(|x| x * 2);
    
    // Test filter
    let filtered = mapped.filter(|&x| x > 5);
    
    // Test take
    let taken = filtered.take(2);
    
    // Test collect
    let result: Vec<i32> = taken.collect().await;
    
    assert_eq!(result, vec![6, 8]);
}

#[tokio::test]
async fn test_stream_chain() {
    let result: Vec<i32> = from_iter(1..=10)
        .map(|x| x * 2)
        .filter(|&x| x % 4 == 0)
        .take(3)
        .collect()
        .await;
    
    assert_eq!(result, vec![4, 8, 12]);
}

#[tokio::test]
async fn test_skip_operation() {
    let result: Vec<i32> = from_iter(1..=10)
        .skip(3)
        .take(3)
        .collect()
        .await;
    
    assert_eq!(result, vec![4, 5, 6]);
}

#[tokio::test]
async fn test_enumerate_operation() {
    let result: Vec<(usize, i32)> = from_iter(vec![10, 20, 30])
        .enumerate()
        .collect()
        .await;
    
    assert_eq!(result, vec![(0, 10), (1, 20), (2, 30)]);
}

#[tokio::test]
async fn test_chain_operation() {
    let stream1 = from_iter(vec![1, 2, 3]);
    let stream2 = from_iter(vec![4, 5, 6]);
    
    let result: Vec<i32> = stream1.chain(stream2).collect().await;
    
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_stream_next() {
    let stream = from_iter(vec![1, 2, 3]);
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_empty_stream() {
    let stream = empty::<i32>();
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_once_stream() {
    let stream = once(42);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_repeat_stream() {
    let stream = repeat(5).take(3);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![5, 5, 5]);
}

#[tokio::test]
async fn test_pending_stream() {
    let stream = pending::<i32>();
    // This should not complete immediately
    // We use a timeout to ensure it does not yield
    let result = timeout(Duration::from_millis(100), stream.take(1).collect::<Vec<_>>()).await;
    assert!(result.is_err(), "Pending stream should not yield within timeout");
}

#[tokio::test]
async fn test_complex_stream_chain() {
    let result: Vec<i32> = from_iter(1..=20)
        .filter(|&x| x % 2 == 0)  // Even numbers
        .map(|x| x * 3)           // Multiply by 3
        .skip(2)                  // Skip first 2
        .take(5)                  // Take next 5
        .enumerate()              // Add indices
        .map(|(i, x)| x + i as i32)      // Add index to value
        .collect()
        .await;
    
    // Even numbers: 2,4,6,8,10,12,14,16,18,20
    // After map: 6,12,18,24,30,36,42,48,54,60
    // After skip(2): 18,24,30,36,42,48,54,60
    // After take(5): 18,24,30,36,42
    // After enumerate: (0,18),(1,24),(2,30),(3,36),(4,42)
    // After final map: 18,25,32,39,46
    assert_eq!(result, vec![18, 25, 32, 39, 46]);
}

#[tokio::test]
async fn test_stream_composition() {
    let stream1 = from_iter(vec![1, 2, 3]);
    let stream2 = from_iter(vec![4, 5, 6]);
    let stream3 = from_iter(vec![7, 8, 9]);
    
    let result: Vec<i32> = stream1
        .chain(stream2)
        .chain(stream3)
        .filter(|&x| x % 2 == 0)
        .map(|x| x * 2)
        .collect()
        .await;
    
    // Original: 1,2,3,4,5,6,7,8,9
    // After filter: 2,4,6,8
    // After map: 4,8,12,16
    assert_eq!(result, vec![4, 8, 12, 16]);
}

// Test that our stream types implement the correct traits
#[test]
fn test_stream_trait_implementation() {
    let stream = from_iter(vec![1, 2, 3]);
    
    // This should compile if our Stream trait is correctly implemented
    let _mapped = stream.map(|x| x * 2);
}

// Test that we can use our streams with async/await
#[tokio::test]
async fn test_async_stream_usage() {
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    
    // This should work with async/await
    let result: Vec<i32> = stream
        .map(|x| x * 2)
        .filter(|&x| x > 5)
        .collect()
        .await;
    
    assert_eq!(result, vec![6, 8, 10]);
}

#[tokio::test]
async fn test_stream_performance_chain() {
    // Test a long chain of operations
    let result: Vec<i32> = from_iter(1..=1000)
        .filter(|&x| x % 2 == 0)
        .map(|x| x * 2)
        .filter(|&x| x % 4 == 0)
        .take(10)
        .collect()
        .await;
    
    // Even numbers: 2,4,6,8,10,12,14,16,18,20,...
    // After map: 4,8,12,16,20,24,28,32,36,40,...
    // After filter: 4,8,12,16,20,24,28,32,36,40
    // After take(10): 4,8,12,16,20,24,28,32,36,40
    assert_eq!(result, vec![4, 8, 12, 16, 20, 24, 28, 32, 36, 40]);
} 