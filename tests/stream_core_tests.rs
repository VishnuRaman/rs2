use rs2_stream::stream::{Stream, StreamExt, UtilityStreamExt, empty, once, repeat, from_iter};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;
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

#[tokio::test]
async fn test_basic_stream_operations() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
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
    let stream = repeat(42).take(5);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![42, 42, 42, 42, 42]);
}

#[tokio::test]
async fn test_from_iter_stream() {
    let values = vec![1, 2, 3, 4, 5];
    let stream = from_iter(values);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

// #[tokio::test]
// async fn test_pending_stream() {
//     let mut stream = Box::pin(pending::<i32>());
//     // Should always be pending (manual poll required)
// }

#[tokio::test]
async fn test_stream_collect() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_stream_take() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.take(3).collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_stream_skip() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.skip(2).collect().await;
    assert_eq!(result, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_stream_map() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.map(|x| x * 2).collect().await;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_stream_filter() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.filter(|x| x % 2 == 0).collect().await;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_stream_fold() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result = stream.fold(0, |acc, x| async move { acc + x }).await;
    assert_eq!(result, 15);
}

#[tokio::test]
async fn test_stream_for_each() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let mut collected = Vec::new();
    
    stream.for_each(|x| {
        collected.push(x);
        async {}
    }).await;
    
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_stream_any() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let has_even = stream.any(|x| x % 2 == 0).await;
    assert!(has_even);
    
    let stream = TestStream::new(vec![1, 3, 5, 7, 9]);
    let has_even = stream.any(|x| x % 2 == 0).await;
    assert!(!has_even);
}

#[tokio::test]
async fn test_stream_all() {
    let stream = TestStream::new(vec![2, 4, 6, 8, 10]);
    let all_even = stream.all(|x| x % 2 == 0).await;
    assert!(all_even);
    
    let stream = TestStream::new(vec![2, 4, 6, 7, 8]);
    let all_even = stream.all(|x| x % 2 == 0).await;
    assert!(!all_even);
}

#[tokio::test]
async fn test_stream_find() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let found = stream.find(|x| *x == 3).await;
    assert_eq!(found, Some(3));
    
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let found = stream.find(|x| *x == 10).await;
    assert_eq!(found, None);
}

#[tokio::test]
async fn test_stream_position() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let pos = stream.position(|x| *x == 3).await;
    assert_eq!(pos, Some(2));
    
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let pos = stream.position(|x| *x == 10).await;
    assert_eq!(pos, None);
}

#[tokio::test]
async fn test_stream_count() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let count = stream.count().await;
    assert_eq!(count, 5);
    
    let stream = TestStream::new(vec![]);
    let count = stream.count().await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_stream_nth() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let third = stream.nth(2).await;
    assert_eq!(third, Some(3));
    
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let tenth = stream.nth(9).await;
    assert_eq!(tenth, None);
}

#[tokio::test]
async fn test_stream_last() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let last = stream.last().await;
    assert_eq!(last, Some(5));
    
    let stream = TestStream::new(vec![]);
    let last = stream.last().await;
    assert_eq!(last, None);
}

#[tokio::test]
async fn test_stream_step_by() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.step_by(2).collect().await;
    assert_eq!(result, vec![1, 3, 5, 7, 9]);
}

#[tokio::test]
async fn test_stream_inspect() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let mut collected = Vec::new();
    let result: Vec<i32> = stream.inspect(|x| {
        collected.push(*x);
    }).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_stream_enumerate() {
    let stream = TestStream::new(vec![10, 20, 30, 40, 50]);
    let result: Vec<(usize, i32)> = stream.enumerate().collect().await;
    assert_eq!(result, vec![(0, 10), (1, 20), (2, 30), (3, 40), (4, 50)]);
} 