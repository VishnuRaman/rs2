use rs2_stream::stream::{Stream, StreamExt, ConstructorStreamExt, UtilityStreamExt, empty, once, repeat, from_iter, pending, repeat_with, once_with, unfold};
use rs2_stream::stream::AdvancedStreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{timeout, Duration};
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

#[tokio::test]
async fn test_empty() {
    let result: Vec<i32> = empty::<i32>().collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_once() {
    let result: Vec<i32> = once(42).collect().await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_repeat() {
    let result: Vec<i32> = repeat(42).take(5).collect().await;
    assert_eq!(result, vec![42, 42, 42, 42, 42]);
}

#[tokio::test]
async fn test_from_iter() {
    let result: Vec<i32> = from_iter(vec![1, 2, 3, 4, 5]).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_pending() {
    // Pending stream should never yield any values, so it should timeout
    let result = timeout(
        Duration::from_millis(100),
        pending::<i32>().take(5).collect::<Vec<i32>>()
    ).await;
    
    // Should timeout, which is expected behavior for a pending stream
    assert!(result.is_err());
}

#[tokio::test]
async fn test_repeat_with() {
    let mut counter = 0;
    let result: Vec<i32> = repeat_with(|| {
        counter += 1;
        counter
    }).take(5).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_once_with() {
    let result: Vec<i32> = once_with(|| 42).collect().await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_unfold() {
    let result: Vec<i32> = unfold(0, |mut state| async move {
        if state < 5 {
            let value = state;
            state += 1;
            Some((value, state))
        } else {
            None
        }
    }).collect().await;
    assert_eq!(result, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn test_unfold_with_string() {
    let result: Vec<String> = unfold("hello".to_string(), |mut state| async move {
        if state.len() > 0 {
            let value = state.clone();
            state = state[1..].to_string();
            Some((value, state))
        } else {
            None
        }
    }).collect().await;
    assert_eq!(result, vec!["hello", "ello", "llo", "lo", "o"]);
}

#[tokio::test]
async fn test_chain_constructors() {
    let result: Vec<i32> = once(1)
        .chain(from_iter(vec![2, 3]))
        .chain(repeat(4).take(2))
        .collect()
        .await;
    assert_eq!(result, vec![1, 2, 3, 4, 4]);
}

#[tokio::test]
async fn test_map_constructors() {
    let result: Vec<i32> = from_iter(vec![1, 2, 3])
        .map(|x| x * 2)
        .collect()
        .await;
    assert_eq!(result, vec![2, 4, 6]);
}

#[tokio::test]
async fn test_filter_constructors() {
    let result: Vec<i32> = from_iter(vec![1, 2, 3, 4, 5, 6])
        .filter(|x| x % 2 == 0)
        .collect()
        .await;
    assert_eq!(result, vec![2, 4, 6]);
}

#[tokio::test]
async fn test_zip_constructors() {
    let result: Vec<(i32, i32)> = from_iter(vec![1, 2, 3])
        .zip(from_iter(vec![10, 20, 30]))
        .collect()
        .await;
    assert_eq!(result, vec![(1, 10), (2, 20), (3, 30)]);
}

#[tokio::test]
async fn test_take_constructors() {
    let result: Vec<i32> = from_iter(vec![1, 2, 3, 4, 5])
        .take(3)
        .collect()
        .await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_skip_constructors() {
    let result: Vec<i32> = from_iter(vec![1, 2, 3, 4, 5])
        .skip(2)
        .collect()
        .await;
    assert_eq!(result, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_complex_pipeline() {
    let result: Vec<i32> = from_iter(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .filter(|x| x % 2 == 0)  // Keep even numbers
        .map(|x| x * 2)          // Double them
        .take(3)                 // Take first 3
        .collect()
        .await;
    assert_eq!(result, vec![4, 8, 12]); // (2*2), (4*2), (6*2)
}

#[tokio::test]
async fn test_empty_chain() {
    let result: Vec<i32> = empty::<i32>()
        .chain(empty::<i32>())
        .collect()
        .await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_once_chain_empty() {
    let result: Vec<i32> = once(42)
        .chain(empty::<i32>())
        .collect()
        .await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_empty_chain_once() {
    let result: Vec<i32> = empty::<i32>()
        .chain(once(42))
        .collect()
        .await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_repeat_with_condition() {
    let mut counter = 0;
    let result: Vec<i32> = repeat_with(|| {
        counter += 1;
        counter
    })
    .take_while(|&x| x <= 5)
    .collect()
    .await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_unfold_fibonacci() {
    let result: Vec<i32> = unfold((0, 1), |(a, b)| async move {
        if a > 100 {
            None
        } else {
            Some((a, (b, a + b)))
        }
    }).collect().await;
    assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]);
}

#[tokio::test]
async fn test_skip_while() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.skip_while(|x| *x < 5).collect().await;
    assert_eq!(result, vec![5, 6, 7, 8, 9, 10]);
}

#[tokio::test]
async fn test_skip_while_all_skipped() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.skip_while(|x| *x < 10).collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_skip_while_none_skipped() {
    let stream = TestStream::new(vec![5, 6, 7, 8, 9]);
    let result: Vec<i32> = stream.skip_while(|x| *x < 3).collect().await;
    assert_eq!(result, vec![5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn test_take_while() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.take_while(|x| *x < 5).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_take_while_all_taken() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.take_while(|x| *x < 10).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_take_while_none_taken() {
    let stream = TestStream::new(vec![5, 6, 7, 8, 9]);
    let result: Vec<i32> = stream.take_while(|x| *x < 3).collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_skip_while_with_state() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let mut sum = 0;
    let result: Vec<i32> = stream.skip_while(|x| {
        sum += x;
        sum < 10
    }).collect().await;
    // Should skip until sum >= 10 (1+2+3+4 = 10, so starts from 4)
    assert_eq!(result, vec![4, 5, 6, 7, 8, 9, 10]);
}

#[tokio::test]
async fn test_take_while_with_state() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let mut sum = 0;
    let result: Vec<i32> = stream.take_while(|x| {
        sum += x;
        sum <= 10
    }).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_repeat_with_random_like() {
    let mut seed = 42u32;
    let result: Vec<u32> = repeat_with(|| {
        seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
        seed
    }).take(5).collect().await;
    assert_eq!(result.len(), 5);
    assert_ne!(result[0], result[1]);
    assert_ne!(result[1], result[2]);
    assert_ne!(result[2], result[3]);
    assert_ne!(result[3], result[4]);
}

#[tokio::test]
async fn test_once_with_side_effects() {
    let mut side_effect = 0;
    let result: Vec<i32> = once_with(|| {
        side_effect += 100;
        side_effect
    }).collect().await;
    assert_eq!(result, vec![100]);
    assert_eq!(side_effect, 100);
}

#[tokio::test]
async fn test_unfold_early_termination() {
    let result: Vec<i32> = unfold(0, |mut state| async move {
        if state == 3 {
            None // Early termination
        } else {
            let value = state * state;
            state += 1;
            Some((value, state))
        }
    }).collect().await;
    assert_eq!(result, vec![0, 1, 4]);
}

#[tokio::test]
async fn test_skip_while_empty_stream() {
    let stream = TestStream::new(vec![]);
    let result: Vec<i32> = stream.skip_while(|_| true).collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_take_while_empty_stream() {
    let stream = TestStream::new(vec![]);
    let result: Vec<i32> = stream.take_while(|_| true).collect().await;
    assert_eq!(result, Vec::<i32>::new());
} 