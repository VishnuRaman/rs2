use rs2_stream::stream::{Stream, StreamExt, AdvancedStreamExt, UtilityStreamExt, from_iter, empty};
use std::pin::Pin;
use std::task::{Context, Poll};
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
async fn test_flat_map() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let stream = stream.flat_map::<i32, _, _>(|x| {
        from_iter(vec![x, x * 10, x * 100])
    });
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 10, 100, 2, 20, 200, 3, 30, 300]);
}

#[tokio::test]
async fn test_flat_map_empty_inner() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let stream = stream.flat_map::<i32, _, _>(|_| empty::<i32>());
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_filter_map() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let stream = StreamExt::filter_map(stream, |x| {
        if x % 2 == 0 {
            Some(x * 2)
        } else {
            None
        }
    });
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![4, 8, 12, 16, 20]);
}

#[tokio::test]
async fn test_filter_map_all_none() {
    let stream = TestStream::new(vec![1, 3, 5, 7, 9]);
    let stream = StreamExt::filter_map(stream, |_| None::<i32>);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_scan() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let stream = stream.scan(0, |state, x| {
        *state += x;
        if *state > 10 {
            None
        } else {
            Some(*state)
        }
    });
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 3, 6, 10]);
}

#[tokio::test]
async fn test_scan_early_termination() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let stream = stream.scan(0, |state, x| {
        *state += x;
        if *state >= 5 {
            None
        } else {
            Some(*state)
        }
    });
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 3]);
}

#[tokio::test]
async fn test_zip() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4, 5]);
    let stream2 = TestStream::new(vec![10, 20, 30, 40, 50]);
    let stream = stream1.zip(stream2);
    
    let result: Vec<(i32, i32)> = stream.collect().await;
    assert_eq!(result, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
}

#[tokio::test]
async fn test_zip_different_lengths() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![10, 20, 30, 40, 50]);
    let stream = stream1.zip(stream2);
    
    let result: Vec<(i32, i32)> = stream.collect().await;
    assert_eq!(result, vec![(1, 10), (2, 20), (3, 30)]);
}

#[tokio::test]
async fn test_zip_empty_streams() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![10, 20, 30]);
    let stream = stream1.zip(stream2);
    let result: Vec<(i32, i32)> = stream.collect().await;
    assert_eq!(result, Vec::<(i32, i32)>::new());
}

#[tokio::test]
async fn test_flatten() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let stream = AdvancedStreamExt::flatten(stream.map(|x| {
        from_iter(vec![x, x * 2, x * 3])
    }));
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 2, 4, 6, 3, 6, 9]);
}

#[tokio::test]
async fn test_flatten_empty_inner() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let stream = AdvancedStreamExt::flatten(stream.map(|_| empty::<i32>()));
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_chain() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let stream = stream1.chain(stream2);
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_chain_empty_first() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let stream = stream1.chain(stream2);
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![4, 5, 6]);
}

#[tokio::test]
async fn test_chain_empty_second() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let stream = stream1.chain(stream2);
    
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_chain_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let stream = stream1.chain(stream2);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_flat_map_with_state() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut counter = 0;
    let stream = stream.flat_map::<i32, _, _>(|x| {
        counter += 1;
        from_iter(vec![x * counter, x * counter * 2])
    });
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 4, 8, 9, 18]);
}

#[tokio::test]
async fn test_filter_map_with_condition() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let stream = StreamExt::filter_map(stream, |x| {
        if x % 2 == 0 {
            Some(format!("even: {}", x))
        } else if x > 5 {
            Some(format!("large: {}", x))
        } else {
            None
        }
    });
    
    let result: Vec<String> = stream.collect().await;
    assert_eq!(result, vec![
        "even: 2".to_string(),
        "even: 4".to_string(),
        "even: 6".to_string(),
        "large: 7".to_string(),
        "even: 8".to_string(),
        "large: 9".to_string(),
        "even: 10".to_string(),
    ]);
}

#[tokio::test]
async fn test_scan_with_string_accumulation() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let stream = stream.scan(String::new(), |state, x| {
        if state.is_empty() {
            *state = x.to_string();
        } else {
            *state = format!("{}, {}", state, x);
        }
        Some(state.clone())
    });
    
    let result: Vec<String> = stream.collect().await;
    assert_eq!(result, vec![
        "1".to_string(),
        "1, 2".to_string(),
        "1, 2, 3".to_string(),
        "1, 2, 3, 4".to_string(),
        "1, 2, 3, 4, 5".to_string(),
    ]);
}

#[tokio::test]
async fn test_zip_with_different_types() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4, 5]);
    let stream2 = from_iter(vec!["a", "b", "c", "d", "e"]);
    let stream = stream1.zip(stream2);
    
    let result: Vec<(i32, &str)> = stream.collect().await;
    assert_eq!(result, vec![(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]);
}

#[tokio::test]
async fn test_flatten_nested_streams() {
    let stream = TestStream::new(vec![1, 2]);
    let stream = AdvancedStreamExt::flatten(AdvancedStreamExt::flatten(stream.map(|x| {
        from_iter(vec![x, x * 2]).map(|y| from_iter(vec![y, y * 5]))
    })));
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 5, 2, 10, 2, 10, 4, 20]);
} 