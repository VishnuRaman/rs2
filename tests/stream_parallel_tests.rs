use std::time::Duration;
use tokio::time::sleep;
use rs2_stream::stream::{StreamExt, from_iter, ParallelStreamExt};
use rs2_stream::rs2_stream_ext::RS2StreamExt;

#[tokio::test]
async fn test_par_eval_map_preserves_order() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move {
            // Add delay to ensure concurrency is working
            sleep(Duration::from_millis(10)).await;
            Ok::<_, rs2_stream::error::StreamError>(x * 2)
        })
        .collect()
        .await;
    
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_all_results() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(2, |x| async move {
            // Add delay to ensure concurrency is working
            sleep(Duration::from_millis(10)).await;
            Ok::<_, rs2_stream::error::StreamError>(x * 2)
        })
        .collect()
        .await;
    let mut result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    result.sort();
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_par_eval_map_concurrency_1() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(1, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x + 10) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, vec![11, 12, 13]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_concurrency_1() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(1, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x + 10) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, vec![11, 12, 13]);
}

#[tokio::test]
async fn test_par_eval_map_empty_stream() {
    let data: Vec<i32> = vec![];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x * 2) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_par_eval_map_unordered_empty_stream() {
    let data: Vec<i32> = vec![];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(2, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x * 2) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_par_eval_map_single_item() {
    let data = vec![42];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(3, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x.to_string()) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, vec!["42".to_string()]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_single_item() {
    let data = vec![42];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(3, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x.to_string()) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(result, vec!["42".to_string()]);
}

#[tokio::test]
async fn test_par_eval_map_high_concurrency() {
    let data: Vec<i32> = (0..100).collect();
    let stream = from_iter(data.clone());
    
    let result: Vec<_> = stream
        .par_eval_map(10, |x| async move {
            sleep(Duration::from_millis(1)).await;
            Ok::<_, rs2_stream::error::StreamError>(x * 2)
        })
        .collect()
        .await;
    
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    let expected: Vec<_> = data.iter().map(|x| x * 2).collect();
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_par_eval_map_unordered_high_concurrency() {
    let data: Vec<i32> = (0..100).collect();
    let stream = from_iter(data.clone());
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(10, |x| async move {
            sleep(Duration::from_millis(1)).await;
            Ok::<_, rs2_stream::error::StreamError>(x * 2)
        })
        .collect()
        .await;
    let mut result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    let mut expected: Vec<_> = data.iter().map(|x| x * 2).collect();
    result.sort();
    expected.sort();
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_par_eval_map_with_different_delays() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(3, |x| async move {
            // Different delays to test ordering
            let delay = if x % 2 == 0 { 20 } else { 5 };
            sleep(Duration::from_millis(delay)).await;
            Ok::<_, rs2_stream::error::StreamError>(x * 10)
        })
        .collect()
        .await;
    
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    // Should maintain order despite different processing times
    assert_eq!(result, vec![10, 20, 30, 40, 50]);
}

#[tokio::test]
async fn test_par_eval_map_error_handling() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move {
            if x == 3 {
                Err(rs2_stream::error::StreamError::Custom(format!("Error on {}", x)))
            } else {
                Ok(x * 2)
            }
        })
        .collect()
        .await;
    
    let expected: Vec<_> = vec![
        Ok(2),
        Ok(4),
        Err(rs2_stream::error::StreamError::Custom("Error on 3".to_string())),
        Ok(8),
        Ok(10)
    ];
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_par_eval_map_with_string_processing() {
    let data = vec!["hello", "world", "rust", "stream"];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |s| async move {
            sleep(Duration::from_millis(5)).await;
            Ok::<_, rs2_stream::error::StreamError>(s.to_uppercase())
        })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    
    assert_eq!(result, vec!["HELLO", "WORLD", "RUST", "STREAM"]);
}

#[tokio::test]
async fn test_par_eval_map_chaining() {
    let data = vec![1, 2, 3, 4];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x * 2) })
        .par_eval_map(2, |x| async move {
            let x = x.unwrap();
            Ok::<_, rs2_stream::error::StreamError>(x + 1)
        })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    
    assert_eq!(result, vec![3, 5, 7, 9]);
}

#[tokio::test] 
async fn test_par_eval_map_zero_concurrency() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    
    // Concurrency of 0 should still work (treat as 1)
    let result: Vec<_> = stream
        .par_eval_map(0, |x| async move { Ok::<_, rs2_stream::error::StreamError>(x * 2) })
        .collect()
        .await;
    let result: Vec<_> = result.into_iter().map(|r| r.unwrap()).collect();
    
    // Should handle gracefully (likely sequential processing)
    assert_eq!(result.len(), 3);
}

#[tokio::test]
async fn test_performance_comparison() {
    let data: Vec<i32> = (0..50).collect();
    
    // Sequential processing
    let start = std::time::Instant::now();
    let stream = from_iter(data.clone());
    let _sequential: Vec<_> = stream
        .map(|x| async move {
            sleep(Duration::from_millis(10)).await;
            x * 2
        })
        .then(|fut| fut)
        .collect()
        .await;
    let sequential_time = start.elapsed();
    
    // Parallel processing
    let start = std::time::Instant::now();
    let stream = from_iter(data);
    let _parallel: Vec<_> = stream
        .par_eval_map(5, |x| async move {
            sleep(Duration::from_millis(10)).await;
            Ok::<_, rs2_stream::error::StreamError>(x * 2)
        })
        .collect()
        .await;
    let parallel_time = start.elapsed();
    
    // Parallel should be significantly faster for this workload
    println!("Sequential: {:?}, Parallel: {:?}", sequential_time, parallel_time);
    // Note: This is more of a benchmark than assertion, but parallel should be faster
}

#[tokio::test]
async fn test_par_join_basic() {
    // Create multiple streams with different data
    let stream1 = from_iter(vec![1, 2, 3]).map(|x| async move {
        sleep(Duration::from_millis(50)).await;
        format!("stream1-{}", x)
    }).then(|f| f);
    
    let stream2 = from_iter(vec![4, 5, 6]).map(|x| async move {
        sleep(Duration::from_millis(30)).await;
        format!("stream2-{}", x)
    }).then(|f| f);
    
    let stream3 = from_iter(vec![7, 8]).map(|x| async move {
        sleep(Duration::from_millis(20)).await;
        format!("stream3-{}", x)
    }).then(|f| f);
    
    // Create a stream of streams using boxed streams for type compatibility
    let streams = from_iter(vec![
        Box::new(stream1) as Box<dyn rs2_stream::stream::Stream<Item = String> + Send>,
        Box::new(stream2) as Box<dyn rs2_stream::stream::Stream<Item = String> + Send>,
        Box::new(stream3) as Box<dyn rs2_stream::stream::Stream<Item = String> + Send>,
    ]);
    
    // Process all streams with max 2 concurrent streams
    let results: Vec<String> = streams
        .par_join_rs2(2)
        .collect()
        .await;
    
    // Should get all items from all streams (8 total items)
    assert_eq!(results.len(), 8);
    
    // Verify we have items from all streams
    let stream1_count = results.iter().filter(|s| s.starts_with("stream1")).count();
    let stream2_count = results.iter().filter(|s| s.starts_with("stream2")).count();
    let stream3_count = results.iter().filter(|s| s.starts_with("stream3")).count();
    
    assert_eq!(stream1_count, 3);
    assert_eq!(stream2_count, 3);
    assert_eq!(stream3_count, 2);
}

#[tokio::test]
async fn test_par_join_concurrency_limit() {
    let start = std::time::Instant::now();
    
    // Create a stream that yields 5 streams, each with a delay
    let stream = from_iter(1..=5).map(|i| {
        let stream = from_iter(vec![i]).map(move |x| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            x
        }).then(|f| f);
        Box::new(stream) as Box<dyn rs2_stream::stream::Stream<Item = i32> + Send>
    });

    // Use par_join with concurrency limit of 2
    let result: Vec<_> = stream
        .par_join_rs2(2)
        .collect()
        .await;

    let duration = start.elapsed();
    println!("Completed concurrency limit test in {:?} with {} results", duration, result.len());
    
    // Should have 5 results
    assert_eq!(result.len(), 5);
    
    // Results should be 1, 2, 3, 4, 5 (order may vary due to concurrency)
    let mut sorted_result = result.clone();
    sorted_result.sort();
    assert_eq!(sorted_result, vec![1, 2, 3, 4, 5]);
    
    // Should complete faster than sequential (5 * 100ms = 500ms)
    // but slower than fully parallel (100ms)
    assert!(duration < Duration::from_millis(600));
    assert!(duration > Duration::from_millis(100));
}

#[tokio::test]
async fn test_par_join_empty_streams() {
    // Mix of empty and non-empty streams
    let stream1 = from_iter(vec![1, 2]);
    let stream2 = from_iter(vec![]); // Empty stream
    let stream3 = from_iter(vec![3]);
    
    let streams = from_iter(vec![
        Box::new(stream1) as Box<dyn rs2_stream::stream::Stream<Item = i32> + Send>,
        Box::new(stream2) as Box<dyn rs2_stream::stream::Stream<Item = i32> + Send>,
        Box::new(stream3) as Box<dyn rs2_stream::stream::Stream<Item = i32> + Send>,
    ]);
    
    let results: Vec<i32> = streams
        .par_join_rs2(3)
        .collect()
        .await;
    
    // Should only get items from non-empty streams
    assert_eq!(results.len(), 3);
    let mut sorted_results = results.clone();
    sorted_results.sort();
    assert_eq!(sorted_results, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_par_join_single_stream() {
    // Test with just one stream
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let streams = from_iter(vec![
        Box::new(stream) as Box<dyn rs2_stream::stream::Stream<Item = i32> + Send>
    ]);
    
    let results: Vec<i32> = streams
        .par_join_rs2(10) // High concurrency limit
        .collect()
        .await;
    
    assert_eq!(results, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_par_join_no_streams() {
    // Test with empty stream of streams
    let streams: Vec<Box<dyn rs2_stream::stream::Stream<Item = i32> + Send>> = vec![];
    let stream_of_streams = from_iter(streams);
    
    let results: Vec<i32> = stream_of_streams
        .par_join_rs2(5)
        .collect()
        .await;
    
    assert_eq!(results, Vec::<i32>::new());
} 

#[tokio::test]
async fn test_par_join_debug() {
    // Simple test with just one stream to debug the issue
    let stream = from_iter(vec![1]).map(|x| async move {
        sleep(Duration::from_millis(10)).await;
        format!("item-{}", x)
    }).then(|f| f);
    
    let streams = from_iter(vec![
        Box::new(stream) as Box<dyn rs2_stream::stream::Stream<Item = String> + Send>
    ]);
    
    println!("Starting par_join test...");
    let results: Vec<String> = streams
        .par_join_rs2(1)
        .collect()
        .await;
    println!("Completed par_join test with {} results", results.len());
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "item-1");
} 