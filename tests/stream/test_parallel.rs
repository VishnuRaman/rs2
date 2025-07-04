use std::time::Duration;
use tokio::time::sleep;
use rs2_stream::stream::{Stream, StreamExt, ParallelStreamExt, from_iter};

#[tokio::test]
async fn test_par_eval_map_preserves_order() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move {
            // Add delay to ensure concurrency is working
            sleep(Duration::from_millis(10)).await;
            x * 2
        })
        .collect()
        .await;
    
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_all_results() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    
    let mut result: Vec<_> = stream
        .par_eval_map_unordered(2, |x| async move {
            // Add delay to ensure concurrency is working
            sleep(Duration::from_millis(10)).await;
            x * 2
        })
        .collect()
        .await;
    
    // Sort since order is not guaranteed
    result.sort();
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_par_eval_map_concurrency_1() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(1, |x| async move { x + 10 })
        .collect()
        .await;
    
    assert_eq!(result, vec![11, 12, 13]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_concurrency_1() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(1, |x| async move { x + 10 })
        .collect()
        .await;
    
    assert_eq!(result, vec![11, 12, 13]);
}

#[tokio::test]
async fn test_par_eval_map_empty_stream() {
    let data: Vec<i32> = vec![];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move { x * 2 })
        .collect()
        .await;
    
    assert_eq!(result, vec![]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_empty_stream() {
    let data: Vec<i32> = vec![];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(2, |x| async move { x * 2 })
        .collect()
        .await;
    
    assert_eq!(result, vec![]);
}

#[tokio::test]
async fn test_par_eval_map_single_item() {
    let data = vec![42];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(3, |x| async move { x.to_string() })
        .collect()
        .await;
    
    assert_eq!(result, vec!["42".to_string()]);
}

#[tokio::test]
async fn test_par_eval_map_unordered_single_item() {
    let data = vec![42];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map_unordered(3, |x| async move { x.to_string() })
        .collect()
        .await;
    
    assert_eq!(result, vec!["42".to_string()]);
}

#[tokio::test]
async fn test_par_eval_map_high_concurrency() {
    let data: Vec<i32> = (0..100).collect();
    let stream = from_iter(data.clone());
    
    let result: Vec<_> = stream
        .par_eval_map(10, |x| async move {
            sleep(Duration::from_millis(1)).await;
            x * 2
        })
        .collect()
        .await;
    
    let expected: Vec<_> = data.iter().map(|x| x * 2).collect();
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_par_eval_map_unordered_high_concurrency() {
    let data: Vec<i32> = (0..100).collect();
    let stream = from_iter(data.clone());
    
    let mut result: Vec<_> = stream
        .par_eval_map_unordered(10, |x| async move {
            sleep(Duration::from_millis(1)).await;
            x * 2
        })
        .collect()
        .await;
    
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
            x * 10
        })
        .collect()
        .await;
    
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
                Err(format!("Error on {}", x))
            } else {
                Ok(x * 2)
            }
        })
        .collect()
        .await;
    
    assert_eq!(result, vec![
        Ok(2), 
        Ok(4), 
        Err("Error on 3".to_string()), 
        Ok(8), 
        Ok(10)
    ]);
}

#[tokio::test]
async fn test_par_eval_map_with_string_processing() {
    let data = vec!["hello", "world", "rust", "stream"];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |s| async move {
            sleep(Duration::from_millis(5)).await;
            s.to_uppercase()
        })
        .collect()
        .await;
    
    assert_eq!(result, vec!["HELLO", "WORLD", "RUST", "STREAM"]);
}

#[tokio::test]
async fn test_par_eval_map_chaining() {
    let data = vec![1, 2, 3, 4];
    let stream = from_iter(data);
    
    let result: Vec<_> = stream
        .par_eval_map(2, |x| async move { x * 2 })
        .par_eval_map(2, |x| async move { x + 1 })
        .collect()
        .await;
    
    assert_eq!(result, vec![3, 5, 7, 9]);
}

#[tokio::test] 
async fn test_par_eval_map_zero_concurrency() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    
    // Concurrency of 0 should still work (treat as 1)
    let result: Vec<_> = stream
        .par_eval_map(0, |x| async move { x * 2 })
        .collect()
        .await;
    
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
            x * 2
        })
        .collect()
        .await;
    let parallel_time = start.elapsed();
    
    // Parallel should be significantly faster for this workload
    println!("Sequential: {:?}, Parallel: {:?}", sequential_time, parallel_time);
    // Note: This is more of a benchmark than assertion, but parallel should be faster
} 