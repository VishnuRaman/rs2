use rs2_stream::stream::StreamExt;
use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};
use rs2_stream::stream_performance_metrics::HealthThresholds;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use rs2_stream::rs2_result_stream_ext::RS2ResultStreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::{empty, from_iter};

// Tests for RResultStreamExt trait
#[test]
fn test_recover_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2")]);

        let result = stream
            .recover_rs2(|e| async move {
                match e {
                    "error1" => 42,
                    "error2" => 43,
                    _ => 0,
                }
            })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![1, 42, 3, 43]);
    });
}

#[test]
fn test_on_error_resume_next_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2")]);

        let result = stream
            .on_error_resume_next_rs2(|e| match e {
                "error1" => from_iter(vec![42]),
                "error2" => from_iter(vec![43]),
                _ => from_iter(vec![0]),
            })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![1, 42, 3, 43]);
    });
}

#[test]
fn test_retry_rs2() {
    use rs2_stream::rs2_result_stream_ext::RS2ResultStreamExt;
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Counter to track how many times we've seen each index
        let counter = std::sync::Arc::new(std::sync::Mutex::new(vec![0, 0, 0]));
        let counter_for_check = counter.clone(); // Clone for checking later

        // Start with a normal stream
        let make_stream = {
            let counter = counter.clone();
            move || {
                let counter_clone = counter.clone();
                from_iter(vec![0, 1, 2])
                    .map_rs2(move |i| {
                        let mut counts = counter_clone.lock().unwrap();
                        counts[i] += 1;
                        // First item always succeeds
                        // Second item fails twice then succeeds
                        // Third item always fails
                        match i {
                            0 => Ok(i),
                            1 => {
                                if counts[i] <= 2 {
                                    Err("temp error")
                                } else {
                                    Ok(i)
                                }
                            }
                            _ => Err("permanent error"),
                        }
                    })
            }
        };

        let stream = make_stream();

        let result = stream.retry_with_policy_rs2(
            rs2_stream::error::RetryPolicy::Immediate { max_retries: 3 },
            make_stream
        ).collect::<Vec<_>>().await;

        // The retry_with_policy_rs2 function yields all items from all attempts, not just the final result
        // First attempt: Ok(0), Err("temp error")
        // Second attempt: Ok(0), Err("temp error")
        // Third attempt: Ok(0), Ok(1), Err("permanent error")
        // Fourth attempt: Ok(0), Ok(1), Err("permanent error")
        assert_eq!(
            result,
            vec![
                Ok(0),
                Err("temp error"), // First attempt
                Ok(0),
                Err("temp error"), // Second attempt
                Ok(0),
                Ok(1),
                Err("permanent error"), // Third attempt
                Ok(0),
                Ok(1),
                Err("permanent error"), // Fourth attempt
            ]
        );

        // Check retry counts
        // The retry_with_policy_rs2 function restarts the stream from the beginning each time
        // Item 0 is processed 4 times (once in each of the 4 attempts)
        // Item 1 is processed 4 times (once in each of the 4 attempts)
        // Item 2 is processed 2 times (only in the 3rd and 4th attempts, after item 1 succeeds)
        let counts = counter_for_check.lock().unwrap();
        assert_eq!(*counts, vec![4, 4, 2]);
    });
}

// Tests for RS2StreamExt trait
#[test]
fn test_map_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        let result = stream.map_rs2(|x| x * 2).collect::<Vec<_>>().await;

        assert_eq!(result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_filter_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        let result = stream.filter_rs2(|x| x % 2 == 0).collect::<Vec<_>>().await;

        assert_eq!(result, vec![2, 4]);
    });
}

#[test]
fn test_flat_map_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3]);

        let result = stream
            .flat_map_rs2(|x| from_iter(vec![x, x * 10]))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![1, 10, 2, 20, 3, 30]);
    });
}

#[test]
fn test_eval_map_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3]);

        let result = stream
            .eval_map_rs2(|x| async move { x * 2 })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![2, 4, 6]);
    });
}

#[test]
fn test_merge_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream1 = from_iter(vec![1, 3, 5]);
        let stream2 = from_iter(vec![2, 4, 6]);

        let result = stream1.merge_rs2(stream2).collect::<Vec<_>>().await;

        // Sort since merge doesn't guarantee order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        assert_eq!(sorted_result, vec![1, 2, 3, 4, 5, 6]);
    });
}

#[test]
fn test_zip_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream1 = from_iter(vec![1, 2, 3]);
        let stream2 = from_iter(vec![4, 5, 6]);

        let result = stream1.zip_rs2(stream2).collect::<Vec<_>>().await;

        assert_eq!(result, vec![(1, 4), (2, 5), (3, 6)]);
    });
}

#[test]
fn test_throttle_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let delay = std::time::Duration::from_millis(50);

        let start = std::time::Instant::now();
        let result = stream.throttle_rs2(delay).collect::<Vec<_>>().await;
        let elapsed = start.elapsed();

        assert_eq!(result, vec![1, 2, 3, 4, 5]);
        assert!(elapsed.as_millis() >= 200);
    });
}

#[test]
fn test_par_eval_map_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        let result = stream
            .par_eval_map_rs2(2, |x| async move { x * 2 })
            .collect::<Vec<_>>()
            .await;

        // Sort since parallel execution might change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        assert_eq!(sorted_result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_par_eval_map_unordered_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        let result = stream
            .par_eval_map_unordered_rs2(2, |x| async move { x * 2 })
            .collect::<Vec<_>>()
            .await;

        // Sort since unordered execution will change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        assert_eq!(sorted_result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_zip_with_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream1 = from_iter(vec![1, 2, 3]);
        let stream2 = from_iter(vec![4, 5, 6]);

        // Test with a function that adds the elements
        let result = stream1
            .zip_with_rs2(stream2, |a, b| a + b)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![5, 7, 9]);
    });
}

#[test]
fn test_zip_with_rs2_different_types() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream1 = from_iter(vec![1, 2, 3]);
        let stream2 = from_iter(vec!["a", "b", "c"]);

        // Test with a function that combines different types
        let result = stream1
            .zip_with_rs2(stream2, |a, b| format!("{}{}", a, b))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            result,
            vec!["1a".to_string(), "2b".to_string(), "3c".to_string()]
        );
    });
}

#[test]
fn test_zip_with_rs2_early_termination() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream1 = from_iter(vec![1, 2, 3, 4, 5]);
        let stream2 = from_iter(vec![10, 20, 30]);

        // Test that the rs2_stream terminates when the shorter rs2_stream ends
        let result = stream1
            .zip_with_rs2(stream2, |a, b| a + b)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![11, 22, 33]);
    });
}

// #[test]
// fn test_debounce_rs2() {
//     // This test is commented out because it uses stream! macro which doesn't implement the custom Stream trait
// }

// #[test]
// fn test_sample_rs2() {
//     // This test is commented out because it uses stream! macro which doesn't implement the custom Stream trait
// }

#[test]
fn test_par_join_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream of streams
        let streams = vec![
            from_iter(vec![1, 2, 3]),
            from_iter(vec![4, 5, 6]),
            from_iter(vec![7, 8, 9]),
        ];
        let stream_of_streams = from_iter(streams);
        let concurrency = 2;

        // Apply par_join using the extension method
        let result = stream_of_streams
            .par_join_rs2(concurrency)
            .collect::<Vec<_>>()
            .await;

        // Sort the result since parallel execution might change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        // We expect all elements from all streams
        assert_eq!(sorted_result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    });
}

#[test]
fn test_par_join_rs2_with_different_sizes() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream of streams with different sizes
        let streams = vec![
            from_iter(vec![1, 2]),
            from_iter(vec![3, 4, 5, 6]),
            from_iter(vec![7]),
            from_iter(vec![8, 9, 10]),
        ];
        let stream_of_streams = from_iter(streams);
        let concurrency = 2;

        // Apply par_join using the extension method
        let result = stream_of_streams
            .par_join_rs2(concurrency)
            .collect::<Vec<_>>()
            .await;

        // Sort the result since parallel execution might change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        // We expect all elements from all streams
        assert_eq!(sorted_result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });
}

// #[test]
// fn test_timeout_rs2() {
//     // This test is commented out because it uses stream! macro which doesn't implement the custom Stream trait
// }

// #[test]
// fn test_timeout_rs2_no_timeout() {
//     // This test is commented out because it uses stream! macro which doesn't implement the custom Stream trait
// }

#[test]
fn test_for_each_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Create a vector to store the results
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = results.clone();

        // Apply for_each_rs2 to process each item
        stream
            .for_each_rs2(move |x| {
                let results = results_clone.clone();
                async move {
                    let mut results = results.lock().await;
                    results.push(x * 2);
                }
            })
            .await;

        // Check that all items were processed correctly
        let final_results = results.lock().await;
        assert_eq!(*final_results, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_for_each_rs2_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3]);

        // Create a vector to store the results
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = results.clone();

        // Apply for_each_rs2 with an async operation
        stream
            .for_each_rs2(move |x| {
                let results = results_clone.clone();
                async move {
                    // Simulate some async work
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let mut results = results.lock().await;
                    results.push(x * 2);
                }
            })
            .await;

        // Check that all items were processed correctly
        let final_results = results.lock().await;
        assert_eq!(*final_results, vec![2, 4, 6]);
    });
}

#[test]
fn test_for_each_rs2_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Create a vector to store the results
        let results = Arc::new(Mutex::new(Vec::<i32>::new()));
        let results_clone = results.clone();

        // Apply for_each_rs2 to process each item
        stream
            .for_each_rs2(move |x: i32| {
                let results = results_clone.clone();
                async move {
                    let mut results = results.lock().await;
                    results.push(x * 2);
                }
            })
            .await;

        // Check that no items were processed (since the stream is empty)
        let final_results = results.lock().await;
        assert_eq!(*final_results, Vec::<i32>::new());
    });
}

#[test]
fn test_collect_rs2_vec() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Collect into a Vec
        let result = stream.collect_rs2().await;

        // Check that all items were collected correctly
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_collect_rs2_hashset() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers with duplicates
        let stream = from_iter(vec![1, 2, 2, 3, 3, 3, 4, 5, 5]);

        // Collect into a Vec first, then convert to HashSet
        let result = stream.collect_rs2().await;

        // Check that all unique items were collected correctly
        let expected: HashSet<_> = vec![1, 2, 3, 4, 5].into_iter().collect();
        let result_set: HashSet<_> = result.into_iter().collect();
        assert_eq!(result_set, expected);
    });
}

#[test]
fn test_collect_rs2_btreeset() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers in random order with duplicates
        let stream = from_iter(vec![5, 3, 1, 4, 2, 3, 5]);

        // Collect into a Vec first, then convert to BTreeSet
        let result = stream.collect_rs2().await;

        // Check that all unique items were collected correctly and in sorted order
        let expected: BTreeSet<_> = vec![1, 2, 3, 4, 5].into_iter().collect();
        let result_set: BTreeSet<_> = result.into_iter().collect();
        assert_eq!(result_set, expected);
    });
}

#[test]
fn test_collect_rs2_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Collect into a Vec
        let result: Vec<i32> = stream.collect_rs2().await;

        // Check that the result is an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_collect_with_config_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Create a custom buffer configuration with a small initial capacity
        let config = BufferConfig {
            initial_capacity: 2,
            max_capacity: Some(10),
            growth_strategy: GrowthStrategy::Exponential(2.0),
        };

        // Collect into a Vec with custom buffer configuration
        let result = stream.collect_with_config_rs2::<Vec<_>>(config).await;

        // Check that all items were collected correctly
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_sliding_window_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply sliding window with size 3
        let result = stream.sliding_window_rs2(3).collect::<Vec<_>>().await;

        // Check that the sliding windows are correct
        assert_eq!(result, vec![vec![1, 2, 3], vec![2, 3, 4], vec![3, 4, 5],]);
    });
}

#[test]
fn test_sliding_window_rs2_small_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with fewer elements than the window size
        let stream = from_iter(vec![1, 2]);

        // Apply sliding window with size 3
        let result = stream.sliding_window_rs2(3).collect::<Vec<_>>().await;

        // Check that no windows are emitted
        assert_eq!(result, Vec::<Vec<i32>>::new());
    });
}

#[test]
fn test_sliding_window_rs2_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Apply sliding window with size 3
        let result = stream.sliding_window_rs2(3).collect::<Vec<_>>().await;

        // Check that no windows are emitted
        assert_eq!(result, Vec::<Vec<i32>>::new());
    });
}

#[test]
fn test_sliding_window_rs2_size_zero() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply sliding window with size 0
        let result = stream.sliding_window_rs2(0).collect::<Vec<_>>().await;

        // Check that no windows are emitted
        assert_eq!(result, Vec::<Vec<i32>>::new());
    });
}

#[test]
fn test_batch_process_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5, 6]);

        // Apply batch processing with batch size 2
        let result: Vec<i32> = stream
            .batch_process_rs2(2, |batch| {
                // Process each batch by summing its elements
                vec![batch.iter().sum::<i32>()]
            })
            .collect::<Vec<_>>()
            .await;

        // Check that the batches are processed correctly
        assert_eq!(result, vec![3, 7, 11]);
    });
}

#[test]
fn test_batch_process_rs2_uneven_batches() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with a number of elements not divisible by the batch size
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply batch processing with batch size 2
        let result: Vec<i32> = stream
            .batch_process_rs2(2, |batch| {
                // Process each batch by summing its elements
                vec![batch.iter().sum::<i32>()]
            })
            .collect::<Vec<_>>()
            .await;

        // Check that the batches are processed correctly, including the last uneven batch
        assert_eq!(result, vec![3, 7, 5]);
    });
}

#[test]
fn test_batch_process_rs2_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Apply batch processing with batch size 2
        let result: Vec<i32> = stream
            .batch_process_rs2(2, |batch| {
                // Process each batch by summing its elements
                vec![batch.iter().sum::<i32>()]
            })
            .collect::<Vec<_>>()
            .await;

        // Check that no batches are processed
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_with_metrics_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply with_metrics
        let (metrics_stream, metrics) =
            stream.with_metrics_rs2("test_stream".to_string(), HealthThresholds::default());

        // Collect the stream to ensure all items are processed
        let result = metrics_stream.collect_rs2().await;

        // Give async tasks time to complete
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Check that the stream items are unchanged
        assert_eq!(result, vec![1, 2, 3, 4, 5]);

        // Check that metrics were collected
        let metrics_data = metrics.lock().await;
        assert_eq!(metrics_data.items_processed, 5);
        assert!(metrics_data.throughput_items_per_sec() > 0.0);
    });
}

#[test]
fn test_with_metrics_rs2_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Apply with_metrics
        let (metrics_stream, metrics) =
            stream.with_metrics_rs2("empty_stream".to_string(), HealthThresholds::default());

        // Collect the stream to ensure all items are processed
        let result: Vec<i32> = metrics_stream.collect_rs2().await;

        // Check that the stream is empty
        assert_eq!(result, Vec::<i32>::new());

        // Check that metrics were collected
        let metrics_data = metrics.lock().await;
        assert_eq!(metrics_data.items_processed, 0);
    });
}

#[test]
fn test_interleave_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a main stream and additional streams to interleave
        let main_stream = from_iter(vec![1, 4, 7]);
        let stream1 = from_iter(vec![2, 5, 8]);
        let stream2 = from_iter(vec![3, 6, 9]);

        // Apply interleave by chaining
        let result = main_stream
            .interleave_rs2(vec![stream1, stream2])
            .collect_rs2()
            .await;

        // Check that the streams are interleaved in round-robin fashion
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    });
}

#[test]
fn test_interleave_rs2_different_lengths() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create streams of different lengths
        let main_stream = from_iter(vec![1, 4]);
        let stream1 = from_iter(vec![2, 5, 8]);
        let stream2 = from_iter(vec![3]);

        // Apply interleave by chaining
        let result = main_stream
            .interleave_rs2(vec![stream1, stream2])
            .collect_rs2()
            .await;

        // Check that the streams are interleaved until all are exhausted
        assert_eq!(result, vec![1, 2, 3, 4, 5, 8]);
    });
}

#[test]
fn test_interleave_rs2_empty_streams() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a main stream and empty additional streams
        let main_stream = from_iter(vec![1, 2, 3]);
        let empty_stream1 = from_iter(vec![]);
        let empty_stream2 = from_iter(vec![]);

        // Apply interleave
        let result = main_stream
            .interleave_rs2(vec![empty_stream1, empty_stream2])
            .collect::<Vec<_>>()
            .await;

        // Check that only the main stream's items are emitted
        assert_eq!(result, vec![1, 2, 3]);
    });
}

#[test]
fn test_interleave_rs2_all_empty() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create all empty streams
        let main_stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);
        let empty_stream1: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);
        let empty_stream2: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Apply interleave
        let result = main_stream
            .interleave_rs2(vec![empty_stream1, empty_stream2])
            .collect::<Vec<_>>()
            .await;

        // Check that no items are emitted
        assert_eq!(result, Vec::<i32>::new());
    });
}

// #[test]
// fn test_tick_rs() {
//     // This test is commented out because tick_rs2 requires Clone trait which is not implemented for stream types
// }

#[test]
fn test_bracket_rs() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Track resource acquisition and release
        let acquired = Arc::new(std::sync::Mutex::new(false));
        let released = Arc::new(std::sync::Mutex::new(false));

        let acquired_clone = Arc::clone(&acquired);
        let released_clone = Arc::clone(&released);

        // Create a stream using bracket_rs
        let stream = empty::<i32>().bracket_rs2(
            async move {
                *acquired_clone.lock().unwrap() = true;
                "resource"
            },
            |resource| {
                assert_eq!(resource, "resource");
                from_iter(vec![1, 2, 3])
            },
            move |resource| async move {
                assert_eq!(resource, "resource");
                *released_clone.lock().unwrap() = true;
            },
        );

        // Collect the stream
        let result = stream.collect::<Vec<_>>().await;

        // Verify the stream produced the expected values
        assert_eq!(result, vec![1, 2, 3]);

        // Verify resource was acquired and released
        assert!(*acquired.lock().unwrap());
        assert!(*released.lock().unwrap());
    });
}

#[test]
fn test_rate_limit_backpressure() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of results with explicit error type
        let stream = from_iter(vec![Ok::<i32, &str>(1), Ok(2), Ok(3), Ok(4), Ok(5)]);

        // Apply rate_limit_backpressure
        let result = stream
            .rate_limit_backpressure_rs2(2)
            .collect::<Vec<_>>()
            .await;

        // Verify the stream produced the expected values
        assert_eq!(result, vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)]);
    });
}

#[test]
fn test_bracket_case_extension() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Track resource acquisition and release
        let acquired = Arc::new(std::sync::Mutex::new(false));
        let released = Arc::new(std::sync::Mutex::new(false));
        let exit_case = Arc::new(std::sync::Mutex::new(None::<String>));

        let acquired_clone = Arc::clone(&acquired);
        let released_clone = Arc::clone(&released);
        let exit_case_clone = Arc::clone(&exit_case);

        // Create a stream using bracket extension method
        let stream = from_iter(vec![Ok::<i32, ()>(1), Ok::<i32, ()>(2), Ok::<i32, ()>(3)]).bracket_rs2(
            async move {
                *acquired_clone.lock().unwrap() = true;
                "resource"
            },
            |resource| {
                assert_eq!(resource, "resource");
                from_iter(vec![Ok::<i32, ()>(4), Ok::<i32, ()>(5), Ok::<i32, ()>(6)])
            },
            move |resource| async move {
                assert_eq!(resource, "resource");
                *released_clone.lock().unwrap() = true;
            },
        );

        // Collect the stream
        let result = stream.collect::<Vec<_>>().await;

        // Verify the stream produced the expected values
        assert_eq!(result, vec![Ok(4), Ok(5), Ok(6)]);

        // Verify resource was acquired and released
        assert!(*acquired.lock().unwrap());
        assert!(*released.lock().unwrap());

        // Resource was acquired and released successfully
    });
}

#[test]
fn test_bracket_case_extension_with_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Track resource acquisition and release
        let acquired = Arc::new(std::sync::Mutex::new(false));
        let released = Arc::new(std::sync::Mutex::new(false));
        let exit_case = Arc::new(std::sync::Mutex::new(None::<String>));

        let acquired_clone = Arc::clone(&acquired);
        let released_clone = Arc::clone(&released);
        let exit_case_clone = Arc::clone(&exit_case);

        // Create a stream using bracket extension method with an error
        let stream = from_iter(vec![Ok::<i32, ()>(1), Ok::<i32, ()>(2), Ok::<i32, ()>(3)]).bracket_rs2(
            async move {
                *acquired_clone.lock().unwrap() = true;
                "resource"
            },
            |resource| {
                assert_eq!(resource, "resource");
                from_iter(vec![Ok::<i32, ()>(4), Err(()), Ok::<i32, ()>(6)])
            },
            move |resource| async move {
                assert_eq!(resource, "resource");
                *released_clone.lock().unwrap() = true;
            },
        );

        // Collect the stream
        let result = stream.collect::<Vec<_>>().await;

        // Verify the stream produced the expected values (including the error)
        assert_eq!(result, vec![Ok(4), Err(()), Ok(6)]);

        // Verify resource was acquired and released
        assert!(*acquired.lock().unwrap());
        assert!(*released.lock().unwrap());

        // Resource was acquired and released successfully (even with an error in the stream)
    });
}

#[test]
fn test_chunk_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let stream = from_iter(vec![1, 2, 3, 4, 5, 6]);

        // Apply chunking with size 2
        let result = stream.chunk_rs2(2).collect::<Vec<_>>().await;

        // Check that the chunks are correct
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5, 6],]);
    });
}

#[test]
fn test_chunk_rs2_uneven_chunks() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with a number of elements not divisible by the chunk size
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply chunking with size 2
        let result = stream.chunk_rs2(2).collect::<Vec<_>>().await;

        // Check that the chunks are correct, including the last uneven chunk
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5],]);
    });
}

#[test]
fn test_chunk_rs2_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let stream: rs2_stream::stream::Iter<std::vec::IntoIter<i32>> = from_iter(vec![]);

        // Apply chunking with size 2
        let result = stream.chunk_rs2(2).collect::<Vec<_>>().await;

        // Check that no chunks are emitted
        assert_eq!(result, Vec::<Vec<i32>>::new());
    });
}

#[test]
fn test_map_parallel_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with a decent number of items
        let item_count = 1000;
        let source_data: Vec<usize> = (0..item_count).collect();

        // Apply parallel mapping
        let stream = from_iter(source_data.clone()).map_parallel_rs2(|x| x * 2);

        // Collect the results
        let results: Vec<usize> = stream.collect().await;

        // Sort since parallel execution might change order
        let mut sorted_results = results.clone();
        sorted_results.sort();

        // Verify the results
        assert_eq!(results.len(), item_count, "Should have processed all items");

        // Create expected results
        let expected: Vec<usize> = source_data.iter().map(|&x| x * 2).collect();
        let mut sorted_expected = expected.clone();
        sorted_expected.sort();

        // Verify that the results are correctly transformed
        assert_eq!(
            sorted_results, sorted_expected,
            "Results should match expected transformations"
        );
    });
}

#[test]
fn test_map_parallel_with_concurrency_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with a decent number of items
        let item_count = 1000;
        let source_data: Vec<usize> = (0..item_count).collect();

        // Apply parallel mapping with custom concurrency
        let concurrency = 4; // Use a small, fixed concurrency for testing
        let stream = from_iter(source_data.clone())
            .map_parallel_with_concurrency_rs2(concurrency, |x| x * 3);

        // Collect the results
        let results: Vec<usize> = stream.collect().await;

        // Sort since parallel execution might change order
        let mut sorted_results = results.clone();
        sorted_results.sort();

        // Verify the results
        assert_eq!(results.len(), item_count, "Should have processed all items");

        // Create expected results
        let expected: Vec<usize> = source_data.iter().map(|&x| x * 3).collect();
        let mut sorted_expected = expected.clone();
        sorted_expected.sort();

        // Verify that the results are correctly transformed
        assert_eq!(
            sorted_results, sorted_expected,
            "Results should match expected transformations"
        );
    });
}
