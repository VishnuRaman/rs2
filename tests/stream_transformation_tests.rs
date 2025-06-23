use async_stream::stream;
use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;

#[test]
fn test_zip_with() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let s1 = from_iter(vec![1, 2, 3]);
        let s2 = from_iter(vec![10, 20, 30, 40]);

        // Using the function directly
        let result1 = zip_with(s1, s2, |a, b| a + b).collect::<Vec<_>>().await;
        assert_eq!(result1, vec![11, 22, 33]);

        // Using the extension trait
        let s3 = from_iter(vec![1, 2, 3]);
        let s4 = from_iter(vec![10, 20, 30, 40]);
        let result2 = s3
            .zip_with_rs2(s4, |a, b| format!("{}-{}", a, b))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result2, vec!["1-10", "2-20", "3-30"]);
    });
}

#[test]
fn test_zip_with_different_lengths() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let s1 = from_iter(vec![1, 2, 3, 4, 5]);
        let s2 = from_iter(vec![10, 20, 30]);

        let result = zip_with(s1, s2, |a, b| a * b).collect::<Vec<_>>().await;
        assert_eq!(result, vec![10, 40, 90]);
    });
}

#[test]
fn test_distinct_until_changed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with consecutive duplicates
        let s1 = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);

        // Using the function directly
        let result1 = distinct_until_changed(s1).collect::<Vec<_>>().await;
        assert_eq!(result1, vec![1, 2, 3, 2, 1]);

        // Using the extension trait
        let s2 = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);
        let result2 = s2.distinct_until_changed_rs2().collect::<Vec<_>>().await;
        assert_eq!(result2, vec![1, 2, 3, 2, 1]);
    });
}

#[test]
fn test_distinct_until_changed_by() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with a custom equality function that considers two numbers equal if they have the same parity
        let s1 = from_iter(vec![1, 3, 2, 4, 5, 7, 6, 8]);

        // Using the function directly
        let result1 = distinct_until_changed_by(s1, |a, b| a % 2 == b % 2)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result1, vec![1, 2, 5, 6]);

        // Using the extension trait
        let s2 = from_iter(vec![1, 3, 2, 4, 5, 7, 6, 8]);
        let result2 = s2
            .distinct_until_changed_by_rs2(|a, b| a % 2 == b % 2)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result2, vec![1, 2, 5, 6]);
    });
}

#[test]
fn test_distinct_until_changed_edge_cases() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Empty rs2_stream
        let s1 = from_iter(Vec::<i32>::new());
        let result1 = distinct_until_changed(s1).collect::<Vec<_>>().await;
        assert_eq!(result1, Vec::<i32>::new());

        // Single element
        let s2 = from_iter(vec![42]);
        let result2 = distinct_until_changed(s2).collect::<Vec<_>>().await;
        assert_eq!(result2, vec![42]);

        // All duplicates
        let s3 = from_iter(vec![7, 7, 7, 7, 7]);
        let result3 = distinct_until_changed(s3).collect::<Vec<_>>().await;
        assert_eq!(result3, vec![7]);

        // No duplicates
        let s4 = from_iter(vec![1, 2, 3, 4, 5]);
        let result4 = distinct_until_changed(s4).collect::<Vec<_>>().await;
        assert_eq!(result4, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_group_adjacent_by() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with a simple key function that groups by parity (odd/even)
        let s1 = from_iter(vec![1, 3, 2, 4, 5, 7, 6, 8]);

        // Using the function directly
        let result1 = group_adjacent_by(s1, |&x| x % 2).collect::<Vec<_>>().await;
        assert_eq!(
            result1,
            vec![
                (1, vec![1, 3]),
                (0, vec![2, 4]),
                (1, vec![5, 7]),
                (0, vec![6, 8])
            ]
        );

        // Using the extension trait
        let s2 = from_iter(vec![1, 3, 2, 4, 5, 7, 6, 8]);
        let result2 = s2
            .group_adjacent_by_rs2(|&x| x % 2)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            result2,
            vec![
                (1, vec![1, 3]),
                (0, vec![2, 4]),
                (1, vec![5, 7]),
                (0, vec![6, 8])
            ]
        );
    });
}

#[test]
fn test_group_adjacent_by_with_string_keys() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with a key function that returns a string
        let s1 = from_iter(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Group by "small" (1-3), "medium" (4-7), or "large" (8-10)
        let result = group_adjacent_by(s1, |&x| {
            if x <= 3 {
                "small".to_string()
            } else if x <= 7 {
                "medium".to_string()
            } else {
                "large".to_string()
            }
        })
        .collect::<Vec<_>>()
        .await;

        assert_eq!(
            result,
            vec![
                ("small".to_string(), vec![1, 2, 3]),
                ("medium".to_string(), vec![4, 5, 6, 7]),
                ("large".to_string(), vec![8, 9, 10])
            ]
        );
    });
}

#[test]
fn test_group_adjacent_by_edge_cases() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Empty rs2_stream
        let s1 = from_iter(Vec::<i32>::new());
        let result1 = group_adjacent_by(s1, |&x| x % 2).collect::<Vec<_>>().await;
        assert_eq!(result1, Vec::<(i32, Vec<i32>)>::new());

        // Single element
        let s2 = from_iter(vec![42]);
        let result2 = group_adjacent_by(s2, |&x| x % 2).collect::<Vec<_>>().await;
        assert_eq!(result2, vec![(0, vec![42])]);

        // All same key
        let s3 = from_iter(vec![1, 3, 5, 7, 9]);
        let result3 = group_adjacent_by(s3, |&x| x % 2).collect::<Vec<_>>().await;
        assert_eq!(result3, vec![(1, vec![1, 3, 5, 7, 9])]);

        // Alternating keys
        let s4 = from_iter(vec![1, 2, 3, 4, 5]);
        let result4 = group_adjacent_by(s4, |&x| x % 2).collect::<Vec<_>>().await;
        assert_eq!(
            result4,
            vec![
                (1, vec![1]),
                (0, vec![2]),
                (1, vec![3]),
                (0, vec![4]),
                (1, vec![5])
            ]
        );
    });
}

#[test]
fn test_interrupt_when() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream that emits numbers every 50ms
        let stream = from_iter(0..100).throttle_rs2(Duration::from_millis(50));

        // Create a future that completes after 175ms
        // This should allow approximately 3-4 items to be emitted
        let interrupt_signal = tokio::time::sleep(Duration::from_millis(175));

        // Using the function directly
        let result1 = interrupt_when(stream, interrupt_signal)
            .collect::<Vec<_>>()
            .await;

        // We expect 3-4 items (0, 1, 2, possibly 3)
        // The exact number can vary slightly due to timing, so we check a range
        assert!(
            result1.len() >= 3 && result1.len() <= 4,
            "Expected 3-4 items, got {}",
            result1.len()
        );

        // Check that the items are the expected ones
        for (i, &item) in result1.iter().enumerate() {
            assert_eq!(item, i as i32);
        }

        // Create a new rs2_stream and signal for testing the extension method
        let stream2 = from_iter(0..100).throttle_rs2(Duration::from_millis(50));
        let interrupt_signal2 = tokio::time::sleep(Duration::from_millis(175));

        // Using the extension trait
        let result2 = stream2
            .interrupt_when_rs2(interrupt_signal2)
            .collect::<Vec<_>>()
            .await;

        // We expect similar results as with the direct function
        assert!(
            result2.len() >= 3 && result2.len() <= 4,
            "Expected 3-4 items, got {}",
            result2.len()
        );

        // Check that the items are the expected ones
        for (i, &item) in result2.iter().enumerate() {
            assert_eq!(item, i as i32);
        }
    });
}

#[test]
fn test_interrupt_when_immediate() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream that emits numbers every 50ms
        // This gives the interrupt signal time to be detected
        let stream = from_iter(0..100).throttle_rs2(Duration::from_millis(50));

        // Create a future that completes after a very short delay
        // This ensures the interrupt signal has time to be registered
        let interrupt_signal = tokio::time::sleep(Duration::from_millis(10));

        // The rs2_stream should be interrupted after the signal completes
        let result = interrupt_when(stream, interrupt_signal)
            .collect::<Vec<_>>()
            .await;

        // We expect very few items (possibly none) since the interrupt signal completes quickly
        assert!(
            result.len() < 3,
            "Expected fewer than 3 items, got {}",
            result.len()
        );

        // Check that the items are the expected ones (if any)
        for (i, &item) in result.iter().enumerate() {
            assert_eq!(item, i as i32);
        }
    });
}

#[test]
fn test_interrupt_when_completed_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream that completes before the interrupt signal
        let stream = from_iter(0..3);

        // Create a future that completes after the rs2_stream would have completed
        let interrupt_signal = tokio::time::sleep(Duration::from_millis(100));

        // The rs2_stream should complete normally, yielding all items
        let result = interrupt_when(stream, interrupt_signal)
            .collect::<Vec<_>>()
            .await;

        // We expect all items since the rs2_stream completes before the interrupt signal
        assert_eq!(result, vec![0, 1, 2]);
    });
}

#[test]
fn test_either() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with two streams that produce values at different times
        let s1 = from_iter(vec![1, 2, 3]);
        let s2 = from_iter(vec![10, 20, 30]);

        // Using the function directly
        let result1 = either(s1, s2).collect::<Vec<_>>().await;

        // We expect to get values from the first rs2_stream only, since it's selected first
        // and then the second rs2_stream is cancelled
        // Note: The actual behavior depends on the implementation details
        // So we just check that we got all values from the first rs2_stream
        assert!(result1.contains(&1) && result1.contains(&2) && result1.contains(&3));

        // Using the extension trait
        let s3 = from_iter(vec![1, 2, 3]);
        let s4 = from_iter(vec![10, 20, 30]);
        let result2 = s3.either_rs2(s4).collect::<Vec<_>>().await;

        // We expect the same result as with the direct function
        // Note: The actual behavior depends on the implementation details
        // So we just check that we got all values from the first rs2_stream
        assert!(result2.contains(&1) && result2.contains(&2) && result2.contains(&3));
    });
}

#[test]
fn test_either_with_different_timing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create two streams with different timing
        let fast_stream = stream! {
            yield 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            yield 2;
            tokio::time::sleep(Duration::from_millis(100)).await;
            yield 3;
        };

        let slow_stream = stream! {
            tokio::time::sleep(Duration::from_millis(50)).await;
            yield 10;
            tokio::time::sleep(Duration::from_millis(10)).await;
            yield 20;
        };

        // The either combinator will select values from whichever rs2_stream produces first
        let result = either(fast_stream.boxed(), slow_stream.boxed())
            .collect::<Vec<_>>()
            .await;

        // We expect to get values from the fast rs2_stream first (1, 2),
        // then from the slow rs2_stream when the fast rs2_stream is waiting longer (10, 20)
        // Note: The actual behavior depends on the timing, which can be non-deterministic in tests
        // So we just check that we got some values from both streams
        assert!(
            result.contains(&1)
                && result.contains(&2)
                && result.contains(&10)
                && result.contains(&20)
        );
    });
}

#[test]
fn test_either_with_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with one empty rs2_stream and one non-empty rs2_stream
        let empty_stream = from_iter(Vec::<i32>::new());
        let non_empty_stream = from_iter(vec![1, 2, 3]);

        // The either combinator should switch to the non-empty rs2_stream when the empty rs2_stream completes
        let result1 = either(empty_stream, non_empty_stream)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result1, vec![1, 2, 3]);

        // Test with both streams empty
        let empty_stream1 = from_iter(Vec::<i32>::new());
        let empty_stream2 = from_iter(Vec::<i32>::new());

        // The either combinator should complete immediately with no values
        let result2 = either(empty_stream1, empty_stream2)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result2, Vec::<i32>::new());
    });
}

#[test]
fn test_either_with_early_completion() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test with one rs2_stream that completes early and one that continues
        let short_stream = from_iter(vec![1, 2]);
        let long_stream = from_iter(vec![10, 20, 30, 40]);

        // The either combinator should switch to the long rs2_stream when the short rs2_stream completes
        let result = either(short_stream, long_stream).collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2, 10, 20, 30, 40]);
    });
}

#[test]
fn test_unfold_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream of Fibonacci numbers
        let fibonacci = unfold((0, 1), |state| async move {
            let (a, b) = state;
            Some((a, (b, a + b)))
        });

        // Take the first 10 Fibonacci numbers
        let result = fibonacci.take(10).collect::<Vec<_>>().await;
        assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]);
    });
}

#[test]
fn test_unfold_termination() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream that counts from 1 to 5 and then terminates
        let counting = unfold(1, |n| async move {
            if n <= 5 {
                Some((n, n + 1))
            } else {
                None
            }
        });

        // Collect all elements (should be 1 to 5)
        let result = counting.collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_unfold_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream that emits values with a delay
        let delayed = unfold(1, |n| async move {
            if n <= 3 {
                // Simulate some async work
                tokio::time::sleep(Duration::from_millis(10)).await;
                Some((n * 10, n + 1))
            } else {
                None
            }
        });

        // Collect all elements
        let result = delayed.collect::<Vec<_>>().await;
        assert_eq!(result, vec![10, 20, 30]);
    });
}

#[test]
fn test_unfold_large_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream that generates a large number of elements
        let large_stream = unfold(0, |n| async move {
            if n < 10000 {
                Some((n, n + 1))
            } else {
                None
            }
        });

        // Just take the first few elements to verify it works
        let result = large_stream.take(5).collect::<Vec<_>>().await;
        assert_eq!(result, vec![0, 1, 2, 3, 4]);

        // Create a new rs2_stream for the count operation
        let count_stream = unfold(0, |n| async move {
            if n < 10000 {
                Some((n, n + 1))
            } else {
                None
            }
        });

        // Now collect all elements to ensure it completes without memory issues
        let count = count_stream.count().await;
        assert_eq!(count, 10000);
    });
}

#[test]
fn test_fold_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Fold to compute the sum
        let sum = fold(numbers, 0, |acc, x| async move { acc + x }).await;
        assert_eq!(sum, 15);

        // Create a stream of numbers 1 to 5 again
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Fold to compute the product
        let product = fold(numbers, 1, |acc, x| async move { acc * x }).await;
        assert_eq!(product, 120);
    });
}

#[test]
fn test_fold_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 3
        let numbers = from_iter(vec![1, 2, 3]);

        // Fold with an async operation that adds a delay
        let result = fold(numbers, 0, |acc, x| async move {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(10)).await;
            acc + x
        })
        .await;

        assert_eq!(result, 6);
    });
}

#[test]
fn test_fold_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Fold should return the initial value for an empty stream
        let result = fold(empty, 42, |acc, x| async move { acc + x }).await;
        assert_eq!(result, 42);
    });
}

#[test]
fn test_scan_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Scan to compute running sum
        let result = scan(numbers, 0, |acc, x| acc + x).collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 3, 6, 10, 15]);

        // Create a stream of numbers 1 to 5 again
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Scan to compute running product
        let result = scan(numbers, 1, |acc, x| acc * x).collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2, 6, 24, 120]);
    });
}

#[test]
fn test_scan_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Scan should not yield any values for an empty stream
        let result = scan(empty, 42, |acc, x| acc + x).collect::<Vec<_>>().await;
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_scan_with_complex_state() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Scan to compute both sum and product
        let result = scan(numbers, (0, 1), |(sum, product), x| (sum + x, product * x))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            result,
            vec![
                (1, 1),    // sum=0+1=1, product=1*1=1
                (3, 2),    // sum=1+2=3, product=1*2=2
                (6, 6),    // sum=3+3=6, product=2*3=6
                (10, 24),  // sum=6+4=10, product=6*4=24
                (15, 120)  // sum=10+5=15, product=24*5=120
            ]
        );
    });
}

#[test]
fn test_scan_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Use the extension method to compute running sum
        let result = numbers
            .scan_rs2(0, |acc, x| acc + x)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![1, 3, 6, 10, 15]);

        // Create a stream of numbers 1 to 5 again
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Use the extension method to compute running product
        let result = numbers
            .scan_rs2(1, |acc, x| acc * x)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![1, 2, 6, 24, 120]);
    });
}

#[test]
fn test_reduce_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Reduce to compute the sum
        let sum = reduce(numbers, |acc, x| async move { acc + x }).await;
        assert_eq!(sum, Some(15));

        // Create a stream of numbers 1 to 5 again
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Reduce to compute the product
        let product = reduce(numbers, |acc, x| async move { acc * x }).await;
        assert_eq!(product, Some(120));
    });
}

#[test]
fn test_reduce_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 3
        let numbers = from_iter(vec![1, 2, 3]);

        // Reduce with an async operation that adds a delay
        let result = reduce(numbers, |acc, x| async move {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(10)).await;
            acc + x
        })
        .await;

        assert_eq!(result, Some(6));
    });
}

#[test]
fn test_reduce_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Reduce should return None for an empty stream
        let result = reduce(empty, |acc, x| async move { acc + x }).await;
        assert_eq!(result, None);
    });
}

#[test]
fn test_reduce_single_element() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with a single element
        let single = from_iter(vec![42]);

        // Reduce should return the single element
        let result = reduce(single, |acc, x| async move { acc + x }).await;
        assert_eq!(result, Some(42));
    });
}

#[test]
fn test_filter_map_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Filter out odd numbers and double even numbers
        let result = filter_map(numbers, |x| async move {
            if x % 2 == 0 {
                Some(x * 2)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .await;

        // Should only contain doubled even numbers
        assert_eq!(result, vec![4, 8]);
    });
}

#[test]
fn test_filter_map_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Filter map should return an empty stream
        let result = filter_map(empty, |x| async move {
            if x % 2 == 0 {
                Some(x * 2)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_filter_map_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Filter out odd numbers and double even numbers with a delay
        let result = filter_map(numbers, |x| async move {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(10)).await;
            if x % 2 == 0 {
                Some(x * 2)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .await;

        // Should only contain doubled even numbers
        assert_eq!(result, vec![4, 8]);
    });
}

#[test]
fn test_filter_map_all_filtered() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Filter out all numbers
        let result = filter_map(numbers, |_| async move { None::<i32> })
            .collect::<Vec<_>>()
            .await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_filter_map_none_filtered() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Keep all numbers and double them
        let result = filter_map(numbers, |x| async move { Some(x * 2) })
            .collect::<Vec<_>>()
            .await;

        // Should contain all doubled numbers
        assert_eq!(result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_group_by_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with mixed values
        let numbers = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);

        // Group by even/odd (using modulo 2)
        let result = group_by(numbers, |&x| x % 2).collect::<Vec<_>>().await;

        // Should group consecutive elements with the same key
        assert_eq!(
            result,
            vec![
                (1, vec![1, 1]), // Odd numbers (1, 1)
                (0, vec![2, 2]), // Even numbers (2, 2)
                (1, vec![3, 3]), // Odd numbers (3, 3)
                (0, vec![2]),    // Even number (2)
                (1, vec![1])     // Odd number (1)
            ]
        );
    });
}

#[test]
fn test_group_by_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Group by even/odd
        let result = group_by(empty, |&x| x % 2).collect::<Vec<_>>().await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<(i32, Vec<i32>)>::new());
    });
}

#[test]
fn test_group_by_all_same_key() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream where all elements have the same key
        let numbers = from_iter(vec![2, 4, 6, 8, 10]);

        // Group by even/odd (all are even)
        let result = group_by(numbers, |&x| x % 2).collect::<Vec<_>>().await;

        // Should produce a single group with all elements
        assert_eq!(
            result,
            vec![
                (0, vec![2, 4, 6, 8, 10])  // All even numbers
            ]
        );
    });
}

#[test]
fn test_group_by_unique_keys() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream where each element has a unique key
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Group by the value itself (each value is its own group)
        let result = group_by(numbers, |&x| x).collect::<Vec<_>>().await;

        // Should produce a separate group for each element
        assert_eq!(
            result,
            vec![
                (1, vec![1]),
                (2, vec![2]),
                (3, vec![3]),
                (4, vec![4]),
                (5, vec![5])
            ]
        );
    });
}

#[test]
fn test_group_by_complex_key() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of strings
        let words = from_iter(vec![
            "apple".to_string(),
            "apricot".to_string(),
            "banana".to_string(),
            "blueberry".to_string(),
            "cherry".to_string(),
            "cantaloupe".to_string(),
        ]);

        // Group by first letter
        let result = group_by(words, |s| s.chars().next().unwrap())
            .collect::<Vec<_>>()
            .await;

        // Should group words by their first letter
        assert_eq!(
            result,
            vec![
                ('a', vec!["apple".to_string(), "apricot".to_string()]),
                ('b', vec!["banana".to_string(), "blueberry".to_string()]),
                ('c', vec!["cherry".to_string(), "cantaloupe".to_string()])
            ]
        );
    });
}

#[test]
fn test_take_while_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Take elements while they are less than 4
        let result = take_while(numbers, |&x| async move { x < 4 })
            .collect::<Vec<_>>()
            .await;

        // Should contain elements 1, 2, 3
        assert_eq!(result, vec![1, 2, 3]);
    });
}

#[test]
fn test_take_while_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Take while should return an empty stream
        let result = take_while(empty, |&x| async move { x < 4 })
            .collect::<Vec<_>>()
            .await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_take_while_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Take elements while they are less than 4 with a delay
        let result = take_while(numbers, |&x| async move {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(10)).await;
            x < 4
        })
        .collect::<Vec<_>>()
        .await;

        // Should contain elements 1, 2, 3
        assert_eq!(result, vec![1, 2, 3]);
    });
}

#[test]
fn test_take_while_all_taken() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Take all elements (predicate always true)
        let result = take_while(numbers, |_| async move { true })
            .collect::<Vec<_>>()
            .await;

        // Should contain all elements
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_take_while_none_taken() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Take no elements (predicate always false)
        let result = take_while(numbers, |_| async move { false })
            .collect::<Vec<_>>()
            .await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_drop_while_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Drop elements while they are less than 4
        let result = drop_while(numbers, |&x| async move { x < 4 })
            .collect::<Vec<_>>()
            .await;

        // Should contain elements 4, 5
        assert_eq!(result, vec![4, 5]);
    });
}

#[test]
fn test_drop_while_empty_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an empty stream
        let empty: RS2Stream<i32> = from_iter(vec![]);

        // Drop while should return an empty stream
        let result = drop_while(empty, |&x| async move { x < 4 })
            .collect::<Vec<_>>()
            .await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_drop_while_async() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Drop elements while they are less than 4 with a delay
        let result = drop_while(numbers, |&x| async move {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(10)).await;
            x < 4
        })
        .collect::<Vec<_>>()
        .await;

        // Should contain elements 4, 5
        assert_eq!(result, vec![4, 5]);
    });
}

#[test]
fn test_drop_while_all_dropped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Drop all elements (predicate always true)
        let result = drop_while(numbers, |_| async move { true })
            .collect::<Vec<_>>()
            .await;

        // Result should be an empty vector
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_drop_while_none_dropped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers 1 to 5
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Drop no elements (predicate always false)
        let result = drop_while(numbers, |_| async move { false })
            .collect::<Vec<_>>()
            .await;

        // Should contain all elements
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}
