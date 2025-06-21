use futures_util::stream::StreamExt;
use rs2_stream::error::RetryPolicy;
use rs2_stream::rs2::*;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

#[test]
fn test_retry_with_policy_immediate() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)]);

        // Create a retry policy
        let policy = RetryPolicy::Immediate { max_retries: 2 };

        // Apply the retry policy with a rs2_stream factory
        let result = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)])
            })
            .collect::<Vec<_>>()
            .await;

        // The retry_with_policy function yields all items from all attempts
        // First attempt: Ok(1), Err("error1")
        // Second attempt: Ok(1), Err("error1")
        // Third attempt: Ok(1), Err("error1")
        // Since we hit max_retries, we stop
        assert_eq!(
            result,
            vec![
                Ok(1),
                Err("error1"), // First attempt
                Ok(1),
                Err("error1"), // Second attempt
                Ok(1),
                Err("error1"), // Third attempt
            ]
        );
    });
}

#[test]
fn test_retry_with_policy_fixed_delay() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3)]);

        // Create a retry policy with a fixed delay
        let delay = Duration::from_millis(100);
        let policy = RetryPolicy::Fixed {
            max_retries: 1,
            delay,
        };

        // Measure the time it takes to process the rs2_stream with retries
        let start = Instant::now();
        let result = stream
            .retry_with_policy_rs2(policy, || from_iter(vec![Ok(1), Err("error1"), Ok(3)]))
            .collect::<Vec<_>>()
            .await;
        let elapsed = start.elapsed();

        // The retry_with_policy function yields all items from all attempts
        // First attempt: Ok(1), Err("error1")
        // Second attempt: Ok(1), Err("error1")
        assert_eq!(
            result,
            vec![
                Ok(1),
                Err("error1"), // First attempt
                Ok(1),
                Err("error1"), // Second attempt
            ]
        );

        // Verify that the delay was applied
        assert!(
            elapsed.as_millis() >= 100,
            "Expected delay of at least 100ms"
        );
    });
}

#[test]
fn test_map_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2")]);

        // Map errors to a different type
        let result = stream
            .map_error_rs2(|e| format!("Mapped: {}", e))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            result,
            vec![
                Ok(1),
                Err("Mapped: error1".to_string()),
                Ok(3),
                Err("Mapped: error2".to_string()),
            ]
        );
    });
}

#[test]
fn test_or_else() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2")]);

        // Replace errors with fallback values
        let result = stream
            .or_else_rs2(|e| match e {
                "error1" => 42,
                "error2" => 43,
                _ => 0,
            })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result, vec![1, 42, 3, 43]);
    });
}

#[test]
fn test_collect_ok() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)]);

        // Collect only successful values
        let result = stream.collect_ok_rs2().collect::<Vec<_>>().await;

        // Should contain a single Vec with all successful values
        assert_eq!(result, vec![vec![1, 3, 5]]);
    });
}

#[test]
fn test_collect_err() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)]);

        // Collect only errors
        let result = stream.collect_err_rs2().collect::<Vec<_>>().await;

        // Should contain a single Vec with all errors
        assert_eq!(result, vec![vec!["error1", "error2"]]);
    });
}
