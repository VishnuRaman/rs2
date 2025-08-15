use rs2_stream::error::RetryPolicy;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use rs2_stream::rs2_result_stream_ext::RS2ResultStreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::from_iter;

#[test]
fn test_retry_with_policy_immediate() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)]);

        // Create a retry policy
        let policy = RetryPolicy::Immediate { max_retries: 2 };

        // Apply the retry policy with a rs2_stream factory
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)])
            })
            .collect_rs2()
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
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || from_iter(vec![Ok(1), Err("error1"), Ok(3)]))
            .collect_rs2()
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
        let result: Vec<Result<i32, String>> = stream
            .map_err(|e| format!("Mapped: {}", e))
            .collect_rs2()
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
        let result: Vec<i32> = stream
            .handle_errors(|e| match e {
                "error1" => 42,
                "error2" => 43,
                _ => 0,
            })
            .collect_rs2()
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
        let result = stream.collect_ok().await;

        // Should contain all successful values
        assert_eq!(result, vec![1, 3, 5]);
    });
}

#[test]
fn test_collect_err() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream with errors
        let stream = from_iter(vec![Ok(1), Err("error1"), Ok(3), Err("error2"), Ok(5)]);

        // Collect only errors
        let result = stream.collect_err().await;

        // Should contain all errors
        assert_eq!(result, vec!["error1", "error2"]);
    });
}

#[test]
fn test_retry_debug() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a simple stream with just one error
        let stream = from_iter(vec![Ok(1), Err("error1")]);

        // Create a retry policy with 1 retry (should retry once)
        let policy = RetryPolicy::Immediate { max_retries: 1 };

        // Apply the retry policy
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1")])
            })
            .collect_rs2()
            .await;

        println!("Debug result: {:?}", result);
        
        // Should have: Ok(1), Err("error1"), Ok(1), Err("error1") (one retry)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Ok(1));
        assert_eq!(result[1], Err("error1"));
        assert_eq!(result[2], Ok(1));
        assert_eq!(result[3], Err("error1"));
    });
}

#[test]
fn test_retry_simple() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a simple stream with just one error
        let stream = from_iter(vec![Ok(1), Err("error1")]);

        // Create a retry policy
        let policy = RetryPolicy::Immediate { max_retries: 1 };

        // Apply the retry policy
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1")])
            })
            .collect_rs2()
            .await;

        println!("Result: {:?}", result);
        
        // Should have: Ok(1), Err("error1"), Ok(1), Err("error1")
        assert_eq!(result.len(), 4);
    });
}

#[test]
fn test_retry_fixed_delay_debug() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a simple stream with just one error
        let stream = from_iter(vec![Ok(1), Err("error1")]);

        // Create a retry policy with a fixed delay
        let delay = Duration::from_millis(10); // Short delay for testing
        let policy = RetryPolicy::Fixed {
            max_retries: 1,
            delay,
        };

        println!("Starting fixed delay retry test...");
        
        // Apply the retry policy
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1")])
            })
            .collect_rs2()
            .await;

        println!("Fixed delay debug result: {:?}", result);
        
        // Should have: Ok(1), Err("error1"), Ok(1), Err("error1") (one retry)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Ok(1));
        assert_eq!(result[1], Err("error1"));
        assert_eq!(result[2], Ok(1));
        assert_eq!(result[3], Err("error1"));
    });
}

#[test]
fn test_retry_simple_no_delay() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a simple stream with just one error
        let stream = from_iter(vec![Ok(1), Err("error1")]);

        // Create a retry policy with no delay
        let policy = RetryPolicy::Immediate { max_retries: 1 };

        println!("Starting simple no-delay retry test...");
        
        // Apply the retry policy
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1")])
            })
            .collect_rs2()
            .await;

        println!("Simple no-delay result: {:?}", result);
        
        // Should have: Ok(1), Err("error1"), Ok(1), Err("error1") (one retry)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Ok(1));
        assert_eq!(result[1], Err("error1"));
        assert_eq!(result[2], Ok(1));
        assert_eq!(result[3], Err("error1"));
    });
}

#[test]
fn test_retry_minimal_delay() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a simple stream with just one error
        let stream = from_iter(vec![Ok(1), Err("error1")]);

        // Create a retry policy with minimal delay
        let delay = Duration::from_millis(1); // 1ms delay
        let policy = RetryPolicy::Fixed {
            max_retries: 1,
            delay,
        };

        println!("Starting minimal delay retry test...");
        
        // Apply the retry policy
        let result: Vec<Result<i32, &str>> = stream
            .retry_with_policy_rs2(policy, || {
                from_iter(vec![Ok(1), Err("error1")])
            })
            .collect_rs2()
            .await;

        println!("Minimal delay result: {:?}", result);
        
        // Should have: Ok(1), Err("error1"), Ok(1), Err("error1") (one retry)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Ok(1));
        assert_eq!(result[1], Err("error1"));
        assert_eq!(result[2], Ok(1));
        assert_eq!(result[3], Err("error1"));
    });
}




