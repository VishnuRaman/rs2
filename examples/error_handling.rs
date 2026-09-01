//! Handling errors in RS2 streams.
//!
//! RS2 streams are infallible: errors travel *in band* as `Result` items, which
//! is the standard Rust shape (`futures`' `TryStream` works the same way).
//! `RS2ResultStreamExt` is the toolkit for streams of `Result<T, E>`.
//!
//! Run with: `cargo run --example error_handling`

use futures_util::stream::StreamExt;
use rs2_stream::error::RetryPolicy;
use rs2_stream::rs2::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn sample() -> RS2Stream<Result<i32, String>> {
    from_iter(vec![
        Ok(1),
        Err("network timeout".to_string()),
        Ok(3),
        Err("bad payload".to_string()),
        Ok(5),
    ])
}

#[tokio::main]
async fn main() {
    println!("=== recover: replace each error with a value ===");
    let out = sample()
        .recover_rs2(|e| async move {
            println!("  recovering from: {}", e);
            -1
        })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, -1, 3, -1, 5], "recover replaces each error in place");
    println!("  -> {:?}\n", out);

    println!("=== or_else: same idea, synchronous ===");
    let out = sample().or_else_rs2(|_e| 0).collect::<Vec<_>>().await;
    println!("  -> {:?}\n", out);

    println!("=== on_error_resume_next: substitute a whole stream per error ===");
    // The source keeps going after each error.
    let out = sample()
        .on_error_resume_next_rs2(|e| {
            if e.contains("timeout") {
                from_iter(vec![-10, -11])
            } else {
                from_iter(vec![-20])
            }
        })
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?}\n", out);

    println!("=== handle_error_with: stop at the first error ===");
    // FS2's `handleErrorWith`. Note how 3 and 5 never appear: unlike
    // `on_error_resume_next`, the source terminates and the handler takes over.
    let out = sample()
        .handle_error_with_rs2(|e| {
            println!("  handling: {} (source stops here)", e);
            from_iter(vec![-99])
        })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, -99], "handle_error_with terminates the source");
    println!("  -> {:?}\n", out);

    println!("=== attempt: emit up to and including the first error, then stop ===");
    let out = sample().attempt_rs2().collect::<Vec<_>>().await;
    println!("  -> {:?}\n", out);

    println!("=== map_error: change the error type ===");
    #[derive(Debug)]
    struct AppError {
        code: u16,
    }
    let out = sample()
        .map_error_rs2(|e| AppError {
            code: if e.contains("timeout") { 504 } else { 400 },
        })
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?}\n", out);

    println!("=== collect_ok / collect_err: partition the outcomes ===");
    let oks = sample().collect_ok_rs2().collect::<Vec<_>>().await;
    let errs = sample().collect_err_rs2().collect::<Vec<_>>().await;
    println!("  ok values:  {:?}", oks);
    println!("  error values: {:?}\n", errs);

    println!("=== retry: re-run the whole stream on failure ===");
    // The stream succeeds only on the third attempt.
    let attempts = Arc::new(Mutex::new(0));
    let counter = attempts.clone();
    let make = move || {
        // NOTE: `use rs2_stream::rs2::*` brings RS2's own `drop` combinator into
        // scope, shadowing `std::mem::drop`. Scope the guard instead.
        let attempt = {
            let mut n = counter.lock().unwrap();
            *n += 1;
            *n
        };
        if attempt < 3 {
            from_iter(vec![Ok(1), Err(format!("attempt {} failed", attempt))])
        } else {
            from_iter(vec![Ok(1), Ok(2)])
        }
    };
    let out = make().retry_rs2(3, make.clone()).collect::<Vec<_>>().await;
    println!("  after {} attempts -> {:?}", attempts.lock().unwrap(), out);
    println!();

    println!("=== retry_with_policy: back off between attempts ===");
    let attempts = Arc::new(Mutex::new(0));
    let counter = attempts.clone();
    let make = move || {
        let attempt = {
            let mut n = counter.lock().unwrap();
            *n += 1;
            *n
        };
        if attempt < 2 {
            from_iter(vec![Err::<i32, String>("still failing".to_string())])
        } else {
            from_iter(vec![Ok(42)])
        }
    };
    let policy = RetryPolicy::Exponential {
        max_retries: 3,
        initial_delay: Duration::from_millis(20),
        multiplier: 2.0,
    };
    let out = make()
        .retry_with_policy_rs2(policy, make.clone())
        .collect::<Vec<_>>()
        .await;
    println!("  after {} attempts -> {:?}", attempts.lock().unwrap(), out);

    println!("\nTip: `recover`/`or_else` fix each error in place; \
`on_error_resume_next` substitutes a stream per error; \
`handle_error_with` and `attempt` stop at the first one.");
}
