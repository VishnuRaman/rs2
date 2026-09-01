//! Guaranteed cleanup: bracket, bracket_case, try_bracket_case, on_finalize.
//!
//! Every one of these runs its cleanup on *all* termination paths — the stream
//! finishing, the consumer stopping early, or the stream being dropped.
//! `ExitCase` tells the finalizer which of those happened, mirroring FS2's
//! `Resource.ExitCase`.
//!
//! Run with: `cargo run --example resource_finalizers`

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Cleanup on the early-termination path is spawned rather than awaited
/// (`Drop` cannot await), so give it a moment before checking.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn describe(case: &ExitCase<String>) -> String {
    match case {
        ExitCase::Completed => "Completed — the stream was exhausted".into(),
        ExitCase::Canceled => "Canceled — the consumer stopped early".into(),
        ExitCase::Errored(e) => format!("Errored({}) — the stream failed", e),
    }
}

#[tokio::main]
async fn main() {
    println!("=== bracket: acquire / use / release ===");
    let released = Arc::new(AtomicBool::new(false));
    let flag = released.clone();
    let out = bracket(
        async {
            println!("  acquired connection");
            "conn"
        },
        |_conn| from_iter(vec![1, 2, 3]),
        move |_conn| {
            let flag = flag.clone();
            async move {
                println!("  released connection");
                flag.store(true, Ordering::SeqCst);
            }
        },
    )
    .collect::<Vec<_>>()
    .await;
    assert_eq!(out, vec![1, 2, 3]);
    assert!(released.load(Ordering::SeqCst), "release must run on completion");
    println!("  -> {:?}, released={}\n", out, released.load(Ordering::SeqCst));

    println!("=== bracket: release still runs when the consumer stops early ===");
    let released = Arc::new(AtomicBool::new(false));
    let flag = released.clone();
    let out = bracket(
        async { "conn" },
        |_c| from_iter(vec![1, 2, 3, 4, 5]),
        move |_c| {
            let flag = flag.clone();
            async move { flag.store(true, Ordering::SeqCst) }
        },
    )
    .take(2)
    .collect::<Vec<_>>()
    .await;
    settle().await;
    println!(
        "  took {:?} of 5, released={}\n",
        out,
        released.load(Ordering::SeqCst)
    );

    println!("=== bracket_case: errors are DATA, the stream still completes ===");
    // In-band `Err` items are values flowing through, exactly as `Left` is in
    // FS2. They do not make the stream fail, so the exit case is Completed.
    let _ = bracket_case(
        async { "tx" },
        |_t| from_iter(vec![Ok::<i32, String>(1), Err("row 2 invalid".into()), Ok(3)]),
        |_t, case: ExitCase<String>| async move {
            println!("  exit case: {}", describe(&case));
        },
    )
    .collect::<Vec<_>>()
    .await;
    settle().await;
    println!();

    println!("=== try_bracket_case: an Err TERMINATES the stream ===");
    // Use this when an error means "stop". Note 3 never arrives, and the
    // finalizer is told Errored — so a transaction could roll back here.
    let out = try_bracket_case(
        async { "tx" },
        |_t| from_iter(vec![Ok::<i32, String>(1), Err("row 2 invalid".into()), Ok(3)]),
        |_t, case: ExitCase<String>| async move {
            println!("  exit case: {}", describe(&case));
        },
    )
    .collect::<Vec<_>>()
    .await;
    settle().await;
    println!("  -> {:?}\n", out);

    println!("=== try_bracket_case: Canceled when the caller walks away ===");
    let _ = try_bracket_case(
        async { "tx" },
        |_t| from_iter(vec![Ok::<i32, String>(1), Ok(2), Ok(3), Ok(4)]),
        |_t, case: ExitCase<String>| async move {
            println!("  exit case: {}", describe(&case));
        },
    )
    .take(2)
    .collect::<Vec<_>>()
    .await;
    settle().await;
    println!();

    println!("=== on_finalize: cleanup without a resource ===");
    let out = on_finalize(from_iter(vec![1, 2, 3]), || async {
        println!("  finalizer ran");
    })
    .collect::<Vec<_>>()
    .await;
    println!("  -> {:?}\n", out);

    println!("=== on_finalize_case: told how it ended ===");
    let _ = on_finalize_case(from_iter(vec![1, 2, 3, 4]), |case: ExitCase<()>| async move {
        println!("  ended as {:?} (consumer took only 2 of 4)", case);
    })
    .take(2)
    .collect::<Vec<_>>()
    .await;
    settle().await;

    println!("=== the same as chained methods ===");
    // `on_finalize_rs2` / `on_finalize_case_rs2` chain onto any stream.
    let out = from_iter(vec![1, 2, 3, 4, 5])
        .on_finalize_rs2(|| async { println!("  on_finalize_rs2 ran") })
        .take_rs2(3)
        .collect::<Vec<_>>()
        .await;
    settle().await;
    println!("  -> {:?}", out);

    let _ = from_iter(vec![1, 2, 3, 4])
        .on_finalize_case_rs2(|case: ExitCase<()>| async move {
            println!("  on_finalize_case_rs2 ended as {:?}", case);
        })
        .take_rs2(2)
        .collect::<Vec<_>>()
        .await;
    settle().await;

    println!("\nSummary of exit cases:");
    println!("  bracket_case      Err item -> Completed (errors are data)");
    println!("  try_bracket_case  Err item -> Errored   (errors terminate)");
    println!("  both              take(n)  -> Canceled");
}
