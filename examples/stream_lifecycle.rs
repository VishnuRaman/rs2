//! Combining and terminating streams: concat, interrupt_when, reduce, fold.
//!
//! Run with: `cargo run --example stream_lifecycle`

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("=== concat: run streams one after another ===");
    // Unlike `merge`/`race`, this is strictly sequential: the second stream is
    // not touched until the first is exhausted.
    let out = concat(vec![
        from_iter(vec![1, 2]),
        from_iter(vec![3, 4]),
        from_iter(vec![5]),
    ])
    .collect::<Vec<_>>()
    .await;
    assert_eq!(out, vec![1, 2, 3, 4, 5], "concat is strictly sequential");
    println!("  -> {:?}\n", out);

    println!("=== merge: both sides run concurrently ===");
    let a = from_iter(vec![1, 2, 3]).throttle_rs2(Duration::from_millis(30));
    let b = from_iter(vec![10, 20, 30]).throttle_rs2(Duration::from_millis(30));
    let out = merge(a, b).collect::<Vec<_>>().await;
    println!("  -> {:?}", out);
    println!("     (contrast with concat: values from both sides are mixed)\n");

    println!("=== interrupt_when: stop on an external signal ===");
    // The source is infinite; the signal is what ends it.
    let ticking = from_iter(0..).throttle_rs2(Duration::from_millis(40));
    let signal = sleep(Duration::from_millis(250));
    let start = Instant::now();
    let out = interrupt_when(ticking, signal).collect::<Vec<_>>().await;
    println!("  got {} items in {:?} before the signal fired", out.len(), start.elapsed());
    println!("     (an infinite stream terminated cleanly)\n");

    println!("=== fold: reduce to a single value, with a seed ===");
    let total = fold(from_iter(1..=10u32), 0u32, |acc, x| async move { acc + x }).await;
    assert_eq!(total, 55);
    println!("  sum 1..=10 -> {}\n", total);

    println!("=== reduce: like fold, but seeded by the first element ===");
    let max = reduce(from_iter(vec![3, 9, 2, 7]), |a, b| async move { a.max(b) }).await;
    assert_eq!(max, Some(9));
    println!("  max -> {:?}", max);

    // The distinction that matters: reduce has nothing to return for an empty
    // stream, so it yields None rather than inventing a value.
    let empty_max = reduce(from_iter(Vec::<i32>::new()), |a, b| async move { a.max(b) }).await;
    assert_eq!(empty_max, None, "reduce has no seed, so an empty stream gives None");
    println!("  max of empty stream -> {:?}\n", empty_max);

    println!("=== scan: fold that emits every intermediate value ===");
    let running = scan(from_iter(1..=5u32), 0u32, |acc, x| acc + x)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(running, vec![1, 3, 6, 10, 15]);
    println!("  running totals -> {:?}", running);
}
