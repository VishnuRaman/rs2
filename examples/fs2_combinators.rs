//! Combinators that mirror Scala FS2, with the naming differences called out.
//!
//! Run with: `cargo run --example fs2_combinators`

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    println!("=== eval_tap (FS2 evalTap): observe without changing ===");
    let out = eval_tap(from_iter(vec![1, 2, 3]), |x: &i32| {
        let x = *x;
        async move { println!("  saw {}", x) }
    })
    .collect::<Vec<_>>()
    .await;
    assert_eq!(out, vec![1, 2, 3], "eval_tap passes elements through unchanged");
    println!("  -> {:?}\n", out);

    println!("=== zip_with_index (FS2 zipWithIndex) ===");
    let out = zip_with_index(from_iter(vec!["a", "b", "c"]))
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?}\n", out);

    println!("=== chunk_n (FS2 chunkN): control the trailing partial chunk ===");
    let kept = chunk_n(from_iter(0..7u32), 3, true).collect::<Vec<_>>().await;
    let dropped = chunk_n(from_iter(0..7u32), 3, false).collect::<Vec<_>>().await;
    assert_eq!(kept, vec![vec![0, 1, 2], vec![3, 4, 5], vec![6]]);
    assert_eq!(dropped, vec![vec![0, 1, 2], vec![3, 4, 5]]);
    println!("  allow_fewer=true  -> {:?}", kept);
    println!("  allow_fewer=false -> {:?}\n", dropped);

    println!("=== group_within (FS2 groupWithin): chunk by size OR time ===");
    // Two items arrive immediately, then a long gap. The chunk size of 10 is
    // never reached, so the 150ms timeout is what makes progress happen.
    let src = async_stream::stream! {
        yield 1u32;
        yield 2u32;
        tokio::time::sleep(Duration::from_millis(400)).await;
        yield 3u32;
    }
    .boxed();
    let start = Instant::now();
    let out = group_within(src, 10, Duration::from_millis(150))
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?} in {:?}", out, start.elapsed());
    println!("     (first chunk flushed on the timeout, not on a full buffer)\n");

    println!("=== interleave (FS2 interleave): stops at the shorter side ===");
    let out = interleave(from_iter(vec![1, 3, 5]), from_iter(vec![2]))
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?}", out);

    println!("=== interleave_all (FS2 interleaveAll): continues with the longer ===");
    let out = interleave_all(from_iter(vec![1, 3, 5]), from_iter(vec![2]))
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?}", out);

    println!("=== interleave_many: round-robin over many, no FS2 equivalent ===");
    let out = interleave_many(vec![
        from_iter(vec![1, 4]),
        from_iter(vec![2, 5, 7]),
        from_iter(vec![3]),
    ])
    .collect::<Vec<_>>()
    .await;
    println!("  -> {:?}\n", out);

    println!("=== race vs merge_either: two different things ===");
    // `race` emits from whichever side is ready first, same item type.
    let out = race(from_iter(vec![1, 2, 3]), from_iter(vec![10, 20]))
        .collect::<Vec<_>>()
        .await;
    println!("  race        -> {:?}", out);

    // `merge_either` is FS2's `either`: it TAGS each value with its branch, and
    // the two sides may have different types. RS2's `either` is a deprecated
    // alias for `race`, so the FS2 combinator needed a different name.
    let tagged = merge_either(from_iter(vec![1u32, 2]), from_iter(vec!["a", "b"]))
        .collect::<Vec<_>>()
        .await;
    println!("  merge_either -> {:?}", tagged);

    // `Either::left` / `Either::right` turn a tagged value back into an Option,
    // which makes it easy to split the merged stream apart again.
    let numbers: Vec<u32> = tagged.iter().cloned().filter_map(Either::left).collect();
    let words: Vec<&str> = tagged.into_iter().filter_map(Either::right).collect();
    println!("  split back -> numbers {:?}, words {:?}\n", numbers, words);

    println!("=== metered (FS2 metered) — the FS2 name for throttle ===");
    let start = Instant::now();
    let out = metered(from_iter(0..3u32), Duration::from_millis(100))
        .collect::<Vec<_>>()
        .await;
    println!("  -> {:?} in {:?}", out, start.elapsed());
    println!("     (~200ms: the first item is immediate, then one gap each)");

    println!("\n=== the same combinators as chained methods ===");
    // Every free function above also exists as an `_rs2` method on
    // RS2StreamExt, which is usually how you will reach for them: the method
    // form chains, the free-function form nests.
    let out = from_iter(1..=8u32)
        .eval_tap_rs2(|x: &u32| {
            let x = *x;
            async move {
                if x == 1 {
                    println!("  eval_tap_rs2 saw the first item ({})", x);
                }
            }
        })
        .zip_with_index_rs2()
        .map_rs2(|(value, idx)| format!("{}@{}", value, idx))
        .chunk_n_rs2(3, false)
        .collect::<Vec<_>>()
        .await;
    println!("  chained -> {:?}", out);
    println!("     (chunk_n_rs2(3, false) dropped the short final chunk)");

    let paced = from_iter(0..3u32)
        .metered_rs2(Duration::from_millis(50))
        .collect::<Vec<_>>()
        .await;
    println!("  metered_rs2 -> {:?}", paced);

    let woven = from_iter(vec![1, 3, 5])
        .interleave_rs2(from_iter(vec![2, 4, 6]))
        .collect::<Vec<_>>()
        .await;
    println!("  interleave_rs2 -> {:?}", woven);

    let woven_all = from_iter(vec![1, 3, 5])
        .interleave_all_rs2(from_iter(vec![2]))
        .collect::<Vec<_>>()
        .await;
    println!("  interleave_all_rs2 -> {:?}", woven_all);

    let tagged = from_iter(vec![1u32, 2])
        .merge_either_rs2(from_iter(vec!["x", "y"]))
        .collect::<Vec<_>>()
        .await;
    println!("  merge_either_rs2 -> {:?}", tagged);

    let raced = from_iter(vec![1, 2])
        .race_rs2(from_iter(vec![10, 20]))
        .collect::<Vec<_>>()
        .await;
    println!("  race_rs2 -> {:?}", raced);

    let batched = from_iter(0..7u32)
        .group_within_rs2(3, Duration::from_millis(50))
        .collect::<Vec<_>>()
        .await;
    println!("  group_within_rs2 -> {:?}", batched);

    let interrupted = from_iter(0..)
        .throttle_rs2(Duration::from_millis(20))
        .interrupt_when_rs2(tokio::time::sleep(Duration::from_millis(120)))
        .collect::<Vec<_>>()
        .await;
    println!("  interrupt_when_rs2 -> {} items before the signal", interrupted.len());

    let skipped = from_iter(0..6u32).skip_rs2(4).collect::<Vec<_>>().await;
    println!("  skip_rs2(4) -> {:?}", skipped);
    println!("     (this used to be `drop_rs2`, which shadowed std::mem::drop)");
}
