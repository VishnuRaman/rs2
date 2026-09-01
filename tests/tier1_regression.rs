//! Regression tests for the Tier 1 correctness fixes.
//!
//! Each test here fails against the pre-fix implementation. They assert on
//! ordering, completeness, and resource lifecycle — the properties the existing
//! suite did not cover, which is why these bugs survived 89 passing tests.

use futures_util::stream::StreamExt;
use rs2_stream::advanced_analytics::*;
use rs2_stream::pipeline::builder::{Pipeline, PipelineConfig};
use rs2_stream::rs2::*;
use rs2_stream::state::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

// ---------------------------------------------------------------------------
// 1. merge interleaves instead of concatenating
// ---------------------------------------------------------------------------

#[tokio::test]
async fn merge_interleaves_both_sides() {
    // Both sides emit on the same cadence, so a correct merge alternates.
    // The old chain-based implementation returned [1,2,3,10,20,30].
    let a = from_iter(vec![1, 2, 3]).throttle_rs2(Duration::from_millis(30));
    let b = from_iter(vec![10, 20, 30]).throttle_rs2(Duration::from_millis(30));

    let out = merge(a, b).collect::<Vec<_>>().await;

    assert_eq!(out.len(), 6, "no items may be lost: {:?}", out);

    // The second half must not be entirely the second stream.
    let tail_all_from_b = out[3..].iter().all(|x| *x >= 10);
    assert!(
        !tail_all_from_b,
        "streams were concatenated, not merged: {:?}",
        out
    );
}

#[tokio::test]
async fn merge_does_not_wait_for_a_slow_first_stream() {
    // If s1 is slow, s2's items must still come through promptly.
    let slow = from_iter(vec![1, 2]).throttle_rs2(Duration::from_millis(200));
    let fast = from_iter(vec![10, 20, 30]);

    let out = merge(slow, fast).take(3).collect::<Vec<_>>().await;

    // `throttle` emits its first item immediately and only then sleeps, so
    // either side may legitimately win the first slot. What distinguishes a
    // real merge is what happens next: while `slow` is parked for 200ms, the
    // remaining slots must go to `fast`. The concatenating implementation
    // returned [1, 2, 10] — just one item from `fast`.
    let from_fast = out.iter().filter(|x| **x >= 10).count();
    assert!(
        from_fast >= 2,
        "fast stream starved while slow one was parked: {:?}",
        out
    );
}

// ---------------------------------------------------------------------------
// 2. par_eval_map preserves input order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn par_eval_map_preserves_order() {
    // Deliberately inverted completion times: the first input is the slowest.
    let input = vec![5u64, 1, 4, 2, 3];
    let out = par_eval_map(from_iter(input.clone()), 4, |x| async move {
        tokio::time::sleep(Duration::from_millis(x * 20)).await;
        x
    })
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out, input, "results must follow input order");
}

#[tokio::test]
async fn par_eval_map_is_still_concurrent() {
    // 6 items x 100ms at concurrency 6 should take ~100ms, not ~600ms.
    let start = Instant::now();
    let out = par_eval_map(from_iter(0..6u64), 6, |x| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        x
    })
    .collect::<Vec<_>>()
    .await;
    let elapsed = start.elapsed();

    assert_eq!(out, (0..6).collect::<Vec<_>>());
    assert!(
        elapsed < Duration::from_millis(400),
        "ordering fix serialized the work: took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn par_eval_map_tolerates_zero_concurrency() {
    let out = par_eval_map(from_iter(0..3), 0, |x| async move { x })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![0, 1, 2]);
}

// ---------------------------------------------------------------------------
// 3. collect_rs2 does not truncate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn collect_rs2_collects_past_the_old_buffer_cap() {
    // The old default BufferConfig silently stopped at 1_048_576.
    let n = 1_100_000usize;
    let out = from_iter(0..n).collect_rs2::<Vec<_>>().await;
    assert_eq!(out.len(), n, "collect_rs2 truncated the stream");
}

#[tokio::test]
async fn try_collect_bounded_rs2_reports_overflow() {
    let ok = from_iter(0..5).try_collect_bounded_rs2::<Vec<_>>(10).await;
    assert_eq!(ok.expect("under the limit"), (0..5).collect::<Vec<_>>());

    let over = from_iter(0..100).try_collect_bounded_rs2::<Vec<_>>(10).await;
    assert!(over.is_err(), "exceeding the bound must error, not truncate");
}

// ---------------------------------------------------------------------------
// 4 & 5. bracket / bracket_case release on every exit path
// ---------------------------------------------------------------------------

/// Release is spawned on the drop path, so give the runtime a moment to run it.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn bracket_releases_on_normal_completion() {
    let released = Arc::new(AtomicBool::new(false));
    let flag = released.clone();

    let out = bracket(
        async { 1u32 },
        |_| from_iter(vec![1, 2, 3]),
        move |_| {
            let flag = flag.clone();
            async move { flag.store(true, Ordering::SeqCst) }
        },
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out, vec![1, 2, 3]);
    assert!(released.load(Ordering::SeqCst), "release did not run");
}

#[tokio::test]
async fn bracket_releases_when_stream_ends_early() {
    let released = Arc::new(AtomicBool::new(false));
    let flag = released.clone();

    let out = bracket(
        async { 1u32 },
        |_| from_iter(vec![1, 2, 3, 4, 5]),
        move |_| {
            let flag = flag.clone();
            async move { flag.store(true, Ordering::SeqCst) }
        },
    )
    .take(2)
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out, vec![1, 2]);
    settle().await;
    assert!(
        released.load(Ordering::SeqCst),
        "release skipped on early termination"
    );
}

#[tokio::test]
async fn bracket_releases_when_consumer_drops_the_stream() {
    let released = Arc::new(AtomicBool::new(false));
    let flag = released.clone();

    {
        let mut s = bracket(
            async { 1u32 },
            |_| from_iter(vec![1, 2, 3, 4, 5]),
            move |_| {
                let flag = flag.clone();
                async move { flag.store(true, Ordering::SeqCst) }
            },
        );
        assert_eq!(s.next().await, Some(1));
        // `s` dropped here, mid-stream.
    }

    settle().await;
    assert!(released.load(Ordering::SeqCst), "release skipped on drop");
}

#[tokio::test]
async fn bracket_releases_exactly_once() {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();

    bracket(
        async { 1u32 },
        |_| from_iter(vec![1, 2, 3]),
        move |_| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        },
    )
    .collect::<Vec<_>>()
    .await;

    settle().await;
    assert_eq!(count.load(Ordering::SeqCst), 1, "release ran more than once");
}

#[tokio::test]
async fn bracket_case_reports_completed_on_success() {
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = seen.clone();

    let _ = bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Ok(2)]),
        move |_, case: ExitCase<String>| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(match case {
                    ExitCase::Completed => "completed".to_string(),
                    ExitCase::Canceled => "canceled".to_string(),
                    ExitCase::Errored(e) => format!("errored:{}", e),
                });
            }
        },
    )
    .collect::<Vec<_>>()
    .await;

    settle().await;
    assert_eq!(seen.lock().unwrap().as_deref(), Some("completed"));
}

#[tokio::test]
async fn bracket_case_reports_completed_for_in_band_errors() {
    // In-band `Err` items are data, not stream failure — FS2's ExitCase carries
    // a Throwable from the effect's error channel, not an element value.
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = seen.clone();

    let _ = bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Err("boom".to_string())]),
        move |_, case: ExitCase<String>| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(match case {
                    ExitCase::Completed => "completed".to_string(),
                    ExitCase::Canceled => "canceled".to_string(),
                    ExitCase::Errored(e) => format!("errored:{}", e),
                });
            }
        },
    )
    .collect::<Vec<_>>()
    .await;

    settle().await;
    assert_eq!(
        seen.lock().unwrap().as_deref(),
        Some("completed"),
        "a stream containing Err items still ran to exhaustion"
    );
}

#[tokio::test]
async fn bracket_case_reports_canceled_when_consumer_stops_early() {
    // FS2's `Canceled` case: the consumer walked away before the stream was
    // exhausted. This is the distinction that makes bracket_case worth having
    // over bracket, and it was unreachable before — the old implementation
    // passed a hardcoded ExitCase::Completed on the one path where it ran at all.
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = seen.clone();

    let _ = bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Ok(2), Ok(3), Ok(4)]),
        move |_, case: ExitCase<String>| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(match case {
                    ExitCase::Completed => "completed".to_string(),
                    ExitCase::Canceled => "canceled".to_string(),
                    ExitCase::Errored(e) => format!("errored:{}", e),
                });
            }
        },
    )
    .take(2)
    .collect::<Vec<_>>()
    .await;

    settle().await;
    assert_eq!(
        seen.lock().unwrap().as_deref(),
        Some("canceled"),
        "stopping at take(2) of 4 must report Canceled, not Completed"
    );
}

#[tokio::test]
async fn bracket_case_reports_completed_when_fully_drained() {
    // Control for the above: exhausting the stream is Completed, not Canceled.
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = seen.clone();

    let _ = bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Ok(2)]),
        move |_, case: ExitCase<String>| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(match case {
                    ExitCase::Completed => "completed".to_string(),
                    ExitCase::Canceled => "canceled".to_string(),
                    ExitCase::Errored(e) => format!("errored:{}", e),
                });
            }
        },
    )
    .collect::<Vec<_>>()
    .await;

    settle().await;
    assert_eq!(seen.lock().unwrap().as_deref(), Some("completed"));
}

// ---------------------------------------------------------------------------
// 6. stateful throttle actually throttles
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Ev {
    user: String,
    n: u32,
}

fn events(user: &str, count: u32) -> Vec<Ev> {
    (0..count)
        .map(|n| Ev {
            user: user.to_string(),
            n,
        })
        .collect()
}

#[tokio::test]
async fn stateful_throttle_drop_sheds_excess() {
    // 10 items, limit 3 per 60s window: only 3 may survive.
    let out = futures_util::stream::iter(events("u1", 10))
        .stateful_throttle_drop_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user.clone()),
            3,
            Duration::from_secs(60),
            |e| e,
        )
        .collect::<Vec<_>>()
        .await;

    let emitted = out.iter().filter(|r| r.is_ok()).count();
    assert_eq!(emitted, 3, "drop variant did not enforce the rate limit");
}

#[tokio::test]
async fn stateful_throttle_drop_is_per_key() {
    let mut all = events("u1", 5);
    all.extend(events("u2", 5));

    let out = futures_util::stream::iter(all)
        .stateful_throttle_drop_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user.clone()),
            2,
            Duration::from_secs(60),
            |e| e,
        )
        .collect::<Vec<_>>()
        .await;

    let emitted = out.iter().filter(|r| r.is_ok()).count();
    assert_eq!(emitted, 4, "each key gets its own budget of 2");
}

#[tokio::test]
async fn stateful_fold_evicts_by_insertion_age_not_key_ordering() {
    // Eviction only kicks in above MAX_HASHMAP_KEYS (10_000), on cleanup passes
    // every CLEANUP_INTERVAL (1_000) items, so this needs a genuinely large key
    // space. 12_000 distinct keys triggers two eviction passes.
    //
    // Under the old lexicographic policy, which key got dropped depended on its
    // *name*. Under insertion-age eviction, the earliest-inserted keys go first
    // and later ones are still present.
    const N: u32 = 12_000;

    // Insert in DESCENDING key order, so insertion age and lexicographic order
    // point opposite ways. This is what makes the test discriminating: the old
    // policy sorted key names and dropped the smallest — which here are the
    // *newest* keys — while age-ordered eviction drops the largest names first
    // because those arrived earliest.
    let mut evs: Vec<Ev> = (0..N)
        .rev()
        .map(|n| Ev {
            user: format!("k{:05}", n),
            n,
        })
        .collect();

    // "k11999" arrived first; "k00000" arrived last.
    let first_inserted = format!("k{:05}", N - 1);
    let last_inserted = "k00000".to_string();
    evs.push(Ev {
        user: first_inserted.clone(),
        n: 0,
    });
    evs.push(Ev {
        user: last_inserted.clone(),
        n: 0,
    });

    let out = futures_util::stream::iter(evs)
        .stateful_fold_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user.clone()),
            0u64,
            |acc, _item, _state| Box::pin(async move { Ok(acc + 1) }),
        )
        .collect::<Vec<_>>()
        .await;

    let vals: Vec<u64> = out.iter().filter_map(|r| r.as_ref().ok()).copied().collect();
    let first_inserted_acc = vals[vals.len() - 2];
    let last_inserted_acc = vals[vals.len() - 1];

    assert_eq!(
        first_inserted_acc, 1,
        "the earliest-inserted key should have been evicted and restarted \
         from `initial` (lexicographic eviction would have kept it)"
    );
    assert_eq!(
        last_inserted_acc, 2,
        "the most recently inserted key was evicted; its accumulator restarted \
         (this is what lexicographic eviction did wrong)"
    );
}

// ---------------------------------------------------------------------------
// 8. pipeline branch delivers to every sink
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_branch_delivers_all_items_to_both_sinks() {
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    let (a2, b2) = (a.clone(), b.clone());

    let result = Pipeline::<u32>::new()
        .with_config(PipelineConfig {
            name: "branch-test".into(),
            buffer_size: 1024,
            enable_metrics: false,
        })
        .source(|| from_iter(0..100u32))
        .branch(
            "fan-out",
            move |s| {
                let a2 = a2.clone();
                Box::pin(async move {
                    let n = s.collect::<Vec<_>>().await.len();
                    a2.fetch_add(n, Ordering::SeqCst);
                })
            },
            move |s| {
                let b2 = b2.clone();
                Box::pin(async move {
                    let n = s.collect::<Vec<_>>().await.len();
                    b2.fetch_add(n, Ordering::SeqCst);
                })
            },
        )
        .run()
        .await;

    assert!(result.is_ok(), "pipeline failed: {:?}", result.err());
    // Old behaviour: the feeder raced ahead of subscribe and both were 0.
    assert_eq!(a.load(Ordering::SeqCst), 100, "sink A missed items");
    assert_eq!(b.load(Ordering::SeqCst), 100, "sink B missed items");
}

// ---------------------------------------------------------------------------
// 9. time-windowed join
// ---------------------------------------------------------------------------

#[tokio::test]
async fn time_window_join_matches_across_both_streams() {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let left = from_iter(vec![(1u32, base), (2, base + Duration::from_secs(1))]);
    let right = from_iter(vec![(1u32, base), (2, base + Duration::from_secs(1))]);

    let out = join_with_time_window(
        left,
        right,
        TimeJoinConfig {
            window_size: Duration::from_secs(10),
            watermark_delay: Duration::from_secs(0),
        },
        |e: &(u32, SystemTime)| e.1,
        |e: &(u32, SystemTime)| e.1,
        |a, b| (a, b),
        id_key_selector(),
    )
    .collect::<Vec<_>>()
    .await;

    // Key-matched: exactly one join per id.
    assert_eq!(out.len(), 2, "expected one join per id, got {:?}", out.len());
    let mut ids: Vec<u32> = out.iter().map(|((a, _), _)| *a).collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2]);
}

type IdKey = fn(&(u32, SystemTime)) -> u32;

/// Key selector matching on the numeric id of each side.
fn id_key_selector() -> Option<(IdKey, IdKey)> {
    fn k(e: &(u32, SystemTime)) -> u32 {
        e.0
    }
    Some((k as IdKey, k as IdKey))
}

#[tokio::test]
async fn time_window_join_scales_linearly() {
    // The join used to scan the whole opposite buffer per event, and prune the
    // buffers on every event as well — two independent O(n) costs per item.
    // Measured at n=4000: 518ms before the hash index, 15ms after.
    async fn run(n: u32) -> Duration {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let left: Vec<(u32, SystemTime)> = (0..n)
            .map(|i| (i, base + Duration::from_millis(i as u64)))
            .collect();
        let right = left.clone();

        let start = Instant::now();
        let out = join_with_time_window(
            from_iter(left),
            from_iter(right),
            TimeJoinConfig {
                window_size: Duration::from_secs(600),
                watermark_delay: Duration::from_secs(0),
            },
            |e: &(u32, SystemTime)| e.1,
            |e: &(u32, SystemTime)| e.1,
            |a, b| (a, b),
            id_key_selector(),
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(out.len(), n as usize, "one match per distinct key");
        start.elapsed()
    }

    let elapsed = run(4000).await;
    assert!(
        elapsed < Duration::from_millis(150),
        "join is still scanning the whole buffer: 4000 events took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn time_window_join_cross_join_still_works() {
    // With no key selector every pair must be considered, so bucketing must not
    // partition the buffers.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let left = from_iter(vec![(1u32, base), (2u32, base)]);
    let right = from_iter(vec![(9u32, base), (8u32, base)]);

    let out = join_with_time_window(
        left,
        right,
        TimeJoinConfig {
            window_size: Duration::from_secs(600),
            watermark_delay: Duration::from_secs(0),
        },
        |e: &(u32, SystemTime)| e.1,
        |e: &(u32, SystemTime)| e.1,
        |a, b| (a, b),
        None::<(IdKey, IdKey)>,
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out.len(), 4, "2x2 cross join must yield 4 pairs: {:?}", out);
}

#[tokio::test]
async fn time_window_join_does_not_match_across_different_keys() {
    // Guards the hash-bucket index: distinct keys must never join, and the
    // equality check must still gate any hash collision.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let left: Vec<(u32, SystemTime)> = (0..200).map(|i| (i, base)).collect();
    let right: Vec<(u32, SystemTime)> = (200..400).map(|i| (i, base)).collect();

    let out = join_with_time_window(
        from_iter(left),
        from_iter(right),
        TimeJoinConfig {
            window_size: Duration::from_secs(600),
            watermark_delay: Duration::from_secs(0),
        },
        |e: &(u32, SystemTime)| e.1,
        |e: &(u32, SystemTime)| e.1,
        |a, b| (a, b),
        id_key_selector(),
    )
    .collect::<Vec<_>>()
    .await;

    assert!(out.is_empty(), "no keys overlap, expected no joins: {:?}", out.len());
}

#[tokio::test]
async fn time_window_join_does_not_duplicate_pairs() {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    // Two left events sharing an identical timestamp. The old dedup keyed on
    // (t1_nanos, t2_nanos) and would silently drop the second join.
    let left = from_iter(vec![(1u32, base), (2u32, base)]);
    let right = from_iter(vec![(9u32, base)]);

    let out = join_with_time_window(
        left,
        right,
        TimeJoinConfig {
            window_size: Duration::from_secs(10),
            watermark_delay: Duration::from_secs(0),
        },
        |e: &(u32, SystemTime)| e.1,
        |e: &(u32, SystemTime)| e.1,
        |a, b| (a, b),
        None::<(IdKey, IdKey)>,
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(
        out.len(),
        2,
        "both left events must join the right event: {:?}",
        out.len()
    );
}

// ---------------------------------------------------------------------------
// 3b. collect_vec_with_config_rs2 honours BufferConfig without truncating
// ---------------------------------------------------------------------------

#[tokio::test]
async fn collect_vec_with_config_honours_initial_capacity() {
    use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};

    let config = BufferConfig {
        initial_capacity: 4096,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Exponential(2.0),
    };
    let out = from_iter(0..10u32)
        .collect_vec_with_config_rs2(config)
        .await
        .expect("growing strategy never errors");

    assert_eq!(out, (0..10).collect::<Vec<_>>());
    assert!(
        out.capacity() >= 4096,
        "initial_capacity was ignored: capacity {}",
        out.capacity()
    );
}

#[tokio::test]
async fn collect_vec_with_config_never_truncates() {
    use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};

    // max_capacity is a reservation ceiling, not an item limit. The old
    // implementation dropped everything past it.
    let config = BufferConfig {
        initial_capacity: 8,
        max_capacity: Some(16),
        growth_strategy: GrowthStrategy::Exponential(1.5),
    };
    let out = from_iter(0..5000u32)
        .collect_vec_with_config_rs2(config)
        .await
        .expect("growing strategy never errors");

    assert_eq!(out.len(), 5000, "max_capacity must not truncate the stream");
}

#[tokio::test]
async fn collect_vec_with_config_survives_shrinking_growth_factor() {
    use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};

    // Exponential(f) with f < 1 made the old code compute a target below the
    // current capacity and underflow on the subtraction.
    let config = BufferConfig {
        initial_capacity: 64,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Exponential(0.5),
    };
    let out = from_iter(0..1000u32)
        .collect_vec_with_config_rs2(config)
        .await
        .expect("growing strategy never errors");
    assert_eq!(out.len(), 1000);
}

#[tokio::test]
async fn collect_vec_with_config_handles_fixed_and_linear() {
    use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};

    let config = BufferConfig {
        initial_capacity: 16,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Linear(32),
    };
    let out = from_iter(0..2000u32)
        .collect_vec_with_config_rs2(config)
        .await
        .expect("Linear grows");
    assert_eq!(out.len(), 2000, "Linear lost items");
}

#[tokio::test]
async fn collect_vec_with_config_fixed_errors_instead_of_truncating() {
    use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};

    // `Fixed` is documented as "fixed size, don't grow". A user setting it
    // expects a cap, and the only honest way to enforce a cap without dropping
    // data is to fail.
    let config = BufferConfig {
        initial_capacity: 16,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Fixed,
    };
    let result = from_iter(0..2000u32).collect_vec_with_config_rs2(config).await;
    assert!(
        result.is_err(),
        "Fixed must report the overflow, not silently grow or truncate"
    );

    // Within the fixed size it succeeds and does not over-allocate.
    let config = BufferConfig {
        initial_capacity: 16,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Fixed,
    };
    let out = from_iter(0..16u32)
        .collect_vec_with_config_rs2(config)
        .await
        .expect("exactly at the fixed size");
    assert_eq!(out.len(), 16);
}
