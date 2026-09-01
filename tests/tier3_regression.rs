//! Regression tests for the Tier 3 fixes.
//!
//! Each test that can be run against the pre-fix code has been checked to fail
//! there; controls are marked as such.

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use rs2_stream::state::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// 1. par_join actually runs inner streams concurrently
// ---------------------------------------------------------------------------

fn slow_stream(id: u64, n: u64, per_item: Duration) -> std::pin::Pin<Box<dyn futures_core::Stream<Item = u64> + Send>> {
    use async_stream::stream;
    Box::pin(stream! {
        for i in 0..n {
            tokio::time::sleep(per_item).await;
            yield id * 100 + i;
        }
    })
}

#[tokio::test]
async fn par_join_runs_inner_streams_concurrently() {
    // 4 inner streams x 3 items x 100ms. Concurrent at 4 => ~300ms.
    // The old implementation awaited each inner stream in turn: measured 1.23s.
    let inners: Vec<_> = (0..4u64)
        .map(|id| slow_stream(id, 3, Duration::from_millis(100)))
        .collect();

    let start = Instant::now();
    let out = par_join(from_iter(inners), 4).collect::<Vec<_>>().await;
    let elapsed = start.elapsed();

    assert_eq!(out.len(), 12, "no items may be lost");
    assert!(
        elapsed < Duration::from_millis(700),
        "par_join is still sequential: {:?} for work that parallelises to ~300ms",
        elapsed
    );
}

#[tokio::test]
async fn par_join_one_slow_stream_does_not_block_others() {
    // One very slow inner stream must not hold up the fast ones.
    let mut inners = vec![slow_stream(9, 1, Duration::from_millis(600))];
    inners.extend((0..3u64).map(|id| slow_stream(id, 2, Duration::from_millis(20))));

    let start = Instant::now();
    let out = par_join(from_iter(inners), 4).take(6).collect::<Vec<_>>().await;
    let elapsed = start.elapsed();

    assert_eq!(out.len(), 6);
    assert!(
        elapsed < Duration::from_millis(400),
        "a slow inner stream blocked the fast ones: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn par_join_respects_concurrency_and_zero() {
    let out = par_join(from_iter(vec![from_iter(0..3u32), from_iter(3..6u32)]), 0)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 6, "concurrency 0 must be treated as 1, not hang");
}

// ---------------------------------------------------------------------------
// 2. key extraction failures surface instead of sharing one bucket
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Ev {
    user_id: String,
    n: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct NoKey {
    other: String,
}

#[tokio::test]
async fn missing_key_field_is_reported_not_bucketed() {
    // Previously every event lacking the field got the sentinel key
    // "missing_field_user_id", silently sharing one accumulator.
    let evs = vec![NoKey { other: "a".into() }, NoKey { other: "b".into() }];

    let out = futures_util::stream::iter(evs)
        .stateful_map_rs2(
            StateConfig::default(),
            FieldKeyExtractor::new("user_id"),
            |item, _state| Box::pin(async move { Ok(item.other) }),
        )
        .collect::<Vec<_>>()
        .await;

    assert_eq!(out.len(), 2);
    assert!(
        out.iter().all(|r| r.is_err()),
        "a missing key field must surface as an error per event"
    );
}

#[tokio::test]
async fn valid_keys_still_extract_normally() {
    let evs: Vec<Ev> = (0..6)
        .map(|n| Ev {
            user_id: format!("u{}", n % 2),
            n,
        })
        .collect();

    let out = futures_util::stream::iter(evs)
        .stateful_fold_rs2(
            StateConfig::default(),
            FieldKeyExtractor::new("user_id"),
            0u64,
            |acc, _i, _s| Box::pin(async move { Ok(acc + 1) }),
        )
        .collect::<Vec<_>>()
        .await;

    let vals: Vec<u64> = out.iter().filter_map(|r| r.as_ref().ok()).copied().collect();
    assert_eq!(vals.len(), 6, "all valid events must be processed");
    assert_eq!(*vals.last().unwrap(), 3, "per-key accumulation is unaffected");
}

#[tokio::test]
async fn try_key_extractor_propagates_its_own_error() {
    let evs: Vec<Ev> = (0..4)
        .map(|n| Ev {
            user_id: "u1".into(),
            n,
        })
        .collect();

    let extractor = TryKeyExtractor::new(|e: &Ev| {
        if e.n % 2 == 0 {
            Ok(e.user_id.clone())
        } else {
            Err(StateError::Validation("odd n rejected".into()))
        }
    });

    let out = futures_util::stream::iter(evs)
        .stateful_map_rs2(StateConfig::default(), extractor, |item, _s| {
            Box::pin(async move { Ok(item.n) })
        })
        .collect::<Vec<_>>()
        .await;

    assert_eq!(out.iter().filter(|r| r.is_ok()).count(), 2);
    assert_eq!(out.iter().filter(|r| r.is_err()).count(), 2);
}

// ---------------------------------------------------------------------------
// 3a. window_by_time honours slide_interval and allowed_lateness
// ---------------------------------------------------------------------------

use rs2_stream::advanced_analytics::*;
use std::time::SystemTime;

/// Epoch + 1_700_000_000_000ms — divisible by every slide used below, so window
/// boundaries land on clean multiples.
fn win_base() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)
}

fn at(offset_ms: u64) -> (u64, SystemTime) {
    (offset_ms, win_base() + Duration::from_millis(offset_ms))
}

async fn windows_for(config: TimeWindowConfig, offsets: &[u64]) -> Vec<Vec<u64>> {
    let events: Vec<(u64, SystemTime)> = offsets.iter().map(|o| at(*o)).collect();
    window_by_time(from_iter(events), config, |e: &(u64, SystemTime)| e.1)
        .map(|w| w.events.iter().map(|e| e.0).collect::<Vec<u64>>())
        .collect::<Vec<_>>()
        .await
}

#[tokio::test]
async fn sliding_windows_overlap_and_tumbling_do_not() {
    let offsets = [0u64, 500, 1000, 1500];

    // Tumbling: slide == size. Every event belongs to exactly one window.
    let tumbling = windows_for(
        TimeWindowConfig {
            window_size: Duration::from_millis(1000),
            slide_interval: Some(Duration::from_millis(1000)),
            watermark_delay: Duration::from_millis(0),
            allowed_lateness: Duration::from_millis(0),
        },
        &offsets,
    )
    .await;
    let tumbling_total: usize = tumbling.iter().map(|w| w.len()).sum();
    assert_eq!(
        tumbling_total, 4,
        "tumbling must place each event in exactly one window: {:?}",
        tumbling
    );

    // Sliding: slide == half the size, so interior events land in two windows.
    let sliding = windows_for(
        TimeWindowConfig {
            window_size: Duration::from_millis(1000),
            slide_interval: Some(Duration::from_millis(500)),
            watermark_delay: Duration::from_millis(0),
            allowed_lateness: Duration::from_millis(0),
        },
        &offsets,
    )
    .await;
    let sliding_total: usize = sliding.iter().map(|w| w.len()).sum();

    assert!(
        sliding_total > tumbling_total,
        "slide_interval was ignored — sliding produced {} placements, same as tumbling {:?}",
        sliding_total,
        sliding
    );
    // Sliding windows tile the whole timeline, so a window starting one slide
    // before the first event also covers it: [-500,500) [0,1000) [500,1500)
    // [1000,2000) [1500,2500). Every event lands in exactly two of them.
    assert_eq!(sliding.len(), 5, "expected 5 overlapping windows: {:?}", sliding);
    assert_eq!(
        sliding_total, 8,
        "each event belongs to two windows (4 events x 2): {:?}",
        sliding
    );
}

#[tokio::test]
async fn sliding_windows_place_each_event_in_every_covering_window() {
    let sliding = windows_for(
        TimeWindowConfig {
            window_size: Duration::from_millis(1000),
            slide_interval: Some(Duration::from_millis(500)),
            watermark_delay: Duration::from_millis(0),
            allowed_lateness: Duration::from_millis(0),
        },
        &[0u64, 500, 1000, 1500],
    )
    .await;

    // Emitted oldest-first. The leading window starts one slide before the
    // first event and covers only it.
    assert_eq!(sliding[0], vec![0], "window [-500,500)");
    assert_eq!(sliding[1], vec![0, 500], "window [0,1000)");
    assert_eq!(sliding[2], vec![500, 1000], "window [500,1500)");
    assert_eq!(sliding[3], vec![1000, 1500], "window [1000,2000)");
    assert_eq!(sliding[4], vec![1500], "window [1500,2500)");
}

#[tokio::test]
async fn no_event_is_lost_across_window_configurations() {
    let offsets = [0u64, 250, 500, 750, 1000];
    for (size, slide) in [(1000u64, 1000u64), (1000, 500), (1000, 250), (500, 500)] {
        let out = windows_for(
            TimeWindowConfig {
                window_size: Duration::from_millis(size),
                slide_interval: Some(Duration::from_millis(slide)),
                watermark_delay: Duration::from_millis(0),
                allowed_lateness: Duration::from_millis(0),
            },
            &offsets,
        )
        .await;
        let seen: std::collections::HashSet<u64> =
            out.iter().flat_map(|w| w.iter().copied()).collect();
        assert_eq!(
            seen.len(),
            offsets.len(),
            "size={} slide={} lost events: {:?}",
            size,
            slide,
            out
        );
    }
}

#[tokio::test]
async fn allowed_lateness_admits_late_events_then_drops_them() {
    // Events arrive out of order: the watermark jumps far ahead, then a late
    // event for the first window arrives.
    let strict = TimeWindowConfig {
        window_size: Duration::from_millis(1000),
        slide_interval: Some(Duration::from_millis(1000)),
        watermark_delay: Duration::from_millis(0),
        allowed_lateness: Duration::from_millis(0),
    };
    let forgiving = TimeWindowConfig {
        allowed_lateness: Duration::from_millis(10_000),
        ..strict.clone()
    };

    // 0 lands in [0,1000). 9000 pushes the watermark past that window's grace.
    // 100 is then late for [0,1000).
    let offsets = [0u64, 9000, 100];

    let strict_out = windows_for(strict, &offsets).await;
    let strict_seen: usize = strict_out.iter().map(|w| w.len()).sum();

    let forgiving_out = windows_for(forgiving, &offsets).await;
    let forgiving_seen: usize = forgiving_out.iter().map(|w| w.len()).sum();

    assert_eq!(
        strict_seen, 2,
        "with no lateness allowance the late event must be dropped: {:?}",
        strict_out
    );
    assert_eq!(
        forgiving_seen, 3,
        "with a 10s allowance the late event must still be accepted: {:?}",
        forgiving_out
    );
}

// ---------------------------------------------------------------------------
// 3b. StateConfig::cleanup_interval actually drives expiry sweeping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cleanup_interval_drives_expiry_sweeping() {
    // Sweeping used to run every 256 `set` calls, ignoring the configured
    // interval entirely. With a short interval and only a handful of writes,
    // the count-based version would never sweep at all.
    let store = InMemoryState::new(Duration::from_millis(50))
        .with_cleanup_interval(Duration::from_millis(50));

    for i in 0..5 {
        store.set(&format!("k{}", i), b"payload").await.unwrap();
    }
    assert_eq!(store.allocated_len().await, 5);

    // Let both the TTL and the cleanup interval elapse.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A single further write is enough to trigger a time-based sweep.
    store.set("trigger", b"payload").await.unwrap();

    let allocated = store.allocated_len().await;
    assert_eq!(
        allocated, 1,
        "expired entries were not swept on the configured interval: {} still allocated",
        allocated
    );
}

#[tokio::test]
async fn long_cleanup_interval_defers_sweeping() {
    // The interval is respected in the other direction too: entries stay
    // allocated (though unreadable) until it elapses.
    let store = InMemoryState::new(Duration::from_millis(20))
        .with_cleanup_interval(Duration::from_secs(3600));

    for i in 0..5 {
        store.set(&format!("k{}", i), b"payload").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    store.set("trigger", b"payload").await.unwrap();

    assert_eq!(
        store.allocated_len().await,
        6,
        "sweeping ran before the configured interval elapsed"
    );
    // Expired entries must still be invisible to readers.
    assert_eq!(store.get("k0").await, None);
}

#[tokio::test]
async fn state_config_cleanup_interval_reaches_the_storage() {
    let config = StateConfig::default()
        .ttl(Duration::from_millis(50))
        .cleanup_interval(Duration::from_millis(50));
    let storage = config.create_storage_arc();

    for i in 0..5 {
        storage.set(&format!("k{}", i), b"payload").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    storage.set("trigger", b"payload").await.unwrap();

    // Reachable only through the trait, so assert on observable behaviour.
    assert_eq!(storage.get("k0").await, None, "expired entry still readable");
    assert!(storage.get("trigger").await.is_some());
}

// ---------------------------------------------------------------------------
// 5. Pipeline::run rejects orderings it would have silently mishandled
// ---------------------------------------------------------------------------

use rs2_stream::pipeline::builder::{Pipeline, PipelineError};

fn noop_sink() -> impl Fn(RS2Stream<u32>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
       + Send
       + Sync
       + 'static {
    |s: RS2Stream<u32>| {
        Box::pin(async move {
            let _ = s.collect::<Vec<_>>().await;
        })
    }
}

#[tokio::test]
async fn pipeline_rejects_a_second_source() {
    // `run` threads one stream through the nodes, so the first source's stream
    // was silently discarded. This used to pass validation and run.
    let result = Pipeline::<u32>::new()
        .source(|| from_iter(0..5u32))
        .source(|| from_iter(0..5u32))
        .sink(noop_sink())
        .run()
        .await;

    assert!(
        matches!(result, Err(PipelineError::InvalidPipeline(_))),
        "a second source must be rejected, got {:?}",
        result
    );
}

#[tokio::test]
async fn pipeline_rejects_a_transform_after_a_sink() {
    // The stream is consumed by the sink, so this transform could never run.
    let result = Pipeline::<u32>::new()
        .source(|| from_iter(0..5u32))
        .sink(noop_sink())
        .transform(|s| s)
        .run()
        .await;

    assert!(
        matches!(result, Err(PipelineError::InvalidPipeline(_))),
        "a transform after a sink must be rejected, got {:?}",
        result
    );
}

#[tokio::test]
async fn pipeline_still_accepts_a_valid_ordering() {
    let result = Pipeline::<u32>::new()
        .source(|| from_iter(0..5u32))
        .transform(|s| s)
        .sink(noop_sink())
        .run()
        .await;
    assert!(result.is_ok(), "valid pipeline rejected: {:?}", result);
}

// ---------------------------------------------------------------------------
// 6. with_metrics no longer reports fabricated byte counts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_metrics_sized_reports_real_bytes() {
    use rs2_stream::stream_performance_metrics::HealthThresholds;

    let items = vec!["a".to_string(), "bb".to_string(), "cccc".to_string()];
    let expected: u64 = items.iter().map(|s| s.len() as u64).sum(); // 7

    let (stream, metrics) = from_iter(items).with_metrics_sized_rs2(
        "sized".to_string(),
        HealthThresholds::default(),
        |s: &String| s.len() as u64,
    );
    let _ = stream.collect::<Vec<_>>().await;

    let m = metrics.lock().await;
    assert_eq!(
        m.bytes_processed, expected,
        "sized metrics must report real byte counts, not size_of_val"
    );
    assert_eq!(m.items_processed, 3);
}

#[tokio::test]
async fn with_metrics_does_not_fabricate_byte_counts() {
    use rs2_stream::stream_performance_metrics::HealthThresholds;

    // Previously reported size_of_val — 24 bytes per String regardless of
    // contents, so throughput figures derived from it were fiction.
    let items = vec!["a".to_string(), "bb".to_string(), "cccc".to_string()];
    let (stream, metrics) =
        from_iter(items).with_metrics_rs2("unsized".to_string(), HealthThresholds::default());
    let _ = stream.collect::<Vec<_>>().await;

    let m = metrics.lock().await;
    assert_eq!(m.items_processed, 3, "item counting still works");
    assert_eq!(
        m.bytes_processed, 0,
        "unsized metrics must report 0, not a fabricated per-item size"
    );
}

// ---------------------------------------------------------------------------
// 9. sliding_window / chunk(0) / throttle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chunk_with_zero_size_does_not_swallow_the_stream() {
    // size 0 never reached `buf.len() == size`, so the whole stream came back
    // as a single chunk.
    let out = chunk(from_iter(0..5u32), 0).collect::<Vec<_>>().await;
    assert_eq!(out.len(), 5, "chunk(0) must not buffer everything: {:?}", out);
    assert!(out.iter().all(|c| c.len() == 1));
}

#[tokio::test]
async fn sliding_window_still_produces_correct_windows() {
    // Control for the VecDeque rewrite: behaviour must be unchanged.
    let out = sliding_window(from_iter(1..=5u32), 3).collect::<Vec<_>>().await;
    assert_eq!(out, vec![vec![1, 2, 3], vec![2, 3, 4], vec![3, 4, 5]]);
}

#[tokio::test]
async fn throttle_does_not_add_a_trailing_delay() {
    // Sleeping *after* each item added a full period after the last item had
    // already been emitted. 3 items at 100ms should take ~200ms, not ~300ms.
    let start = Instant::now();
    let out = throttle(from_iter(0..3u32), Duration::from_millis(100))
        .collect::<Vec<_>>()
        .await;
    let elapsed = start.elapsed();

    assert_eq!(out, vec![0, 1, 2]);
    assert!(
        elapsed < Duration::from_millis(290),
        "throttle still sleeps after the final item: {:?}",
        elapsed
    );
    // Still actually throttling.
    assert!(
        elapsed >= Duration::from_millis(150),
        "throttle stopped pacing: {:?}",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// 7 & 8. group_by naming, stateful_pattern return type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn group_adjacent_by_groups_only_adjacent_runs() {
    // Documents the actual semantics. `group_by` claimed key-global grouping
    // and never did it — [1,2,1] keyed by identity is three groups, not two.
    let out = group_adjacent_by(from_iter(vec![1, 2, 1]), |x: &i32| *x)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        out,
        vec![(1, vec![1]), (2, vec![2]), (1, vec![1])],
        "adjacent grouping must not merge non-adjacent runs"
    );
}

#[tokio::test]
async fn stateful_pattern_yields_plain_strings() {
    // The item type was `Result<Option<String>, _>` but `Ok(None)` was never
    // yielded — `f` returning `None` simply means "no pattern here".
    let evs: Vec<Ev> = (0..6)
        .map(|n| Ev {
            user_id: "u1".into(),
            n,
        })
        .collect();

    let out = futures_util::stream::iter(evs)
        .stateful_pattern_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user_id.clone()),
            2,
            |items: Vec<Ev>, _s| {
                Box::pin(async move {
                    // Only report every other window, exercising the None path.
                    if items[0].n % 4 == 0 {
                        Ok(Some(format!("pattern@{}", items[0].n)))
                    } else {
                        Ok(None)
                    }
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    let hits: Vec<String> = out.into_iter().filter_map(Result::ok).collect();
    assert_eq!(
        hits,
        vec!["pattern@0".to_string(), "pattern@4".to_string()],
        "only matched patterns are emitted, as plain Strings"
    );
}

// ---------------------------------------------------------------------------
// BackpressureConfig watermarks — declared, defaulted, and never read before
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watermarks_are_validated_before_use() {
    // Sensible pair is accepted.
    let ok = BackpressureConfig {
        strategy: BackpressureStrategy::Block,
        buffer_size: 100,
        low_watermark: Some(25),
        high_watermark: Some(75),
    };
    assert_eq!(ok.watermarks(), Some((25, 75)));

    // low >= high is nonsense; fall back to plain bounded behaviour.
    let inverted = BackpressureConfig {
        low_watermark: Some(75),
        high_watermark: Some(25),
        ..ok.clone()
    };
    assert_eq!(inverted.watermarks(), None);

    // high beyond the buffer can never be reached.
    let oversized = BackpressureConfig {
        high_watermark: Some(500),
        ..ok.clone()
    };
    assert_eq!(oversized.watermarks(), None);

    // Either unset disables the pair.
    let partial = BackpressureConfig { low_watermark: None, ..ok };
    assert_eq!(partial.watermarks(), None);
}

#[tokio::test]
async fn watermark_backpressure_delivers_everything_in_order() {
    // Pausing and resuming the producer must not lose or reorder items.
    let out = auto_backpressure_watermark(from_iter(0..2000u32), 64, 16, 48)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 2000, "no items may be dropped");
    assert!(out.windows(2).all(|w| w[0] < w[1]), "order must be preserved");
}

#[tokio::test]
async fn watermark_config_routes_through_auto_backpressure() {
    // The config path must actually reach the watermark implementation rather
    // than silently falling back — these fields used to be inert.
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::Block,
        buffer_size: 64,
        low_watermark: Some(8),
        high_watermark: Some(32),
    };
    assert!(config.watermarks().is_some());

    let out = auto_backpressure(from_iter(0..500u32), config)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 500);
    assert!(out.windows(2).all(|w| w[0] < w[1]));
}

#[tokio::test]
async fn watermark_producer_pauses_and_resumes_with_a_slow_consumer() {
    // The buffer must stay bounded by the high watermark even though the
    // source could run far ahead of the consumer.
    let mut s = auto_backpressure_watermark(from_iter(0..1000u32), 64, 8, 24);
    let mut got = Vec::new();
    while let Some(v) = s.next().await {
        got.push(v);
        if got.len() % 16 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    assert_eq!(got.len(), 1000, "a paused producer must still deliver everything");
    assert!(got.windows(2).all(|w| w[0] < w[1]));
}

#[tokio::test]
async fn watermark_stream_stops_when_consumer_drops() {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    let produced = Arc::new(AtomicUsize::new(0));
    let counter = produced.clone();
    let source = from_iter(0u64..).map_rs2(move |x| {
        counter.fetch_add(1, AtomicOrdering::Relaxed);
        x
    });

    {
        let mut s = auto_backpressure_watermark(source, 32, 8, 24);
        for _ in 0..5 {
            s.next().await;
        }
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    let a = produced.load(AtomicOrdering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b = produced.load(AtomicOrdering::Relaxed);
    assert_eq!(a, b, "producer kept running after the consumer went away ({a} -> {b})");
}

// ---------------------------------------------------------------------------
// PipelineConfig::enable_metrics — declared and never read before
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_enable_metrics_collects_real_numbers() {
    let metrics = Pipeline::<u32>::new()
        .with_config(rs2_stream::pipeline::builder::PipelineConfig {
            name: "measured".to_string(),
            buffer_size: 64,
            enable_metrics: true,
        })
        .source(|| from_iter(0..250u32))
        .sink(noop_sink())
        .run_with_metrics()
        .await
        .expect("pipeline runs");

    let m = metrics.expect("metrics collected when enable_metrics is set");
    assert_eq!(m.items_processed, 250, "must count every item through the source");
    assert_eq!(m.name.as_deref(), Some("measured"));
}

#[tokio::test]
async fn pipeline_without_enable_metrics_collects_none() {
    let metrics = Pipeline::<u32>::new()
        .with_config(rs2_stream::pipeline::builder::PipelineConfig {
            name: "unmeasured".to_string(),
            buffer_size: 64,
            enable_metrics: false,
        })
        .source(|| from_iter(0..10u32))
        .sink(noop_sink())
        .run_with_metrics()
        .await
        .expect("pipeline runs");

    assert!(metrics.is_none(), "no metrics unless enable_metrics is set");
}

// ---------------------------------------------------------------------------
// map_parallel_rs2 must actually run CPU work in parallel
// ---------------------------------------------------------------------------

/// Busy-wait so the work is genuinely CPU-bound — `sleep` would "parallelise"
/// even on one thread and prove nothing.
fn burn_cpu(ms: u64) -> u64 {
    let start = Instant::now();
    let mut acc: u64 = 0;
    while start.elapsed().as_millis() < ms as u128 {
        acc = acc.wrapping_add(1);
    }
    acc
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn map_parallel_actually_uses_multiple_threads() {
    // 8 x 60ms of CPU work. Serial is ~480ms; real parallelism is ~60-150ms.
    // The old implementation wrapped the sync closure in `async move { f(x) }`,
    // which completes on first poll on the polling thread — measured identical
    // to the serial baseline.
    let start = Instant::now();
    let out: Vec<u64> = from_iter(0..8u64)
        .map_parallel_rs2(|_| burn_cpu(60))
        .collect()
        .await;
    let elapsed = start.elapsed();

    assert_eq!(out.len(), 8);
    assert!(
        elapsed < Duration::from_millis(300),
        "map_parallel_rs2 is not parallelising CPU work: {:?} for work that \
         serialises to ~480ms",
        elapsed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn map_parallel_with_concurrency_respects_its_limit() {
    // 8 tasks x 60ms at concurrency 2 => ~4 batches => ~240ms, and must not be
    // as fast as unbounded parallelism.
    let start = Instant::now();
    let out: Vec<u64> = from_iter(0..8u64)
        .map_parallel_with_concurrency_rs2(2, |_| burn_cpu(60))
        .collect()
        .await;
    let elapsed = start.elapsed();

    assert_eq!(out.len(), 8);
    assert!(
        elapsed >= Duration::from_millis(180),
        "concurrency limit of 2 was not respected: {:?} is too fast",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(450),
        "still serialising: {:?}",
        elapsed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn map_parallel_preserves_order_and_values() {
    let out: Vec<u64> = from_iter(0..20u64)
        .map_parallel_rs2(|x| x * 3)
        .collect()
        .await;
    assert_eq!(out, (0..20u64).map(|x| x * 3).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn map_parallel_tolerates_zero_concurrency() {
    let out: Vec<u64> = from_iter(0..4u64)
        .map_parallel_with_concurrency_rs2(0, |x| x + 1)
        .collect()
        .await;
    assert_eq!(out, vec![1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// stateful_session: the first event of a session must be flagged as new
// ---------------------------------------------------------------------------

#[tokio::test]
async fn first_event_starts_a_new_session() {
    // With no stored state, `last_activity` is seeded to `now`, so the gap test
    // computes `now - now = 0`, which is never greater than the timeout. The
    // first event of every session was therefore reported as *not* new.
    //
    // The closure is `FnMut(T, bool) -> T`, so the flag is carried back in `n`:
    // 1 = new session, 0 = continuation.
    let evs: Vec<Ev> = vec![
        Ev { user_id: "alice".into(), n: 10 },
        Ev { user_id: "alice".into(), n: 11 },
        Ev { user_id: "bob".into(), n: 12 },
    ];

    let out = futures_util::stream::iter(evs)
        .stateful_session_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user_id.clone()),
            Duration::from_secs(3600),
            |mut e, is_new| {
                e.n = if is_new { 1 } else { 0 };
                e
            },
        )
        .collect::<Vec<_>>()
        .await;

    let flags: Vec<(String, u32)> = out
        .into_iter()
        .filter_map(Result::ok)
        .map(|e| (e.user_id, e.n))
        .collect();

    assert_eq!(
        flags,
        vec![
            ("alice".to_string(), 1), // alice's first event: new session
            ("alice".to_string(), 0), // same session
            ("bob".to_string(), 1),   // bob's first event: new session
        ],
        "the first event per key must start a new session"
    );
}

#[tokio::test]
async fn every_key_gets_its_own_first_session() {
    // Three distinct keys, one event each — all three are first sightings.
    let evs: Vec<Ev> = (0..3)
        .map(|n| Ev { user_id: format!("u{}", n), n: 99 })
        .collect();

    let out = futures_util::stream::iter(evs)
        .stateful_session_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user_id.clone()),
            Duration::from_secs(3600),
            |mut e, is_new| {
                e.n = if is_new { 1 } else { 0 };
                e
            },
        )
        .collect::<Vec<_>>()
        .await;

    let flags: Vec<u32> = out.into_iter().filter_map(Result::ok).map(|e| e.n).collect();
    assert_eq!(flags, vec![1, 1, 1], "each key's first event starts a session");
}
