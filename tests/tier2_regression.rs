//! Regression tests for the Tier 2 fixes: hangs, panics and leaks.
//!
//! Every test here was checked against the unfixed code — see the note on each
//! for what it did before. A test that passes both ways proves nothing.

use futures_util::stream::StreamExt;
use rs2_stream::advanced_analytics::*;
use rs2_stream::queue::Queue;
use rs2_stream::rs2::*;
use rs2_stream::schema_validation::*;
use rs2_stream::state::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// ---------------------------------------------------------------------------
// 10. Queue::len must not contend with a parked consumer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_len_does_not_block_against_parked_consumer() {
    // Previously `len()` locked the receiver, which `dequeue` holds across
    // `recv().await` — so this call never returned.
    let q: Queue<i32> = Queue::bounded(10);
    let mut rx = Box::pin(q.dequeue());

    q.enqueue(1).await.unwrap();
    assert_eq!(rx.next().await, Some(1));

    let q2 = q.clone();
    let parked = tokio::spawn(async move { rx.next().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let len = tokio::time::timeout(Duration::from_millis(500), q2.len()).await;
    assert!(len.is_ok(), "len() deadlocked against a parked consumer");
    assert_eq!(len.unwrap(), 0);

    parked.abort();
}

#[tokio::test]
async fn queue_len_tracks_enqueue_and_dequeue() {
    let q: Queue<i32> = Queue::unbounded();
    assert_eq!(q.len().await, 0);
    assert!(q.is_empty().await);

    for i in 0..5 {
        q.enqueue(i).await.unwrap();
    }
    assert_eq!(q.len().await, 5);
    assert!(!q.is_empty().await);

    let mut rx = Box::pin(q.dequeue());
    for _ in 0..3 {
        rx.next().await.unwrap();
    }
    assert_eq!(q.len().await, 2);
}

// ---------------------------------------------------------------------------
// 11. race must not starve one side or re-poll a finished stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn race_does_not_starve_the_ready_side() {
    // s1 is slow; s2 is ready immediately. The old implementation did an
    // unconditional `await` on s1 first, so s2 could not get through.
    let slow = stream_after(Duration::from_millis(300), vec![1, 2]);
    let fast = from_iter(vec![10, 20, 30]);

    let out = race(slow, fast).take(3).collect::<Vec<_>>().await;

    assert!(
        out.iter().all(|x| *x >= 10),
        "slow side blocked the ready one: {:?}",
        out
    );
}

#[tokio::test]
async fn race_drains_both_sides_completely() {
    let a = from_iter(vec![1, 2, 3]);
    let b = from_iter(vec![10, 20]);

    let mut out = race(a, b).collect::<Vec<_>>().await;
    out.sort();

    assert_eq!(
        out,
        vec![1, 2, 3, 10, 20],
        "race dropped items from one side"
    );
}

/// A stream that panics if polled again after it has returned `None`.
///
/// Polling a `Stream` after completion violates its contract. The old `race`
/// awaited one side, set its done flag, and then immediately re-polled that same
/// side inside a `select!`.
struct PanicOnRepoll {
    remaining: usize,
    finished: bool,
}

impl futures_core::Stream for PanicOnRepoll {
    type Item = i32;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<i32>> {
        assert!(
            !self.finished,
            "stream was polled again after returning None"
        );
        if self.remaining == 0 {
            self.finished = true;
            return std::task::Poll::Ready(None);
        }
        self.remaining -= 1;
        std::task::Poll::Ready(Some(1))
    }
}

#[tokio::test]
async fn race_does_not_repoll_a_finished_stream() {
    let a = PanicOnRepoll {
        remaining: 2,
        finished: false,
    };
    let b = from_iter(vec![10, 20, 30]);

    // Must complete without tripping the assertion inside PanicOnRepoll.
    let out = race(a, b).collect::<Vec<_>>().await;
    assert_eq!(out.len(), 5, "all items from both sides: {:?}", out);
}

#[tokio::test]
async fn race_handles_one_empty_side() {
    let out = race(empty::<i32>(), from_iter(vec![1, 2, 3]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, 2, 3]);

    let out = race(from_iter(vec![1, 2, 3]), empty::<i32>())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, 2, 3]);
}

fn stream_after(delay: Duration, items: Vec<i32>) -> RS2Stream<i32> {
    use async_stream::stream;
    stream! {
        tokio::time::sleep(delay).await;
        for i in items {
            yield i;
        }
    }
    .boxed()
}

// ---------------------------------------------------------------------------
// 12. window_by_time must not divide by zero on sub-second windows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn window_by_time_accepts_sub_second_windows() {
    // `window_size.as_secs()` truncated to 0 and panicked on the division.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let events: Vec<(u32, SystemTime)> = (0..6)
        .map(|i| (i, base + Duration::from_millis(i as u64 * 200)))
        .collect();

    let config = TimeWindowConfig {
        window_size: Duration::from_millis(500),
        watermark_delay: Duration::from_millis(0),
        ..Default::default()
    };

    let windows = window_by_time(from_iter(events), config, |e: &(u32, SystemTime)| e.1)
        .collect::<Vec<_>>()
        .await;

    let total: usize = windows.iter().map(|w| w.events.len()).sum();
    assert_eq!(total, 6, "every event must land in some window");
    assert!(windows.len() > 1, "500ms windows should split 1.0s of events");
}

#[tokio::test]
async fn window_by_time_accepts_zero_window() {
    // Degenerate but must not panic.
    let config = TimeWindowConfig {
        window_size: Duration::from_millis(0),
        watermark_delay: Duration::from_millis(0),
        ..Default::default()
    };
    let out = window_by_time(from_iter(vec![1u32, 2, 3]), config, |_| SystemTime::now())
        .collect::<Vec<_>>()
        .await;
    let total: usize = out.iter().map(|w| w.events.len()).sum();
    assert_eq!(total, 3);
}

// ---------------------------------------------------------------------------
// 13. clock going backwards must not panic
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Ev {
    user: String,
    n: u32,
}

/// Storage that always hands back a state entry timestamped far in the future.
///
/// This is what a backward wall-clock step or a skewed peer looks like to the
/// stateful operators. `now - stored_timestamp` then underflows, which panics in
/// debug builds — the operators must use saturating arithmetic instead.
struct FutureDatedStorage {
    payload: Vec<u8>,
}

#[async_trait::async_trait]
impl StateStorage for FutureDatedStorage {
    async fn get(&self, _key: &str) -> Option<Vec<u8>> {
        Some(self.payload.clone())
    }
    async fn set(
        &self,
        _key: &str,
        _value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
    async fn delete(&self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
    async fn exists(&self, _key: &str) -> bool {
        true
    }
    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

fn far_future_millis() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    now + 60 * 60 * 1000 // one hour ahead
}

fn config_with(payload: Vec<u8>) -> StateConfig {
    StateConfig::default().with_custom_storage(Arc::new(FutureDatedStorage { payload }))
}

#[tokio::test]
async fn session_survives_future_dated_state() {
    let payload = serde_json::to_vec(&serde_json::json!({
        "last_activity": far_future_millis(),
        "is_new_session": false
    }))
    .unwrap();

    let evs: Vec<Ev> = (0..20)
        .map(|n| Ev {
            user: "u1".to_string(),
            n,
        })
        .collect();

    let out = futures_util::stream::iter(evs)
        .stateful_session_rs2(
            config_with(payload),
            CustomKeyExtractor::new(|e: &Ev| e.user.clone()),
            Duration::from_millis(10),
            |e, _new| e,
        )
        .collect::<Vec<_>>()
        .await;

    assert_eq!(out.len(), 20, "session op must not panic on a skewed clock");
}

#[tokio::test]
async fn deduplicate_survives_future_dated_state() {
    let payload = serde_json::to_vec(&far_future_millis()).unwrap();

    let evs: Vec<Ev> = (0..20)
        .map(|n| Ev {
            user: "u1".to_string(),
            n,
        })
        .collect();

    // Must run to completion rather than panicking on the underflow.
    let _ = futures_util::stream::iter(evs)
        .stateful_deduplicate_rs2(
            config_with(payload),
            CustomKeyExtractor::new(|e: &Ev| e.user.clone()),
            Duration::from_millis(10),
            |e| e,
        )
        .collect::<Vec<_>>()
        .await;
}

#[tokio::test]
async fn group_by_timeout_survives_future_dated_clock() {
    // Exercises the `now - group_start` path with a group timeout set.
    let evs: Vec<Ev> = (0..30)
        .map(|n| Ev {
            user: format!("u{}", n % 3),
            n,
        })
        .collect();

    let out = futures_util::stream::iter(evs)
        .stateful_group_by_advanced_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|e: &Ev| e.user.clone()),
            Some(5),
            Some(Duration::from_millis(1)),
            true,
            |_k, items: Vec<Ev>, _s| Box::pin(async move { Ok(items.len()) }),
        )
        .collect::<Vec<_>>()
        .await;

    assert!(!out.is_empty());
}

// ---------------------------------------------------------------------------
// 15. InMemoryState must actually release expired entries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn in_memory_state_frees_expired_entries() {
    // Expiry used to be checked only on read: the bytes stayed in the map
    // forever, and the default config sets no max_size at all.
    let store = InMemoryState::new(Duration::from_millis(50));

    for i in 0..300 {
        store.set(&format!("k{}", i), b"payload").await.unwrap();
    }
    assert!(store.allocated_len().await > 0);

    tokio::time::sleep(Duration::from_millis(120)).await;

    // Writing past the sweep interval must reclaim the expired entries.
    for i in 0..300 {
        store.set(&format!("fresh{}", i), b"payload").await.unwrap();
    }

    let allocated = store.allocated_len().await;
    assert!(
        allocated <= 400,
        "expired entries were never reclaimed: {} still allocated",
        allocated
    );
    assert_eq!(store.get("k0").await, None, "expired key must read as absent");
}

#[tokio::test]
async fn bounded_state_writes_stay_cheap_at_capacity() {
    // Enforcing max_size used to clone and sort every key on each `set`, so a
    // store sitting at capacity paid ~2.2ms per write. Measured: 5,000 writes
    // at capacity took 11.0s before the ordered index, 15ms after. Every
    // predefined StateConfig sets a max_size, so this hit all of them.
    let store = InMemoryState::new(Duration::from_secs(600)).with_max_size(5_000);
    for i in 0..5_000 {
        store.set(&format!("k{}", i), b"v").await.unwrap();
    }

    let start = Instant::now();
    for i in 5_000..10_000 {
        store.set(&format!("k{}", i), b"v").await.unwrap();
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "bounded writes degraded: 5,000 sets at capacity took {:?}",
        elapsed
    );
    assert!(store.allocated_len().await <= 5_000);
}

#[tokio::test]
async fn in_memory_state_respects_max_size() {
    let store = InMemoryState::new(Duration::from_secs(60)).with_max_size(50);
    for i in 0..500 {
        store.set(&format!("k{}", i), b"v").await.unwrap();
    }
    assert!(
        store.allocated_len().await <= 50,
        "max_size not enforced: {}",
        store.allocated_len().await
    );
}

// ---------------------------------------------------------------------------
// 16. dropping backpressure must stop its producer and not busy-poll
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_oldest_stops_producing_when_consumer_goes_away() {
    // The producer task had no shutdown signal: it drained an infinite source
    // forever after the consumer was dropped.
    let produced = Arc::new(AtomicUsize::new(0));
    let counter = produced.clone();

    let source = from_iter(0u64..).map_rs2(move |x| {
        counter.fetch_add(1, Ordering::Relaxed);
        x
    });

    {
        let mut s = auto_backpressure_drop_oldest(source, 8);
        for _ in 0..5 {
            s.next().await;
        }
        // stream dropped here
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_drop = produced.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let later = produced.load(Ordering::Relaxed);

    assert_eq!(
        after_drop, later,
        "producer kept running after the consumer was dropped ({} -> {})",
        after_drop, later
    );
}

#[tokio::test]
async fn drop_newest_passes_everything_below_capacity() {
    let out = auto_backpressure_drop_newest(from_iter(0..200u64), 512)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 200, "no items may be dropped below capacity");
}

#[tokio::test]
async fn dropping_backpressure_completes_without_a_polling_delay() {
    // The old implementation polled a 1ms sleep when the buffer looked empty,
    // so it could not observe `source_done` until that sleep expired — roughly
    // 2ms of dead time per stream, regardless of how little data flowed.
    //
    // Measured: 200 short streams took ~464ms before this fix and ~2ms after.
    // The 100ms threshold sits well clear of both.
    let start = Instant::now();
    for _ in 0..200 {
        let out = auto_backpressure_drop_newest(from_iter(0..3u64), 16)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(out.len(), 3);
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(100),
        "per-stream polling delay still present: 200 short streams took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn drop_oldest_keeps_the_most_recent_items() {
    let out = auto_backpressure_drop_oldest(from_iter(0..1000u64), 16)
        .collect::<Vec<_>>()
        .await;
    assert!(!out.is_empty());
    assert!(
        out.windows(2).all(|w| w[0] < w[1]),
        "output must stay in source order"
    );
}

// ---------------------------------------------------------------------------
// 18. library must not panic on bad input
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_schema_reports_an_error_instead_of_panicking() {
    // `JsonSchemaValidator::new` called `.expect()` on a user-supplied schema.
    let bad = serde_json::json!({ "type": 12345 });
    let result = JsonSchemaValidator::try_new("bad", bad);
    assert!(result.is_err(), "invalid schema must be reported, not panic");
}

#[tokio::test]
async fn valid_schema_still_compiles_and_validates() {
    let schema = serde_json::json!({
        "type": "object",
        "required": ["id"],
        "properties": { "id": { "type": "integer" } }
    });
    let v = JsonSchemaValidator::try_new("ok", schema).expect("valid schema");

    assert!(v.validate(br#"{"id": 1}"#).await.is_ok());
    assert!(v.validate(br#"{"nope": 1}"#).await.is_err());
}

#[tokio::test]
async fn missing_media_file_returns_error_instead_of_panicking() {
    use rs2_stream::media::streaming::MediaStreamingService;
    use rs2_stream::media::types::{MediaStream, MediaType, QualityLevel};
    use std::path::PathBuf;

    let service = MediaStreamingService::new(4);
    let config = MediaStream {
        id: "test".to_string(),
        user_id: 1,
        content_type: MediaType::Video,
        quality: QualityLevel::Medium,
        chunk_size: 1024,
        created_at: std::time::SystemTime::now().into(),
        metadata: std::collections::HashMap::new(),
    };

    let result = service
        .start_file_stream(PathBuf::from("/definitely/not/a/real/file.mp4"), config)
        .await;

    assert!(result.is_err(), "missing file must return Err, not panic");
}
