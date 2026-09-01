//! Tests for the FS2-conformance additions.
//!
//! These cover new API rather than bug fixes, so there is no pre-fix baseline to
//! diff against — they assert concrete behaviour instead.

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ---------------------------------------------------------------------------
// try_bracket_case — makes all three FS2 exit cases reachable
// ---------------------------------------------------------------------------

fn record() -> (Arc<Mutex<Option<String>>>, Arc<Mutex<Option<String>>>) {
    let seen = Arc::new(Mutex::new(None));
    (seen.clone(), seen)
}

fn describe(case: ExitCase<String>) -> String {
    match case {
        ExitCase::Completed => "completed".to_string(),
        ExitCase::Canceled => "canceled".to_string(),
        ExitCase::Errored(e) => format!("errored:{}", e),
    }
}

#[tokio::test]
async fn try_bracket_case_reports_errored_and_stops_at_first_err() {
    let (seen, sink) = record();

    let out = try_bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Err("boom".to_string()), Ok(3)]),
        move |_, case| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(describe(case));
            }
        },
    )
    .collect::<Vec<_>>()
    .await;

    // Everything after the first Err is discarded.
    assert_eq!(out, vec![Ok(1), Err("boom".to_string())]);
    settle().await;
    assert_eq!(seen.lock().unwrap().as_deref(), Some("errored:boom"));
}

#[tokio::test]
async fn try_bracket_case_reports_completed_when_no_error() {
    let (seen, sink) = record();

    let out = try_bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Ok(2)]),
        move |_, case| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(describe(case));
            }
        },
    )
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out.len(), 2);
    settle().await;
    assert_eq!(seen.lock().unwrap().as_deref(), Some("completed"));
}

#[tokio::test]
async fn try_bracket_case_reports_canceled_on_early_exit() {
    let (seen, sink) = record();

    let out = try_bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Ok(2), Ok(3), Ok(4)]),
        move |_, case| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(describe(case));
            }
        },
    )
    .take(2)
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out.len(), 2);
    settle().await;
    assert_eq!(seen.lock().unwrap().as_deref(), Some("canceled"));
}

#[tokio::test]
async fn bracket_case_and_try_bracket_case_differ_only_on_errors() {
    // bracket_case passes Err through and keeps going; try_bracket_case stops.
    let passthrough = bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Err("e".into()), Ok(3)]),
        |_, _: ExitCase<String>| async {},
    )
    .collect::<Vec<_>>()
    .await;
    assert_eq!(passthrough.len(), 3, "bracket_case passes errors through");

    let short_circuit = try_bracket_case(
        async { 1u32 },
        |_| from_iter(vec![Ok::<u32, String>(1), Err("e".into()), Ok(3)]),
        |_, _: ExitCase<String>| async {},
    )
    .collect::<Vec<_>>()
    .await;
    assert_eq!(short_circuit.len(), 2, "try_bracket_case stops at the error");
}

// ---------------------------------------------------------------------------
// on_finalize / on_finalize_case
// ---------------------------------------------------------------------------

#[tokio::test]
async fn on_finalize_runs_on_normal_completion() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();

    let out = on_finalize(from_iter(vec![1, 2, 3]), move || {
        let flag = flag.clone();
        async move { flag.store(true, Ordering::SeqCst) }
    })
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out, vec![1, 2, 3]);
    assert!(ran.load(Ordering::SeqCst), "finalizer did not run");
}

#[tokio::test]
async fn on_finalize_runs_on_early_termination() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();

    let out = on_finalize(from_iter(vec![1, 2, 3, 4, 5]), move || {
        let flag = flag.clone();
        async move { flag.store(true, Ordering::SeqCst) }
    })
    .take(2)
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out, vec![1, 2]);
    settle().await;
    assert!(
        ran.load(Ordering::SeqCst),
        "finalizer must run even when the consumer stops early"
    );
}

#[tokio::test]
async fn on_finalize_runs_exactly_once() {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();

    on_finalize(from_iter(vec![1, 2, 3]), move || {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    })
    .collect::<Vec<_>>()
    .await;

    settle().await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn on_finalize_case_distinguishes_completion_from_cancellation() {
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = seen.clone();
    on_finalize_case(from_iter(vec![1, 2, 3]), move |case: ExitCase<()>| {
        let sink = sink.clone();
        async move {
            *sink.lock().unwrap() = Some(format!("{:?}", case));
        }
    })
    .collect::<Vec<_>>()
    .await;
    settle().await;
    assert_eq!(seen.lock().unwrap().as_deref(), Some("Completed"));

    let seen2: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink2 = seen2.clone();
    on_finalize_case(from_iter(vec![1, 2, 3, 4]), move |case: ExitCase<()>| {
        let sink2 = sink2.clone();
        async move {
            *sink2.lock().unwrap() = Some(format!("{:?}", case));
        }
    })
    .take(2)
    .collect::<Vec<_>>()
    .await;
    settle().await;
    assert_eq!(seen2.lock().unwrap().as_deref(), Some("Canceled"));
}

// ---------------------------------------------------------------------------
// race / merge_either
// ---------------------------------------------------------------------------

#[tokio::test]
async fn race_keeps_its_original_behaviour() {
    let out = race(from_iter(vec![1, 2, 3]), from_iter(vec![10, 20]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 5, "race drains both sides");
}

#[tokio::test]
async fn merge_either_tags_each_value_with_its_branch() {
    let out = merge_either(from_iter(vec![1u32, 2]), from_iter(vec!["a", "b"]))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(out.len(), 4);
    let lefts: Vec<u32> = out.iter().cloned().filter_map(Either::left).collect();
    let rights: Vec<&str> = out.into_iter().filter_map(Either::right).collect();
    assert_eq!(lefts, vec![1, 2]);
    assert_eq!(rights, vec!["a", "b"]);
}

// ---------------------------------------------------------------------------
// interleave family
// ---------------------------------------------------------------------------

#[tokio::test]
async fn interleave_stops_at_the_shorter_side() {
    // FS2's `interleave`: alternate from the left, stop when either runs out.
    let out = interleave(from_iter(vec![1, 3, 5]), from_iter(vec![2]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, 2, 3]);
}

#[tokio::test]
async fn interleave_alternates_evenly_when_same_length() {
    let out = interleave(from_iter(vec![1, 3, 5]), from_iter(vec![2, 4, 6]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn interleave_all_continues_with_the_longer_side() {
    // FS2's `interleaveAll`.
    let out = interleave_all(from_iter(vec![1, 3, 5]), from_iter(vec![2]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, 2, 3, 5]);
}

#[tokio::test]
async fn interleave_many_round_robins_and_drops_exhausted() {
    // The behaviour `interleave` used to have.
    let out = interleave_many(vec![
        from_iter(vec![1, 4]),
        from_iter(vec![2, 5, 7]),
        from_iter(vec![3]),
    ])
    .collect::<Vec<_>>()
    .await;
    assert_eq!(out, vec![1, 2, 3, 4, 5, 7]);
}

// ---------------------------------------------------------------------------
// metered / chunk_n
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metered_paces_without_dropping() {
    let start = Instant::now();
    let out = metered(from_iter(0..3u32), Duration::from_millis(100))
        .collect::<Vec<_>>()
        .await;
    let elapsed = start.elapsed();

    assert_eq!(out, vec![0, 1, 2], "metered must not drop elements");
    assert!(elapsed >= Duration::from_millis(150), "not pacing: {:?}", elapsed);
    assert!(elapsed < Duration::from_millis(290), "trailing delay: {:?}", elapsed);
}

#[tokio::test]
async fn chunk_n_can_drop_a_short_final_chunk() {
    let kept = chunk_n(from_iter(0..7u32), 3, true).collect::<Vec<_>>().await;
    assert_eq!(kept, vec![vec![0, 1, 2], vec![3, 4, 5], vec![6]]);

    let dropped = chunk_n(from_iter(0..7u32), 3, false).collect::<Vec<_>>().await;
    assert_eq!(
        dropped,
        vec![vec![0, 1, 2], vec![3, 4, 5]],
        "allow_fewer=false must discard the trailing partial chunk"
    );
}

#[tokio::test]
async fn chunk_n_handles_zero_size() {
    let out = chunk_n(from_iter(0..3u32), 0, true).collect::<Vec<_>>().await;
    assert_eq!(out.len(), 3);
}

// ---------------------------------------------------------------------------
// eval_tap / zip_with_index / group_within
// ---------------------------------------------------------------------------

#[tokio::test]
async fn eval_tap_observes_without_changing_the_stream() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();

    let out = eval_tap(from_iter(vec![1, 2, 3]), move |x: &i32| {
        let sink = sink.clone();
        let x = *x;
        async move {
            sink.lock().unwrap().push(x);
        }
    })
    .collect::<Vec<_>>()
    .await;

    assert_eq!(out, vec![1, 2, 3], "elements pass through unchanged");
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3], "effect saw every element");
}

#[tokio::test]
async fn zip_with_index_numbers_from_zero() {
    let out = zip_with_index(from_iter(vec!["a", "b", "c"]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![("a", 0), ("b", 1), ("c", 2)]);
}

#[tokio::test]
async fn group_within_emits_full_chunks_immediately() {
    let out = group_within(from_iter(0..6u32), 2, Duration::from_secs(30))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![vec![0, 1], vec![2, 3], vec![4, 5]]);
}

#[tokio::test]
async fn group_within_emits_a_partial_chunk_on_timeout() {
    use async_stream::stream;
    // Two items arrive quickly, then a long gap. With a chunk size of 10 the
    // buffer never fills, so only the timeout can make progress.
    let src = stream! {
        yield 1u32;
        yield 2u32;
        tokio::time::sleep(Duration::from_millis(400)).await;
        yield 3u32;
    }
    .boxed();

    let start = Instant::now();
    let out = group_within(src, 10, Duration::from_millis(100))
        .take(1)
        .collect::<Vec<_>>()
        .await;
    let elapsed = start.elapsed();

    assert_eq!(out, vec![vec![1, 2]], "timeout must flush the partial chunk");
    assert!(
        elapsed < Duration::from_millis(350),
        "waited for the buffer to fill instead of timing out: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn group_within_loses_nothing() {
    let out = group_within(from_iter(0..7u32), 3, Duration::from_secs(30))
        .collect::<Vec<_>>()
        .await;
    let flat: Vec<u32> = out.into_iter().flatten().collect();
    assert_eq!(flat, (0..7).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// handle_error_with / attempt (on Result streams)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handle_error_with_terminates_the_source() {
    // FS2's handleErrorWith: the source stops at the first error and the
    // handler's stream takes over.
    let out = from_iter(vec![Ok::<u32, &str>(1), Err("boom"), Ok(3)])
        .handle_error_with_rs2(|_e| from_iter(vec![99, 100]))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        out,
        vec![1, 99, 100],
        "the source must not resume after the handler runs"
    );
}

#[tokio::test]
async fn handle_error_with_passes_through_when_no_error() {
    let out = from_iter(vec![Ok::<u32, &str>(1), Ok(2)])
        .handle_error_with_rs2(|_e| from_iter(vec![99]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![1, 2]);
}

#[tokio::test]
async fn handle_error_with_differs_from_on_error_resume_next() {
    // on_error_resume_next keeps consuming the source and recovers each error;
    // handle_error_with stops at the first one. Same input, different results.
    let resumed = from_iter(vec![Ok::<u32, &str>(1), Err("a"), Ok(3), Err("b")])
        .on_error_resume_next_rs2(|_e| from_iter(vec![0]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(resumed, vec![1, 0, 3, 0], "resume-next recovers every error");

    let handled = from_iter(vec![Ok::<u32, &str>(1), Err("a"), Ok(3), Err("b")])
        .handle_error_with_rs2(|_e| from_iter(vec![0]))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(handled, vec![1, 0], "handle_error_with stops at the first");
}

#[tokio::test]
async fn attempt_stops_after_the_first_error() {
    let out = from_iter(vec![Ok::<u32, &str>(1), Ok(2), Err("boom"), Ok(4)])
        .attempt_rs2()
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        out,
        vec![Ok(1), Ok(2), Err("boom")],
        "everything after the first error is discarded"
    );
}

#[tokio::test]
async fn attempt_passes_a_clean_stream_through() {
    let out = from_iter(vec![Ok::<u32, &str>(1), Ok(2)])
        .attempt_rs2()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out, vec![Ok(1), Ok(2)]);
}
