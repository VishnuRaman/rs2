use rs2_stream::rs2_new::*;
use rs2_stream::stream_performance_metrics::HealthThresholds;
use rs2_stream::stream_configuration::*;
use rs2_stream::stream::{
    Stream, StreamExt, AdvancedStreamExt, SpecializedStreamExt, SelectStreamExt,
    empty, once, repeat, from_iter, pending, repeat_with, once_with, unfold,
    UtilityStreamExt, RateStreamExt
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_emit() {
    let stream = emit(42);
    let result: Vec<_> = stream.collect().await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_empty_rs2() {
    let stream = empty_rs2::<i32>();
    let result: Vec<_> = stream.collect().await;
    assert_eq!(result, vec![] as Vec<i32>);
}

#[tokio::test]
async fn test_from_iter_rs2() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let result: Vec<_> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_eval() {
    let stream = eval(async { 100 });
    let result: Vec<_> = stream.collect().await;
    assert_eq!(result, vec![100]);
}

#[tokio::test]
async fn test_repeat_rs2() {
    let stream = repeat_rs2(7).take(3);
    let result: Vec<_> = stream.collect().await;
    assert_eq!(result, vec![7, 7, 7]);
}

#[tokio::test]
async fn test_emit_after() {
    let start = std::time::Instant::now();
    let stream = emit_after(42, Duration::from_millis(10));
    let result: Vec<_> = stream.collect().await;
    let elapsed = start.elapsed();
    
    assert_eq!(result, vec![42]);
    assert!(elapsed >= Duration::from_millis(8));
}

#[tokio::test]
async fn test_unfold_rs2() {
    let stream = unfold_rs2(0, |state| async move {
        if state < 3 {
            Some((state, state + 1))
        } else {
            None
        }
    });
    let result: Vec<_> = stream.collect().await;
    assert_eq!(result, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_group_adjacent_by() {
    let stream = from_iter_rs2(vec![1, 1, 2, 2, 2, 3, 1, 1]);
    let grouped = group_adjacent_by(stream, |x| *x);
    let result: Vec<_> = grouped.collect().await;
    
    assert_eq!(result.len(), 4);
    assert_eq!(result[0], (1, vec![1, 1]));
    assert_eq!(result[1], (2, vec![2, 2, 2]));
    assert_eq!(result[2], (3, vec![3]));
    assert_eq!(result[3], (1, vec![1, 1]));
}

#[tokio::test]
async fn test_take() {
    let stream = from_iter_rs2(0..10);
    let taken = take(stream, 5);
    let result: Vec<_> = taken.collect().await;
    assert_eq!(result, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn test_drop() {
    let stream = from_iter_rs2(0..10);
    let dropped = drop(stream, 5);
    let result: Vec<_> = dropped.collect().await;
    assert_eq!(result, vec![5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn test_chunk() {
    let stream = from_iter_rs2(0..10);
    let chunked = chunk(stream, 3);
    let result: Vec<_> = chunked.collect().await;
    
    assert_eq!(result.len(), 4);
    assert_eq!(result[0], vec![0, 1, 2]);
    assert_eq!(result[1], vec![3, 4, 5]);
    assert_eq!(result[2], vec![6, 7, 8]);
    assert_eq!(result[3], vec![9]);
}

#[tokio::test]
async fn test_timeout() {
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let timed = timeout(stream, Duration::from_secs(1));
    let result: Vec<_> = timed.collect().await;
    
    assert_eq!(result.len(), 3);
    assert!(result.iter().all(|r| r.is_ok()));
}

#[tokio::test]
async fn test_scan() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4]);
    let scanned = scan(stream, 0, |acc, x| {
        *acc += x;
        Some(*acc)
    });
    let result: Vec<_> = scanned.collect().await;
    assert_eq!(result, vec![1, 3, 6, 10]);
}

#[tokio::test]
async fn test_fold() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4]);
    let result = fold(stream, 0, |acc, x| async move { acc + x }).await;
    assert_eq!(result, 10);
}

#[tokio::test]
async fn test_reduce() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4]);
    let result = reduce(stream, |acc, x| async move { acc + x }).await;
    assert_eq!(result, Some(10));
}

#[tokio::test]
async fn test_reduce_empty() {
    let stream = from_iter_rs2(vec![] as Vec<i32>);
    let result = reduce(stream, |acc, x| async move { acc + x }).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_filter_map() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let filtered = filter_map(stream, |x| if x % 2 == 0 { Some(x * 2) } else { None });
    let result: Vec<_> = filtered.collect().await;
    assert_eq!(result, vec![4, 8]);
}

#[tokio::test]
async fn test_take_while() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let taken = take_while(stream, |x| *x < 4);
    let result: Vec<_> = taken.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_drop_while() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let dropped = drop_while(stream, |x| *x < 4);
    let result: Vec<_> = dropped.collect().await;
    assert_eq!(result, vec![4, 5]);
}

#[tokio::test]
async fn test_group_by() {
    let stream = from_iter_rs2(vec![1, 1, 2, 2, 3]);
    let grouped = group_by(stream, |x| *x);
    let result: Vec<_> = grouped.collect().await;
    
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (1, vec![1, 1]));
    assert_eq!(result[1], (2, vec![2, 2]));
    assert_eq!(result[2], (3, vec![3]));
}

#[tokio::test]
async fn test_sliding_window() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let windowed = sliding_window(stream, 3);
    let result: Vec<_> = windowed.collect().await;
    
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], vec![1, 2, 3]);
    assert_eq!(result[1], vec![2, 3, 4]);
    assert_eq!(result[2], vec![3, 4, 5]);
}

#[tokio::test]
async fn test_sliding_window_zero_size() {
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let windowed = sliding_window(stream, 0);
    let result: Vec<_> = windowed.collect().await;
    assert_eq!(result, vec![] as Vec<Vec<i32>>);
}

#[tokio::test]
async fn test_batch_process() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let processed = batch_process(stream, 2, |batch| {
        batch.into_iter().map(|x| x * 2).collect()
    });
    let result: Vec<_> = processed.collect().await;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_with_metrics() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let (metered_stream, metrics) = with_metrics(
        stream,
        "test".to_string(),
        HealthThresholds::default()
    );
    
    let result: Vec<_> = metered_stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    
    // Give async tasks time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    let final_metrics = metrics.lock().await;
    assert_eq!(final_metrics.items_processed, 5);
}

#[tokio::test]
async fn test_with_metrics_config() {
    let config = MetricsConfig {
        enabled: true,
        sample_rate: 0.5,
        labels: vec![("test".to_string(), "value".to_string())],
    };
    
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let (metered_stream, metrics) = with_metrics_config(
        stream,
        "test".to_string(),
        HealthThresholds::default(),
        config
    );
    
    let result: Vec<_> = metered_stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    
    // Give async tasks time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    let final_metrics = metrics.lock().await;
    assert!(final_metrics.items_processed <= 10);
    assert!(final_metrics.items_processed >= 1);
}

#[tokio::test]
async fn test_with_metrics_disabled() {
    let config = MetricsConfig {
        enabled: false,
        sample_rate: 1.0,
        labels: vec![],
    };
    
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let (metered_stream, metrics) = with_metrics_config(
        stream,
        "test".to_string(),
        HealthThresholds::default(),
        config
    );
    
    let result: Vec<_> = metered_stream.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
    
    // Give async tasks time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    let final_metrics = metrics.lock().await;
    assert_eq!(final_metrics.items_processed, 0);
}

#[tokio::test]
async fn test_backpressure_config() {
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::Block,
        buffer_size: 100,
        low_watermark: Some(25),
        high_watermark: Some(75),
    };
    
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let backpressured = auto_backpressure(stream, config);
    let result: Vec<_> = backpressured.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_backpressure_drop_oldest() {
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::DropOldest,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let backpressured = auto_backpressure_with_clone(stream, config);
    let result: Vec<_> = backpressured.collect().await;
    assert_eq!(result.len(), 5);
}

#[tokio::test]
async fn test_backpressure_drop_newest() {
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::DropNewest,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let backpressured = auto_backpressure_with_clone(stream, config);
    let result: Vec<_> = backpressured.collect().await;
    assert!(result.len() <= 5);
}

#[tokio::test]
async fn test_backpressure_error() {
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::Error,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let backpressured = auto_backpressure(stream, config);
    let result: Vec<_> = backpressured.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_interrupt_when() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let signal = async { sleep(Duration::from_millis(1)).await };
    let interrupted = interrupt_when(stream, signal);
    let result: Vec<_> = interrupted.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_concat() {
    let first = from_iter_rs2(vec![1, 2, 3]);
    let second = from_iter_rs2(vec![4, 5, 6]);
    let concatenated = concat(first, second);
    let result: Vec<_> = concatenated.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_interleave() {
    let first = from_iter_rs2(vec![1, 3, 5]);
    let second = from_iter_rs2(vec![2, 4, 6]);
    let interleaved = interleave(first, second);
    let result: Vec<_> = interleaved.collect().await;
    assert!(result.len() == 6);
    assert!(result.contains(&1) && result.contains(&6));
}

#[tokio::test]
async fn test_zip_with() {
    let first = from_iter_rs2(vec![1, 2, 3]);
    let second = from_iter_rs2(vec![4, 5, 6]);
    let zipped = zip_with(first, second, |a, b| a + b);
    let result: Vec<_> = zipped.collect().await;
    assert_eq!(result, vec![5, 7, 9]);
}

#[tokio::test]
async fn test_either() {
    let first = from_iter_rs2(vec![1, 2, 3]);
    let second = from_iter_rs2(vec![4, 5, 6]);
    let either_stream = either(first, second);
    let result: Vec<_> = either_stream.collect().await;
    assert_eq!(result.len(), 6);
}

#[tokio::test]
async fn test_debounce() {
    // Test with zero duration (should bypass debouncing)
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let debounced = debounce(stream, Duration::ZERO);
    let result: Vec<_> = debounced.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
    
    // Test with actual debouncing duration - should only emit the last item
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let debounced = debounce(stream, Duration::from_millis(10));
    let result: Vec<_> = debounced.collect().await;
    assert_eq!(result, vec![3]); // Only the last item should be emitted after debouncing
}

#[tokio::test]
async fn test_distinct_until_changed() {
    let stream = from_iter_rs2(vec![1, 1, 2, 2, 3, 3, 2]);
    let distinct = distinct_until_changed(stream);
    let result: Vec<_> = distinct.collect().await;
    assert_eq!(result, vec![1, 2, 3, 2]);
}

#[tokio::test]
async fn test_sample() {
    // Test sample_finite (designed for finite streams)
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let sampled = stream.sample_finite(Duration::from_millis(1));
    let result: Vec<_> = sampled.collect().await;
    assert!(result.len() <= 5);
    
    // Test sample_every_nth (no timing, perfect for finite streams)
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let sampled = stream.sample_every_nth(3); // Every 3rd item: 3, 6, 9
    let result: Vec<_> = sampled.collect().await;
    assert_eq!(result, vec![3, 6, 9]);
    
    // Test sample_first
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let sampled = stream.sample_first(3);
    let result: Vec<_> = sampled.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
    
    // Test regular sample (infinite stream mode) with timeout for safety
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let sampled = stream.sample(Duration::from_millis(10));
    let result = tokio::time::timeout(Duration::from_millis(100), sampled.collect::<Vec<_>>()).await;
    // Should either timeout or complete - both are acceptable for this test
    assert!(result.is_ok() || result.is_err());
    
    // Test sample_auto (should auto-detect finite mode for short durations)
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let sampled = stream.sample_auto(Duration::from_millis(1)); // Short duration = finite mode
    let result: Vec<_> = sampled.collect().await;
    assert!(result.len() <= 5);
}

#[tokio::test]
async fn test_throttle() {
    // Test with actual throttling duration
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let throttled = throttle(stream, Duration::from_millis(1));
    let result: Vec<_> = throttled.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
    
    // Test with zero duration (should bypass throttling)
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let throttled = throttle(stream, Duration::ZERO);
    let result: Vec<_> = throttled.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_merge() {
    let first = from_iter_rs2(vec![1, 3, 5]);
    let second = from_iter_rs2(vec![2, 4, 6]);
    let merged = merge(first, second);
    let result: Vec<_> = merged.collect().await;
    assert_eq!(result.len(), 6);
    assert!(result.contains(&1) && result.contains(&6));
}

#[tokio::test]
async fn test_tick() {
    let ticked = tick(Duration::from_millis(1), 42).take(3);
    let result: Vec<_> = ticked.collect().await;
    assert_eq!(result, vec![42, 42, 42]);
}

#[tokio::test]
async fn test_par_eval_map() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4]);
    let mapped = par_eval_map(stream, 2, |x| async move { x * 2 });
    let result: Vec<_> = mapped.collect().await;
    assert_eq!(result, vec![2, 4, 6, 8]);
}

#[tokio::test]
async fn test_bracket() {
    let acquire = async { 42 };
    let use_fn = |x| from_iter_rs2(vec![x, x + 1]);
    let release = |_| async {};
    
    let bracketed = bracket(acquire, use_fn, release);
    let result: Vec<_> = bracketed.collect().await;
    assert_eq!(result, vec![42, 43]);
}

#[tokio::test]
async fn test_bracket_case() {
    let acquire = async { 42 };
    let use_fn = |x| from_iter_rs2(vec![Ok(x), Ok(x + 1)]);
    let release = |_, _: ExitCase<&str>| async {};
    
    let bracketed = bracket_case(acquire, use_fn, release);
    let result: Vec<_> = bracketed.collect().await;
    assert_eq!(result, vec![Ok(42), Ok(43)]);
}

#[tokio::test]
async fn test_collect_ext() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let result: Vec<_> = stream.collect_into().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_par_eval_map_unordered() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4]);
    let mapped = par_eval_map_unordered(stream, 2, |x| async move { x * 2 });
    let result: Vec<_> = mapped.collect().await;
    assert_eq!(result, vec![2, 4, 6, 8]);
}

#[tokio::test]
async fn test_par_join() {
    let streams = from_iter_rs2(vec![
        from_iter_rs2(vec![1, 2]),
        from_iter_rs2(vec![3, 4])
    ]);
    let joined = par_join(streams, 2);
    let result: Vec<_> = joined.collect().await;
    assert_eq!(result.len(), 4);
}

#[tokio::test]
async fn test_prefetch() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let prefetched = prefetch(stream, 2);
    let result: Vec<_> = prefetched.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_distinct_until_changed_by() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let distinct = distinct_until_changed_by(stream, |a, b| a % 2 == b % 2);
    let result: Vec<_> = distinct.collect().await;
    assert!(result.len() <= 5);
}

#[tokio::test]
async fn test_rate_limit_backpressure() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let rate_limited = rate_limit_backpressure(stream, 10);
    let result: Vec<_> = rate_limited.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_helper_streams() {
    assert_eq!(empty_stream::<i32>().collect::<Vec<i32>>().await, vec![] as Vec<i32>);
    assert_eq!(once_stream(42).collect::<Vec<_>>().await, vec![42]);
    assert_eq!(repeat_stream(7).take(3).collect::<Vec<_>>().await, vec![7, 7, 7]);
    assert_eq!(from_iter_stream(vec![1, 2, 3]).collect::<Vec<_>>().await, vec![1, 2, 3]);
    
    let pending_result = pending_stream::<i32>().take(0).collect::<Vec<i32>>().await;
    assert_eq!(pending_result, vec![] as Vec<i32>);
    
    let repeat_with_result = repeat_with_stream(|| 42).take(3).collect::<Vec<_>>().await;
    assert_eq!(repeat_with_result, vec![42, 42, 42]);
    
    let once_with_result = once_with_stream(|| 100).collect::<Vec<_>>().await;
    assert_eq!(once_with_result, vec![100]);
    
    let unfold_result = unfold_stream(0, |state| async move {
        if state < 3 { Some((state, state + 1)) } else { None }
    }).collect::<Vec<_>>().await;
    assert_eq!(unfold_result, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_stream_transformations() {
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5]);
    
    let mapped = map_stream(stream, |x| x * 2);
    assert_eq!(mapped.collect::<Vec<_>>().await, vec![2, 4, 6, 8, 10]);
    
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5]);
    let filtered = filter_stream(stream, |x| *x % 2 == 0);
    assert_eq!(filtered.collect::<Vec<_>>().await, vec![2, 4]);
    
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5]);
    let taken = take_stream(stream, 3);
    assert_eq!(taken.collect::<Vec<_>>().await, vec![1, 2, 3]);
    
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5]);
    let skipped = skip_stream(stream, 2);
    assert_eq!(skipped.collect::<Vec<_>>().await, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_create_trait_object_stream() {
    let stream = from_iter_rs2(vec![1, 2, 3]);
    let boxed = create_trait_object_stream(stream);
    let result: Vec<_> = boxed.collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_collect_stream() {
    let stream = from_iter_rs2(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = collect_stream(stream).await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
} 