use rs2_stream::rs2::*;
use rs2_stream::stream::StreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::collections::{HashSet, BTreeSet, BTreeMap};
use std::time::Duration;

#[tokio::test]
async fn test_collect_stream_vec() {
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5]);
    let result: Vec<_> = collect_stream(stream).await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_collect_into_vec() {
    let stream = from_iter_stream(vec![10, 20, 30]);
    let result: Vec<_> = stream.collect_into().await;
    assert_eq!(result, vec![10, 20, 30]);
}

#[tokio::test]
async fn test_collect_into_hashset() {
    let stream = from_iter_stream(vec![1, 2, 2, 3, 3, 3, 4, 5, 5]);
    let result: HashSet<_> = stream.collect_into().await;
    let expected: HashSet<_> = vec![1, 2, 3, 4, 5].into_iter().collect();
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_collect_into_btreeset() {
    let stream = from_iter_stream(vec![5, 3, 1, 4, 2, 3, 5]);
    let result: BTreeSet<_> = stream.collect_into().await;
    let expected: BTreeSet<_> = vec![1, 2, 3, 4, 5].into_iter().collect();
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_collect_into_btreemap() {
    let stream = from_iter_stream(vec![("b", 2), ("a", 1), ("c", 3)]);
    let result: BTreeMap<&str, i32> = stream.collect_into().await;
    let mut expected = BTreeMap::new();
    expected.insert("a", 1);
    expected.insert("b", 2);
    expected.insert("c", 3);
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_combinators_chain_map_filter_take() {
    let stream = from_iter_stream(0..10)
        .map(|x| x * 2)
        .filter(|&x| x % 4 == 0)
        .take(3);
    let result: Vec<_> = stream.collect_into().await;
    assert_eq!(result, vec![0, 4, 8]);
}

#[tokio::test]
async fn test_empty_stream() {
    let stream = empty_stream::<i32>();
    let result: Vec<i32> = collect_stream(stream).await;
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_tick() {
    // Test tick function that emits an item at regular intervals
    let start_time = std::time::Instant::now();
    
    // Create a tick stream that emits "ping" every 50ms
    let tick_stream = tick(Duration::from_millis(50), "ping");
    
    // Take 3 items and verify they are correct
    let result: Vec<_> = tick_stream.take_rs2(3).collect_rs2().await;
    
    assert_eq!(result, vec!["ping", "ping", "ping"]);
    
    // Verify that roughly the right amount of time has passed (3 ticks * 50ms)
    let elapsed = start_time.elapsed();
    assert!(elapsed >= Duration::from_millis(150)); // At least 150ms should have passed
    assert!(elapsed < Duration::from_millis(300));  // But not too much more
}

#[tokio::test] 
async fn test_tick_with_numbers() {
    // Test tick with different value types
    let tick_stream = tick(Duration::from_millis(10), 42);
    let result: Vec<_> = tick_stream.take_rs2(5).collect_rs2().await;
    assert_eq!(result, vec![42, 42, 42, 42, 42]);
}

#[tokio::test]
async fn test_tick_zero_items() {
    // Test taking zero items from tick stream
    let tick_stream = tick(Duration::from_millis(10), "test");
    let result: Vec<_> = tick_stream.take_rs2(0).collect_rs2().await;
    assert_eq!(result, Vec::<&str>::new());
}

/*
#[tokio::test]
async fn test_trait_object_entry_point() {
    let stream = from_iter_stream(vec![1, 2, 3]);
    let trait_obj = create_trait_object_stream(stream);
    // Use trait object with map_trait_stream
    let mapped = map_trait_stream(trait_obj, |x| x * 10);
    let result: Vec<_> = collect_stream(mapped).await;
    assert_eq!(result, vec![10, 20, 30]);
}
*/ 