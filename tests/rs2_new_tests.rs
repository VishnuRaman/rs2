use rs2_stream::rs2_new::*;
use rs2_stream::stream::StreamExt;
use std::collections::{HashSet, BTreeSet, BTreeMap};

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