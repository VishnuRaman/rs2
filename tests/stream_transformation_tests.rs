use rs2_stream::stream::{StreamExt, AdvancedStreamExt, UtilityStreamExt, from_iter, empty, once};

#[tokio::test]
async fn test_basic_transformations() {
    // Test from_iter and collect
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    
    // Test map functionality
    let stream = from_iter(vec![1, 2, 3]);
    let mapped: Vec<i32> = stream.map(|x| x * 2).collect().await;
    assert_eq!(mapped, vec![2, 4, 6]);
    
    // Test filter functionality
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let filtered: Vec<i32> = stream.filter(|&x| x % 2 == 0).collect().await;
    assert_eq!(filtered, vec![2, 4]);
}

#[tokio::test]
async fn test_scan_and_fold() {
    // Test scan
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let scanned: Vec<i32> = stream.scan(0, |acc, x| {
        *acc += x;
        Some(*acc)
    }).collect().await;
    assert_eq!(scanned, vec![1, 3, 6, 10, 15]);
    
    // Test fold
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let folded = stream.fold(0, |acc, x| async move { acc + x }).await;
    assert_eq!(folded, 15);
}

#[tokio::test] 
async fn test_zip_and_chain() {
    // Test zip
    let s1 = from_iter(vec![1, 2, 3]);
    let s2 = from_iter(vec![10, 20, 30]);
    let zipped: Vec<(i32, i32)> = s1.zip(s2).collect().await;
    assert_eq!(zipped, vec![(1, 10), (2, 20), (3, 30)]);
    
    // Test chain
    let s1 = from_iter(vec![1, 2, 3]);
    let s2 = from_iter(vec![4, 5, 6]);
    let result: Vec<i32> = s1.chain(s2).collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_take_and_skip() {
    // Test take
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let taken: Vec<i32> = stream.take(3).collect().await;
    assert_eq!(taken, vec![1, 2, 3]);
    
    // Test skip
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let skipped: Vec<i32> = stream.skip(2).collect().await;
    assert_eq!(skipped, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_empty_and_once() {
    // Test empty stream
    let stream = empty::<i32>();
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
    
    // Test once stream
    let stream = once(42);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_advanced_operations() {
    // Test filter_map
    let stream = from_iter(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = StreamExt::filter_map(stream, |x| {
        if x % 2 == 0 {
            Some(x * 2)
        } else {
            None
        }
    }).collect().await;
    assert_eq!(result, vec![4, 8]);
    
    // Test enumerate
    let stream = from_iter(vec!["a", "b", "c"]);
    let result: Vec<(usize, &str)> = stream.enumerate().collect().await;
    assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);
}
