use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

#[test]
fn test_emit() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = emit(42);
        let result = stream.collect::<Vec<_>>().await;
        assert_eq!(result, vec![42]);
    });
}

#[test]
fn test_empty() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = empty::<i32>();
        let result = stream.collect::<Vec<_>>().await;
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_from_iter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let result = stream.collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_eval() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = eval(async { 42 });
        let result = stream.collect::<Vec<_>>().await;
        assert_eq!(result, vec![42]);
    });
}

#[test]
fn test_repeat() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = repeat(42);
        let result = stream.take(5).collect::<Vec<_>>().await;
        assert_eq!(result, vec![42, 42, 42, 42, 42]);
    });
}

#[test]
fn test_emit_after() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let start = std::time::Instant::now();
        let stream = emit_after(42, std::time::Duration::from_millis(100));
        let result = stream.collect::<Vec<_>>().await;
        let elapsed = start.elapsed();
        
        assert_eq!(result, vec![42]);
        assert!(elapsed.as_millis() >= 100, "Should have waited at least 100ms");
    });
}

#[test]
fn test_take() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let result = take(stream, 3).collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2, 3]);
    });
}

#[test]
fn test_drop() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let result = drop(stream, 2).collect::<Vec<_>>().await;
        assert_eq!(result, vec![3, 4, 5]);
    });
}