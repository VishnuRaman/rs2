//! Building a pipeline: named stages, config, and validation.
//!
//! A `Pipeline` threads one stream through its nodes in order. `validate()`
//! rejects orderings that could not work — a second source would discard the
//! first stream, and anything after a sink would never run.
//!
//! Run with: `cargo run --example pipeline_builder`

use futures_util::stream::StreamExt;
use rs2_stream::pipeline::builder::{Pipeline, PipelineConfig};
use rs2_stream::rs2::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("=== named stages: the names appear in errors ===");
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = seen.clone();

    let result = Pipeline::<u32>::new()
        .with_config(PipelineConfig {
            name: "orders".to_string(),
            buffer_size: 256,
            enable_metrics: false,
        })
        .named_source("orders-source", || from_iter(1..=10u32))
        .named_transform("double", |s| s.map_rs2(|x| x * 2).boxed())
        .named_transform("only-large", |s| s.filter_rs2(|x| *x > 8).boxed())
        .named_sink("counter", move |s| {
            let counter = counter.clone();
            Box::pin(async move {
                let items = s.collect::<Vec<_>>().await;
                counter.store(items.len(), Ordering::SeqCst);
                println!("  sink received: {:?}", items);
            })
        })
        .run()
        .await;

    // 1..=10 doubled => 2,4,..,20; keeping >8 leaves 10,12,14,16,18,20
    assert!(result.is_ok());
    assert_eq!(seen.load(Ordering::SeqCst), 6);
    println!("  run -> {:?}, {} items\n", result.is_ok(), seen.load(Ordering::SeqCst));

    println!("=== unnamed helpers: source / transform / sink ===");
    // These are just `named_*` with a default name.
    let result = Pipeline::<u32>::new()
        .source(|| from_iter(1..=5u32))
        .transform(|s| s.map_rs2(|x| x + 100).boxed())
        .sink(|s| {
            Box::pin(async move {
                println!("  -> {:?}", s.collect::<Vec<_>>().await);
            })
        })
        .run()
        .await;
    println!("  run -> {:?}\n", result.is_ok());

    println!("=== validate() catches orderings that cannot work ===");

    // Two sources: `run` threads one stream through, so the first would be
    // silently discarded. This used to pass validation and run.
    let two_sources = Pipeline::<u32>::new()
        .named_source("a", || from_iter(0..3u32))
        .named_source("b", || from_iter(0..3u32))
        .sink(|s| Box::pin(async move { let _ = s.collect::<Vec<_>>().await; }))
        .validate();
    println!("  two sources      -> {:?}", two_sources.map_err(|e| e.to_string()));

    // A transform after a sink can never run: the stream is already consumed.
    let after_sink = Pipeline::<u32>::new()
        .source(|| from_iter(0..3u32))
        .sink(|s| Box::pin(async move { let _ = s.collect::<Vec<_>>().await; }))
        .named_transform("too-late", |s| s)
        .validate();
    println!("  transform < sink -> {:?}", after_sink.map_err(|e| e.to_string()));

    // Missing pieces are still caught.
    let no_sink = Pipeline::<u32>::new().source(|| from_iter(0..3u32)).validate();
    println!("  no sink          -> {:?}", no_sink.map_err(|e| e.to_string()));

    println!("\n=== branch: fan out to two sinks ===");
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    let (a2, b2) = (a.clone(), b.clone());
    let result = Pipeline::<u32>::new()
        .with_config(PipelineConfig {
            name: "fan-out".to_string(),
            // buffer_size bounds the broadcast; a sink that falls further
            // behind than this logs a lag warning rather than ending early.
            buffer_size: 1024,
            enable_metrics: false,
        })
        .source(|| from_iter(0..50u32))
        .branch(
            "split",
            move |s| {
                let a2 = a2.clone();
                Box::pin(async move { a2.store(s.collect::<Vec<_>>().await.len(), Ordering::SeqCst) })
            },
            move |s| {
                let b2 = b2.clone();
                Box::pin(async move { b2.store(s.collect::<Vec<_>>().await.len(), Ordering::SeqCst) })
            },
        )
        .run()
        .await;
    println!(
        "  run -> {:?}, sink A got {}, sink B got {}",
        result.is_ok(),
        a.load(Ordering::SeqCst),
        b.load(Ordering::SeqCst)
    );
    println!("     (both sinks see every item — they subscribe before the feed starts)");
}
