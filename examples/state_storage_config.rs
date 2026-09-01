//! Configuring state storage: TTL, size caps, cleanup, and custom backends.
//!
//! Run with: `cargo run --example state_storage_config`

use rs2_stream::state::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== building a StateConfig ===");
    let config = StateConfig::new()
        .ttl(Duration::from_secs(300))
        .cleanup_interval(Duration::from_secs(60))
        .max_size(10_000);
    println!("  {:?}", config);
    println!("  validate -> {:?}\n", config.validate());

    println!("=== validation catches nonsense ===");
    let bad = StateConfig::new().ttl(Duration::from_secs(0));
    println!("  zero ttl              -> {:?}", bad.validate());
    let bad = StateConfig::new()
        .ttl(Duration::from_secs(10))
        .cleanup_interval(Duration::from_secs(60));
    println!("  cleanup longer than ttl -> {:?}\n", bad.validate());

    println!("=== presets ===");
    for (name, c) in [
        ("high_performance", StateConfigs::high_performance()),
        ("session", StateConfigs::session()),
        ("short_lived", StateConfigs::short_lived()),
        ("long_lived", StateConfigs::long_lived()),
    ] {
        println!("  {:16} ttl={:?} cleanup={:?} max_size={:?}",
            name, c.ttl, c.cleanup_interval, c.max_size);
    }
    println!();

    println!("=== create_storage_arc / create_storage: build the backend ===");
    // The stateful operators call this for you; do it by hand when you want
    // to share one backend or inspect it.
    let shared = config.create_storage_arc();
    shared.set("user:1", b"payload").await.unwrap();
    println!("  via Arc backend  -> {:?}", shared.get("user:1").await.map(|v| v.len()));

    let owned = config.create_storage();
    owned.set("user:2", b"other").await.unwrap();
    println!("  via Box backend  -> {:?}\n", owned.get("user:2").await.map(|v| v.len()));

    println!("=== InMemoryState directly: TTL and expiry sweeping ===");
    // `with_cleanup_interval` decides how often expired entries are actually
    // reclaimed. Reads never return an expired entry either way, but without
    // a sweep the bytes stay allocated.
    let store = InMemoryState::new(Duration::from_millis(50))
        .with_cleanup_interval(Duration::from_millis(50));

    for i in 0..5 {
        store.set(&format!("k{}", i), b"payload").await.unwrap();
    }
    println!("  after 5 writes: live={} allocated={}", store.len().await, store.allocated_len().await);

    tokio::time::sleep(Duration::from_millis(120)).await;
    println!("  after TTL:      live={} allocated={} (still allocated, not yet swept)",
        store.len().await, store.allocated_len().await);
    println!("  expired read -> {:?}", store.get("k0").await);

    store.set("trigger", b"payload").await.unwrap();
    println!("  after a write past the cleanup interval: allocated={}\n", store.allocated_len().await);

    println!("=== with_max_size: a hard cap, oldest evicted first ===");
    let bounded = InMemoryState::new(Duration::from_secs(60)).with_max_size(3);
    for i in 0..10 {
        bounded.set(&format!("k{}", i), b"v").await.unwrap();
    }
    assert_eq!(bounded.allocated_len().await, 3, "max_size must be enforced");
    println!("  wrote 10 with max_size=3 -> allocated={}", bounded.allocated_len().await);
    println!("  oldest key k0 -> {:?}", bounded.get("k0").await);
    println!("  newest key k9 -> {:?}", bounded.get("k9").await.map(|v| v.len()));

    println!("\n  Note: the cap is enforced through a write-ordered index, so");
    println!("  staying at capacity costs microseconds per write, not milliseconds.");
}
