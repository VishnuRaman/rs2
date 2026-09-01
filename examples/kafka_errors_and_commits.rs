//! Kafka: surfacing decode failures, manual offset commits, and real metadata.
//!
//! Requires a running Kafka broker. Set RS2_KAFKA_BROKERS, e.g.
//!   docker run -p 9092:9092 apache/kafka:latest
//!   RS2_KAFKA_BROKERS=localhost:9092 cargo run --example kafka_errors_and_commits
//!
//! Without it the example explains what it would do and exits cleanly.

use futures_util::stream::StreamExt;
use rs2_stream::connectors::kafka_connector::KafkaConfig;
use rs2_stream::connectors::*;
use rs2_stream::rs2::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    user_id: String,
    seq: u32,
}

#[tokio::main]
async fn main() {
    let brokers = match std::env::var("RS2_KAFKA_BROKERS") {
        Ok(b) => b,
        Err(_) => {
            println!("RS2_KAFKA_BROKERS not set — skipping the live section.\n");
            explain();
            return;
        }
    };

    let connector = KafkaConnector::new(&brokers);
    if <KafkaConnector as StreamConnector<Event>>::health_check(&connector).await.is_err() {
        println!("Could not reach Kafka at {} — skipping the live section.\n", brokers);
        explain();
        return;
    }

    let topic = format!("rs2-example-{}", uuid_like());
    let produce_cfg = KafkaConfig {
        topic: topic.clone(),
        from_beginning: true,
        // Partition key comes from a named JSON field. Dotted paths work too.
        // (An earlier version derived keys by splitting payloads on '-'.)
        key_field: Some("user_id".to_string()),
        ..Default::default()
    };
    let consume_cfg = KafkaConfig {
        group_id: Some(format!("grp-{}", uuid_like())),
        key_field: None,
        ..produce_cfg.clone()
    };

    println!("=== produce, keyed by user_id ===");
    let events: Vec<Event> = (0..4)
        .map(|seq| Event { user_id: format!("u{}", seq % 2), seq })
        .collect();
    let meta = connector.to_sink(from_iter(events), produce_cfg.clone()).await.unwrap();
    println!("  produced {} messages, {} bytes\n", meta.messages_produced, meta.bytes_sent);

    println!("=== from_source_with_errors: decode failures are visible ===");
    // `from_source` yields bare `T` and must silently skip anything it cannot
    // decode. This variant yields Result so you can see and count failures.
    let stream = connector
        .from_source_with_errors::<Event>(consume_cfg.clone())
        .await
        .unwrap();
    let batch: Vec<_> = stream.take(4).collect::<Vec<_>>().await;
    let ok = batch.iter().filter(|r| r.is_ok()).count();
    let bad = batch.iter().filter(|r| r.is_err()).count();
    println!("  {} decoded, {} failed\n", ok, bad);

    println!("=== manual commits ===");
    // enable_auto_commit was always configurable, but with no way to commit by
    // hand — so setting it false meant offsets were simply never committed.
    let group = format!("manual-{}", uuid_like());
    let manual_cfg = KafkaConfig { group_id: Some(group.clone()), ..consume_cfg.clone() };
    {
        let (stream, handle) = connector
            .from_source_manual_commit::<Event>(manual_cfg.clone())
            .await
            .unwrap();
        let first: Vec<_> = stream.take(2).collect::<Vec<_>>().await;
        println!("  consumed {} then committed", first.len());
        handle.commit_sync().expect("commit");
        // `handle.commit()` is the fire-and-forget variant.
    }
    println!("  a new consumer in group `{}` resumes after that offset\n", group);

    println!("=== real metadata ===");
    // `metadata()` used to return "unknown" and zeros without contacting the
    // broker at all.
    let topic_md = connector.topic_metadata(&topic).await.unwrap();
    println!("  topic {:?}: {} partition(s)", topic_md.topic, topic_md.partition_count);
    let cluster_md = connector.fetch_metadata(None).await.unwrap();
    println!("  cluster-wide: {} partition(s) across all topics", cluster_md.partition_count);
}

fn explain() {
    println!("This example demonstrates:");
    println!("  KafkaConfig::key_field          partition key from a named JSON field");
    println!("  from_source_with_errors()       decode failures as Err instead of silently skipped");
    println!("  from_source_manual_commit()     stream + CommitHandle");
    println!("  CommitHandle::commit_sync()     commit and wait for the broker");
    println!("  topic_metadata() / fetch_metadata()  real broker state, not placeholders");
    println!("\nSee tests/tier3_kafka_regression.rs for the same paths under testcontainers.");
}

/// Small unique suffix so repeated runs do not collide.
fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!("{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())
}
