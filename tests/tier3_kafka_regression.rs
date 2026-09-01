//! Item 4 regression tests: the Kafka connector.
//!
//! These need a running Docker daemon — they start a real Kafka broker via
//! testcontainers rather than mocking the client.

use futures_util::StreamExt;
use rs2_stream::connectors::kafka_connector::KafkaConfig;
use rs2_stream::connectors::*;
use serde::{Deserialize, Serialize};
use serial_test::serial;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::*;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Event {
    user_id: String,
    seq: u32,
}

struct Env {
    connector: KafkaConnector,
    topic: String,
    _container: ContainerAsync<Kafka>,
}

impl Env {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let container = Kafka::default().with_jvm_image().start().await?;
        let bootstrap = format!(
            "0.0.0.0:{}",
            container.get_host_port_ipv4(KAFKA_PORT).await?
        );
        let connector = KafkaConnector::new(&bootstrap);

        for i in 0..30 {
            if <KafkaConnector as StreamConnector<String>>::health_check(&connector)
                .await
                .is_ok()
            {
                break;
            }
            if i == 29 {
                return Err("Kafka container failed to start within 30s".into());
            }
            sleep(Duration::from_millis(1000)).await;
        }

        Ok(Self {
            connector,
            topic: format!("tier3-{}", Uuid::new_v4()),
            _container: container,
        })
    }

    /// Producer-side config: no consumer group needed.
    fn config(&self) -> KafkaConfig {
        KafkaConfig {
            topic: self.topic.clone(),
            from_beginning: true,
            ..Default::default()
        }
    }

    /// Consumer-side config. Kafka requires a group to subscribe.
    fn consumer_config(&self) -> KafkaConfig {
        KafkaConfig {
            group_id: Some(format!("g-{}", Uuid::new_v4())),
            ..self.config()
        }
    }
}

// ---------------------------------------------------------------------------
// key_field replaces the payload-splitting hack
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn key_field_partitions_by_the_named_field() {
    let env = match Env::new().await {
        Ok(e) => e,
        Err(e) => panic!("Kafka container unavailable: {e}"),
    };

    // The old code parsed each payload as a JSON *string* and split on '-',
    // deriving keys from arbitrary user data. A struct payload got no key at
    // all; a string payload got a key nobody asked for.
    let config = KafkaConfig {
        key_field: Some("user_id".to_string()),
        ..env.config()
    };

    let events: Vec<Event> = (0..6)
        .map(|seq| Event {
            user_id: format!("u{}", seq % 2),
            seq,
        })
        .collect();

    let meta = env
        .connector
        .to_sink(
            rs2_stream::rs2::from_iter(events.clone()),
            config.clone(),
        )
        .await
        .expect("produce");
    assert_eq!(meta.messages_produced, 6);

    // Round-trip: every event must come back intact regardless of keying.
    let stream = <KafkaConnector as StreamConnector<Event>>::from_source(
        &env.connector,
        env.consumer_config(),
    )
    .await
    .expect("consume");

    let got: Vec<Event> = stream.take(6).collect::<Vec<_>>().await;
    assert_eq!(got.len(), 6, "all produced events must be consumable");
    for e in &events {
        assert!(got.contains(e), "missing event {:?}", e);
    }
}

#[tokio::test]
#[serial]
async fn absent_key_field_still_produces() {
    let env = Env::new().await.expect("kafka");

    // key_field naming a field that does not exist must not fail the send —
    // the record simply goes without a key.
    let config = KafkaConfig {
        key_field: Some("nonexistent".to_string()),
        ..env.config()
    };

    let meta = env
        .connector
        .to_sink(
            rs2_stream::rs2::from_iter(vec![Event {
                user_id: "u1".into(),
                seq: 1,
            }]),
            config,
        )
        .await
        .expect("produce");

    assert_eq!(meta.messages_produced, 1);
}

// ---------------------------------------------------------------------------
// deserialization failures surface instead of being silently dropped
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn malformed_messages_surface_as_errors() {
    let env = Env::new().await.expect("kafka");

    // Produce Strings, then consume them as Event. Every message is undecodable.
    let _ = <KafkaConnector as StreamConnector<String>>::to_sink(
        &env.connector,
        rs2_stream::rs2::from_iter(vec!["not-an-event".to_string(), "also-not".to_string()]),
        env.config(),
    )
    .await
    .expect("produce");

    let stream = env
        .connector
        .from_source_with_errors::<Event>(env.consumer_config())
        .await
        .expect("consume");

    let got: Vec<_> = stream.take(2).collect::<Vec<_>>().await;
    assert_eq!(got.len(), 2);
    assert!(
        got.iter().all(|r| r.is_err()),
        "undecodable messages must surface as Err, not be skipped: {:?}",
        got.iter().map(|r| r.is_ok()).collect::<Vec<_>>()
    );
}

#[tokio::test]
#[serial]
async fn well_formed_messages_come_back_as_ok() {
    let env = Env::new().await.expect("kafka");

    let events: Vec<Event> = (0..3)
        .map(|seq| Event {
            user_id: "u1".into(),
            seq,
        })
        .collect();

    let _ = env
        .connector
        .to_sink(rs2_stream::rs2::from_iter(events.clone()), env.config())
        .await
        .expect("produce");

    let stream = env
        .connector
        .from_source_with_errors::<Event>(env.consumer_config())
        .await
        .expect("consume");

    let got: Vec<Event> = stream
        .take(3)
        .filter_map(|r| async move { r.ok() })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got.len(), 3);
}

// ---------------------------------------------------------------------------
// metadata() reports the real cluster, not placeholders
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn metadata_reports_real_broker_state() {
    let env = Env::new().await.expect("kafka");

    let _ = env
        .connector
        .to_sink(
            rs2_stream::rs2::from_iter(vec![Event {
                user_id: "u1".into(),
                seq: 1,
            }]),
            env.config(),
        )
        .await
        .expect("produce");

    // Previously returned topic "unknown" and partition_count 0 without ever
    // contacting the broker.
    let topic_md = env
        .connector
        .topic_metadata(&env.topic)
        .await
        .expect("topic metadata");

    assert_eq!(topic_md.topic, env.topic);
    assert!(
        topic_md.partition_count >= 1,
        "real topic must report at least one partition, got {}",
        topic_md.partition_count
    );

    let cluster_md = <KafkaConnector as StreamConnector<Event>>::metadata(&env.connector)
        .await
        .expect("cluster metadata");
    assert!(
        cluster_md.partition_count >= 1,
        "cluster metadata must reflect real partitions"
    );
}

// ---------------------------------------------------------------------------
// manual commit path
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn manual_commit_persists_offsets_across_consumers() {
    let env = Env::new().await.expect("kafka");
    let group = format!("manual-{}", Uuid::new_v4());

    let events: Vec<Event> = (0..4)
        .map(|seq| Event {
            user_id: "u1".into(),
            seq,
        })
        .collect();
    let _ = env
        .connector
        .to_sink(rs2_stream::rs2::from_iter(events), env.config())
        .await
        .expect("produce");

    let config = KafkaConfig {
        group_id: Some(group.clone()),
        ..env.config()
    };

    // Consume two, commit, drop the consumer.
    {
        let (stream, handle) = env
            .connector
            .from_source_manual_commit::<Event>(config.clone())
            .await
            .expect("consume");
        let first: Vec<_> = stream.take(2).collect::<Vec<_>>().await;
        assert_eq!(first.len(), 2);
        handle.commit_sync().expect("commit");
    }

    sleep(Duration::from_millis(500)).await;

    // A new consumer in the same group resumes after the committed offset.
    let resumed = KafkaConfig {
        group_id: Some(group),
        from_beginning: false,
        ..env.config()
    };
    let (stream, _handle) = env
        .connector
        .from_source_manual_commit::<Event>(resumed)
        .await
        .expect("resume");

    let rest = tokio::time::timeout(
        Duration::from_secs(20),
        stream.take(2).collect::<Vec<_>>(),
    )
    .await
    .expect("resumed consumer should receive the uncommitted remainder");

    let seqs: Vec<u32> = rest.into_iter().filter_map(|r| r.ok()).map(|e| e.seq).collect();
    assert_eq!(
        seqs.len(),
        2,
        "committed offsets were not honoured on resume: {:?}",
        seqs
    );
    assert!(
        !seqs.contains(&0),
        "resumed consumer re-read an already-committed message: {:?}",
        seqs
    );
}

#[tokio::test]
#[serial]
async fn consuming_without_a_group_reports_a_usable_error() {
    let env = Env::new().await.expect("kafka");

    // librdkafka's own failure here is `Local: Unknown group`, which says
    // nothing about what the caller did wrong.
    let result = <KafkaConnector as StreamConnector<Event>>::from_source(
        &env.connector,
        env.config(), // no group_id
    )
    .await;

    match result {
        Err(ConnectorError::InvalidConfiguration(msg)) => {
            assert!(
                msg.contains("group"),
                "error should name the missing consumer group: {msg}"
            );
        }
        Err(other) => panic!("expected InvalidConfiguration, got {other:?}"),
        Ok(_) => panic!("consuming without a consumer group should fail"),
    }
}
