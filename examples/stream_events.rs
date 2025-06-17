//! Example of handling media stream events
//!
//! This example demonstrates how to:
//! 1. Create and handle MediaStreamEvent objects
//! 2. Convert events to UserActivity for analytics
//! 3. Process events in a stream
//! 4. Implement a simple event handler

use rs2_stream::media::events::MediaStreamEvent;
use rs2_stream::media::types::{QualityLevel, UserActivity};
use rs2_stream::rs2::*;
use futures_util::StreamExt;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Media Stream Events Example");

    // Create a stream of events
    let event_stream = create_sample_event_stream();

    // Create an event handler
    let mut event_handler = EventHandler::new();

    // Process the events
    println!("Processing events...\n");

    let mut event_stream = std::pin::pin!(event_stream);
    while let Some(event) = event_stream.next().await {
        // Handle the event
        event_handler.handle_event(&event).await;

        // Convert to UserActivity for analytics
        let activity: UserActivity = event.into();

        // Print the activity
        println!(
            "Activity: {} by user {} at {}", 
            activity.activity_type,
            activity.user_id,
            activity.timestamp
        );

        // Print some metadata
        if !activity.metadata.is_empty() {
            println!("  Metadata:");
            for (key, value) in &activity.metadata {
                println!("    {}: {}", key, value);
            }
        }

        println!();

        // Small delay for readability
        sleep(Duration::from_millis(500)).await;
    }

    // Print summary
    let stats = event_handler.get_stats();
    println!("\nEvent Statistics:");
    println!("  Total events: {}", stats.total_events);
    println!("  Stream started events: {}", stats.stream_started);
    println!("  Stream stopped events: {}", stats.stream_stopped);
    println!("  Quality changed events: {}", stats.quality_changed);
    println!("  Buffer underrun events: {}", stats.buffer_underrun);
    println!("  Chunk dropped events: {}", stats.chunk_dropped);

    Ok(())
}

// Create a sample stream of events for demonstration
fn create_sample_event_stream() -> RS2Stream<MediaStreamEvent> {
    let stream_id = "example-stream-123".to_string();
    let user_id = 42;
    let now = Utc::now();

    // Create a vector of events
    let events = vec![
        // Stream started
        MediaStreamEvent::StreamStarted {
            stream_id: stream_id.clone(),
            user_id,
            quality: QualityLevel::High,
            timestamp: now,
        },

        // Quality changed after 2 seconds
        MediaStreamEvent::QualityChanged {
            stream_id: stream_id.clone(),
            user_id,
            old_quality: QualityLevel::High,
            new_quality: QualityLevel::Medium,
            timestamp: now + chrono::Duration::seconds(2),
        },

        // Buffer underrun after 5 seconds
        MediaStreamEvent::BufferUnderrun {
            stream_id: stream_id.clone(),
            user_id,
            buffer_level: 0.1,
            timestamp: now + chrono::Duration::seconds(5),
        },

        // Chunk dropped after 6 seconds
        MediaStreamEvent::ChunkDropped {
            stream_id: stream_id.clone(),
            user_id,
            sequence_number: 42,
            reason: "Network congestion".to_string(),
            timestamp: now + chrono::Duration::seconds(6),
        },

        // Quality changed back after 8 seconds
        MediaStreamEvent::QualityChanged {
            stream_id: stream_id.clone(),
            user_id,
            old_quality: QualityLevel::Medium,
            new_quality: QualityLevel::High,
            timestamp: now + chrono::Duration::seconds(8),
        },

        // Stream stopped after 10 seconds
        MediaStreamEvent::StreamStopped {
            stream_id: stream_id.clone(),
            user_id,
            duration_seconds: 10,
            bytes_transferred: 1_500_000,
            timestamp: now + chrono::Duration::seconds(10),
        },
    ];

    // Convert to a stream
    from_iter(events)
}

// Simple event handler
struct EventHandler {
    stats: EventStats,
}

#[derive(Default)]
struct EventStats {
    total_events: usize,
    stream_started: usize,
    stream_stopped: usize,
    quality_changed: usize,
    buffer_underrun: usize,
    chunk_dropped: usize,
}

impl EventHandler {
    fn new() -> Self {
        Self {
            stats: EventStats::default(),
        }
    }

    async fn handle_event(&mut self, event: &MediaStreamEvent) -> () {
        // In a real application, this would do something with the event
        // like logging, alerting, or triggering adaptive behavior

        // For this example, we just print the event details
        match event {
            MediaStreamEvent::StreamStarted { stream_id, user_id, quality, timestamp } => {
                println!(
                    "Stream started: id={}, user={}, quality={:?}, time={}", 
                    stream_id, user_id, quality, timestamp
                );
                self.stats.stream_started += 1;
            },
            MediaStreamEvent::StreamStopped { stream_id, user_id, duration_seconds, bytes_transferred, timestamp } => {
                println!(
                    "Stream stopped: id={}, user={}, duration={}s, bytes={}, time={}", 
                    stream_id, user_id, duration_seconds, bytes_transferred, timestamp
                );
                self.stats.stream_stopped += 1;
            },
            MediaStreamEvent::QualityChanged { stream_id, user_id, old_quality, new_quality, timestamp } => {
                println!(
                    "Quality changed: id={}, user={}, old={:?}, new={:?}, time={}", 
                    stream_id, user_id, old_quality, new_quality, timestamp
                );
                self.stats.quality_changed += 1;
            },
            MediaStreamEvent::BufferUnderrun { stream_id, user_id, buffer_level, timestamp } => {
                println!(
                    "Buffer underrun: id={}, user={}, level={:.2}, time={}", 
                    stream_id, user_id, buffer_level, timestamp
                );
                self.stats.buffer_underrun += 1;
            },
            MediaStreamEvent::ChunkDropped { stream_id, user_id, sequence_number, reason, timestamp } => {
                println!(
                    "Chunk dropped: id={}, user={}, seq={}, reason='{}', time={}", 
                    stream_id, user_id, sequence_number, reason, timestamp
                );
                self.stats.chunk_dropped += 1;
            },
        }

        self.stats.total_events += 1;
    }

    fn get_stats(&self) -> &EventStats {
        &self.stats
    }
}
