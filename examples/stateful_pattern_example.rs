use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::state::{CustomKeyExtractor, StatefulStreamExt, StateConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecurityEvent {
    ip_address: String,
    event_type: String,
    timestamp: u64,
    severity: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PatternState {
    events: Vec<String>,
    last_detection: u64,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 Stateful Pattern Example ===\n");

    // Create sample security events
    let events = vec![
        SecurityEvent {
            ip_address: "192.168.1.100".to_string(),
            event_type: "login_failed".to_string(),
            timestamp: 1000,
            severity: 2,
        },
        SecurityEvent {
            ip_address: "192.168.1.100".to_string(),
            event_type: "login_failed".to_string(),
            timestamp: 1100,
            severity: 2,
        },
        SecurityEvent {
            ip_address: "192.168.1.100".to_string(),
            event_type: "login_failed".to_string(),
            timestamp: 1200,
            severity: 2,
        },
        SecurityEvent {
            ip_address: "192.168.1.101".to_string(),
            event_type: "port_scan".to_string(),
            timestamp: 1300,
            severity: 3,
        },
        SecurityEvent {
            ip_address: "192.168.1.101".to_string(),
            event_type: "port_scan".to_string(),
            timestamp: 1400,
            severity: 3,
        },
        SecurityEvent {
            ip_address: "192.168.1.102".to_string(),
            event_type: "data_access".to_string(),
            timestamp: 1500,
            severity: 1,
        },
    ];

    // Example 1: Detect brute force attacks (3 failed logins)
    println!("1. Detect Brute Force Attacks (3 failed logins):");
    let brute_force_stream: Vec<String> = rs2_stream::rs2::from_iter_rs2(events.clone())
        .stateful_pattern_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|event: &SecurityEvent| event.ip_address.clone()),
            3, // pattern size
            |pattern_events, state_access| {
                let fut = async move {
                    let mut pattern = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        })
                    } else {
                        PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        }
                    };

                    // Check if all events are failed logins
                    let all_failed_logins = pattern_events
                        .iter()
                        .all(|event| event.event_type == "login_failed");

                    if all_failed_logins {
                        pattern.events = pattern_events
                            .iter()
                            .map(|e| e.event_type.clone())
                            .collect();
                        pattern.last_detection = pattern_events.last().unwrap().timestamp;

                        let pattern_bytes = serde_json::to_vec(&pattern).unwrap();
                        state_access.set(&pattern_bytes).await.unwrap();

                        Ok(Some(format!(
                            "BRUTE_FORCE_DETECTED: IP {} - {} failed logins",
                            pattern_events[0].ip_address,
                            pattern_events.len()
                        )))
                    } else {
                        Ok(None)
                    }
                };
                Box::pin(fut)
            },
        )
        .collect_rs2()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect();

    println!("  Brute force detections:");
    for detection in &brute_force_stream {
        println!("    {}", detection);
    }

    // Example 2: Detect port scanning (2 port scan events)
    println!("\n2. Detect Port Scanning (2 port scan events):");
    let anomaly_stream: Vec<String> = rs2_stream::rs2::from_iter_rs2(events.clone())
        .stateful_pattern_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|event: &SecurityEvent| event.ip_address.clone()),
            2, // pattern size
            |pattern_events, state_access| {
                let fut = async move {
                    let mut ip_pattern = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        })
                    } else {
                        PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        }
                    };

                    // Check if all events are port scans
                    let all_port_scans = pattern_events
                        .iter()
                        .all(|event| event.event_type == "port_scan");

                    if all_port_scans {
                        ip_pattern.events = pattern_events
                            .iter()
                            .map(|e| e.event_type.clone())
                            .collect();
                        ip_pattern.last_detection = pattern_events.last().unwrap().timestamp;

                        let pattern_bytes = serde_json::to_vec(&ip_pattern).unwrap();
                        state_access.set(&pattern_bytes).await.unwrap();

                        Ok(Some(format!(
                            "PORT_SCAN_DETECTED: IP {} - {} scan events",
                            pattern_events[0].ip_address,
                            pattern_events.len()
                        )))
                    } else {
                        Ok(None)
                    }
                };
                Box::pin(fut)
            },
        )
        .collect_rs2()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect();

    println!("  Port scan detections:");
    for detection in &anomaly_stream {
        println!("    {}", detection);
    }

    // Example 3: Detect time-based patterns (events within 100ms)
    println!("\n3. Detect Time-Based Patterns (events within 100ms):");
    let time_pattern_stream: Vec<String> = rs2_stream::rs2::from_iter_rs2(events.clone())
        .stateful_pattern_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|event: &SecurityEvent| event.ip_address.clone()),
            2, // pattern size
            |pattern_events, state_access| {
                let fut = async move {
                    let mut time_pattern = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        })
                    } else {
                        PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        }
                    };

                    // Check if events are within 100ms of each other
                    let time_diff = pattern_events[1].timestamp - pattern_events[0].timestamp;
                    let rapid_sequence = time_diff <= 100;

                    if rapid_sequence {
                        time_pattern.events = pattern_events
                            .iter()
                            .map(|e| e.event_type.clone())
                            .collect();
                        time_pattern.last_detection = pattern_events.last().unwrap().timestamp;
                        let pattern_bytes = serde_json::to_vec(&time_pattern).unwrap();
                        state_access.set(&pattern_bytes).await.unwrap();
                        Ok(Some(format!(
                            "RAPID_SEQUENCE_DETECTED: IP {} - {} events in {}ms",
                            pattern_events[0].ip_address,
                            pattern_events.len(),
                            time_diff
                        )))
                    } else {
                        Ok(None)
                    }
                };
                Box::pin(fut)
            },
        )
        .collect_rs2()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect();

    println!("  Time-based pattern detections:");
    for detection in &time_pattern_stream {
        println!("    {}", detection);
    }

    // Example 4: Detect multi-pattern sequences
    println!("\n4. Detect Multi-Pattern Sequences:");
    let multi_pattern_stream: Vec<String> = rs2_stream::rs2::from_iter_rs2(events.clone())
        .stateful_pattern_rs2(
            StateConfig::default(),
            CustomKeyExtractor::new(|event: &SecurityEvent| event.ip_address.clone()),
            3, // pattern size
            |pattern_events, state_access| {
                let fut = async move {
                    let mut multi_pattern = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        })
                    } else {
                        PatternState {
                            events: Vec::new(),
                            last_detection: 0,
                        }
                    };

                    // Check for mixed pattern (failed login followed by data access)
                    let event_types: Vec<&str> = pattern_events
                        .iter()
                        .map(|e| e.event_type.as_str())
                        .collect();
                    let mixed_pattern = event_types.contains(&"login_failed")
                        && event_types.contains(&"data_access");

                    if mixed_pattern {
                        multi_pattern.events = pattern_events
                            .iter()
                            .map(|e| e.event_type.clone())
                            .collect();
                        multi_pattern.last_detection = pattern_events.last().unwrap().timestamp;
                        let pattern_bytes = serde_json::to_vec(&multi_pattern).unwrap();
                        state_access.set(&pattern_bytes).await.unwrap();
                        Ok(Some(format!(
                            "MIXED_PATTERN_DETECTED: IP {} - sequence: {:?}",
                            pattern_events[0].ip_address, event_types
                        )))
                    } else {
                        Ok(None)
                    }
                };
                Box::pin(fut)
            },
        )
        .collect_rs2()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect();

    println!("  Multi-pattern detections:");
    for detection in &multi_pattern_stream {
        println!("    {}", detection);
    }

    println!("\n=== Stateful Pattern Example Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("1. Brute force detection: Multiple failed login attempts from same IP");
    println!("2. Port scan detection: Multiple port scan events from same IP");
    println!("3. Time-based patterns: Events occurring within rapid timeframes");
    println!("4. Multi-pattern detection: Complex sequences across different event types");
    println!("5. Stateful pattern matching: Maintaining state across pattern detections");
}
