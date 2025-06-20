use futures::StreamExt;
use rs2_stream::state::{CustomKeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    action: String,
    timestamp: u64,
    is_new_session: Option<bool>, // Optional to allow for events without session info
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEvent {
    user_id: String,
    action: String,
    timestamp: u64,
    is_new_session: bool,
}

#[tokio::main]
async fn main() {
    println!("\n=== Stateful Session Example ===");
    let events = vec![
        UserEvent {
            user_id: "alice".to_string(),
            action: "login".to_string(),
            timestamp: 1000,
            is_new_session: None,
        },
        UserEvent {
            user_id: "alice".to_string(),
            action: "click".to_string(),
            timestamp: 1020,
            is_new_session: None,
        },
        UserEvent {
            user_id: "bob".to_string(),
            action: "login".to_string(),
            timestamp: 1050,
            is_new_session: None,
        },
        UserEvent {
            user_id: "alice".to_string(),
            action: "logout".to_string(),
            timestamp: 1100,
            is_new_session: None,
        },
        UserEvent {
            user_id: "bob".to_string(),
            action: "click".to_string(),
            timestamp: 1120,
            is_new_session: None,
        },
    ];

    let session_config = StateConfig::new();
    let session_stream = futures::stream::iter(events).stateful_session_rs2(
        session_config,
        CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
        std::time::Duration::from_secs(60),
        |mut event, is_new_session| {
            event.is_new_session = Some(is_new_session);
            event
        },
    );

    let session_results: Vec<_> = session_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for event in session_results {
        println!(
            "User: {}, Action: {}, New Session: {}",
            event.user_id,
            event.action,
            event.is_new_session.unwrap_or(false)
        );
    }
    println!("=== Stateful Session Example Complete ===");
}
