use futures::StreamExt;
use rs2_stream::state::{CustomKeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transaction {
    user_id: String,
    amount: f64,
    category: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserAggregation {
    total_spent: f64,
    transaction_count: u64,
    categories: HashMap<String, f64>,
    last_transaction: u64,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 Stateful Reduce Example ===\n");

    // Create sample transaction stream
    let transactions = vec![
        Transaction {
            user_id: "user1".to_string(),
            amount: 100.0,
            category: "food".to_string(),
            timestamp: 1000,
        },
        Transaction {
            user_id: "user1".to_string(),
            amount: 50.0,
            category: "transport".to_string(),
            timestamp: 1100,
        },
        Transaction {
            user_id: "user1".to_string(),
            amount: 200.0,
            category: "food".to_string(),
            timestamp: 1200,
        },
        Transaction {
            user_id: "user2".to_string(),
            amount: 75.0,
            category: "entertainment".to_string(),
            timestamp: 1300,
        },
        Transaction {
            user_id: "user2".to_string(),
            amount: 150.0,
            category: "shopping".to_string(),
            timestamp: 1400,
        },
    ];

    // Example 1: Simple aggregation with custom config
    println!("1. Simple Transaction Aggregation:");
    let custom_config = StateConfig::new();

    let aggregation_stream = futures::stream::iter(transactions.clone()).stateful_reduce_rs2(
        custom_config,
        CustomKeyExtractor::new(|tx: &Transaction| tx.user_id.clone()),
        UserAggregation {
            total_spent: 0.0,
            transaction_count: 0,
            categories: HashMap::new(),
            last_transaction: 0,
        },
        |acc, transaction, state_access| {
            Box::pin(async move {
                // Get current state
                let mut state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(acc.clone())
                } else {
                    acc.clone()
                };

                // Update aggregation
                state.total_spent += transaction.amount;
                state.transaction_count += 1;
                state.last_transaction = transaction.timestamp;

                // Update category spending
                *state
                    .categories
                    .entry(transaction.category.clone())
                    .or_insert(0.0) += transaction.amount;

                // Persist state
                let bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&bytes).await.unwrap();

                // Return updated aggregation
                Ok(state)
            })
        },
    );

    let results: Vec<UserAggregation> = aggregation_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for (i, result) in results.iter().enumerate() {
        println!(
            "  Transaction {}: User {} - Total: ${:.2}, Count: {}, Categories: {:?}",
            i + 1,
            if i < 3 { "user1" } else { "user2" },
            result.total_spent,
            result.transaction_count,
            result.categories
        );
    }

    // Example 2: Real-time analytics with session config
    println!("\n2. Real-time Analytics with Session Config:");
    let session_config = StateConfig::new();

    let analytics_stream = futures::stream::iter(transactions.clone()).stateful_reduce_rs2(
        session_config,
        CustomKeyExtractor::new(|tx: &Transaction| "global_analytics".to_string()),
        HashMap::<String, f64>::new(),
        |acc, transaction, state_access| {
            Box::pin(async move {
                // Get current analytics state
                let mut analytics = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(acc.clone())
                } else {
                    acc.clone()
                };

                // Update global analytics
                *analytics.entry("total_revenue".to_string()).or_insert(0.0) += transaction.amount;
                *analytics
                    .entry("total_transactions".to_string())
                    .or_insert(0.0) += 1.0;
                *analytics
                    .entry(format!("category_{}", transaction.category))
                    .or_insert(0.0) += transaction.amount;

                // Persist analytics state
                let bytes = serde_json::to_vec(&analytics).unwrap();
                state_access.set(&bytes).await.unwrap();

                Ok(analytics)
            })
        },
    );

    let analytics_results: Vec<HashMap<String, f64>> = analytics_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for (i, analytics) in analytics_results.iter().enumerate() {
        println!("  Analytics {}: Revenue: ${:.2}, Transactions: {:.0}, Food: ${:.2}, Transport: ${:.2}, Entertainment: ${:.2}, Shopping: ${:.2}",
                 i + 1,
                 analytics.get("total_revenue").unwrap_or(&0.0),
                 analytics.get("total_transactions").unwrap_or(&0.0),
                 analytics.get("category_food").unwrap_or(&0.0),
                 analytics.get("category_transport").unwrap_or(&0.0),
                 analytics.get("category_entertainment").unwrap_or(&0.0),
                 analytics.get("category_shopping").unwrap_or(&0.0));
    }

    // Example 3: Fraud detection with long-lived config
    println!("\n3. Fraud Detection with Long-lived Config:");
    let fraud_config = StateConfig::new();

    let fraud_stream = futures::stream::iter(transactions.clone()).stateful_reduce_rs2(
        fraud_config,
        CustomKeyExtractor::new(|tx: &Transaction| tx.user_id.clone()),
        (0.0, 0u64, false), // (total_amount, count, flagged)
        |acc, transaction, state_access| {
            Box::pin(async move {
                // Get current fraud state
                let mut fraud_state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(acc.clone())
                } else {
                    acc.clone()
                };

                // Update fraud metrics
                fraud_state.0 += transaction.amount;
                fraud_state.1 += 1;

                // Flag if suspicious activity detected
                if fraud_state.0 > 500.0 || fraud_state.1 > 10 {
                    fraud_state.2 = true;
                }

                // Persist fraud state
                let bytes = serde_json::to_vec(&fraud_state).unwrap();
                state_access.set(&bytes).await.unwrap();

                Ok(fraud_state)
            })
        },
    );

    let fraud_results: Vec<(f64, u64, bool)> = fraud_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for (i, fraud_state) in fraud_results.iter().enumerate() {
        println!(
            "  Fraud {}: Total: ${:.2}, Count: {}, Flagged: {}",
            i + 1,
            fraud_state.0,
            fraud_state.1,
            fraud_state.2
        );
    }

    println!("\n=== Stateful Reduce Example Complete ===");
}
