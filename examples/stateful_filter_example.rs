use futures::StreamExt;
use rs2_stream::state::config::StateConfig;
use rs2_stream::state::{CustomKeyExtractor, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Transaction {
    transaction_id: String,
    user_id: String,
    amount: f64,
    category: String,
    timestamp: u64,
    merchant: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserRiskProfile {
    total_transactions: u64,
    total_amount: f64,
    average_amount: f64,
    category_counts: HashMap<String, u64>,
    last_transaction_time: u64,
    risk_score: f64,
}

#[tokio::main]
async fn main() {
    println!("\n=== Stateful Filter Example ===\n");

    // Create sample transactions
    let transactions = vec![
        Transaction {
            transaction_id: "T1".to_string(),
            user_id: "alice".to_string(),
            amount: 50.0,
            category: "food".to_string(),
            timestamp: 1000,
            merchant: "Grocery Store".to_string(),
        },
        Transaction {
            transaction_id: "T2".to_string(),
            user_id: "alice".to_string(),
            amount: 200.0,
            category: "electronics".to_string(),
            timestamp: 1020,
            merchant: "Tech Shop".to_string(),
        },
        Transaction {
            transaction_id: "T3".to_string(),
            user_id: "alice".to_string(),
            amount: 1000.0,
            category: "luxury".to_string(),
            timestamp: 1040,
            merchant: "Jewelry Store".to_string(),
        },
        Transaction {
            transaction_id: "T4".to_string(),
            user_id: "bob".to_string(),
            amount: 25.0,
            category: "food".to_string(),
            timestamp: 1100,
            merchant: "Restaurant".to_string(),
        },
        Transaction {
            transaction_id: "T5".to_string(),
            user_id: "bob".to_string(),
            amount: 5000.0,
            category: "luxury".to_string(),
            timestamp: 1120,
            merchant: "Car Dealer".to_string(),
        },
        Transaction {
            transaction_id: "T6".to_string(),
            user_id: "charlie".to_string(),
            amount: 75.0,
            category: "entertainment".to_string(),
            timestamp: 1200,
            merchant: "Movie Theater".to_string(),
        },
        Transaction {
            transaction_id: "T7".to_string(),
            user_id: "alice".to_string(),
            amount: 3000.0,
            category: "luxury".to_string(),
            timestamp: 1220,
            merchant: "Luxury Store".to_string(),
        },
        Transaction {
            transaction_id: "T8".to_string(),
            user_id: "bob".to_string(),
            amount: 150.0,
            category: "electronics".to_string(),
            timestamp: 1240,
            merchant: "Electronics Store".to_string(),
        },
    ];

    // Example 1: Filter transactions based on user spending patterns
    println!("1. Filtering Transactions Based on User Spending Patterns:");
    let stream = futures::stream::iter(transactions.clone());
    let filtered_stream = stream.stateful_filter_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|transaction: &Transaction| transaction.user_id.clone()),
        |transaction, state_access| {
            let transaction = transaction.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut profile: UserRiskProfile = if state_bytes.is_empty() {
                    UserRiskProfile {
                        total_transactions: 0,
                        total_amount: 0.0,
                        average_amount: 0.0,
                        category_counts: HashMap::new(),
                        last_transaction_time: 0,
                        risk_score: 0.0,
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(UserRiskProfile {
                        total_transactions: 0,
                        total_amount: 0.0,
                        average_amount: 0.0,
                        category_counts: HashMap::new(),
                        last_transaction_time: 0,
                        risk_score: 0.0,
                    })
                };

                // Update profile
                profile.total_transactions += 1;
                profile.total_amount += transaction.amount;
                profile.average_amount = profile.total_amount / profile.total_transactions as f64;
                profile.last_transaction_time = transaction.timestamp;

                // Update category counts
                *profile
                    .category_counts
                    .entry(transaction.category.clone())
                    .or_insert(0) += 1;

                // Calculate risk score based on spending patterns
                let luxury_count = profile.category_counts.get("luxury").unwrap_or(&0);
                let luxury_ratio = *luxury_count as f64 / profile.total_transactions as f64;
                let amount_ratio = transaction.amount / profile.average_amount.max(1.0);

                profile.risk_score = luxury_ratio * 0.4 + (amount_ratio - 1.0).max(0.0) * 0.6;

                // Save updated profile
                let state_bytes = serde_json::to_vec(&profile).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Filter: Allow transactions if risk score is below threshold or amount is reasonable
                let should_allow =
                    profile.risk_score < 0.7 || transaction.amount < profile.average_amount * 2.0;

                if should_allow {
                    println!(
                        "  ALLOWED: User: {} | Amount: ${:.2} | Category: {} | Risk Score: {:.2}",
                        transaction.user_id,
                        transaction.amount,
                        transaction.category,
                        profile.risk_score
                    );
                } else {
                    println!(
                        "  BLOCKED: User: {} | Amount: ${:.2} | Category: {} | Risk Score: {:.2}",
                        transaction.user_id,
                        transaction.amount,
                        transaction.category,
                        profile.risk_score
                    );
                }

                Ok(should_allow)
            })
        },
    );

    let filtered_results: Vec<Transaction> = filtered_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    println!("  Total transactions processed: {}", transactions.len());
    println!("  Transactions allowed: {}", filtered_results.len());

    // Example 2: Filter based on frequency limits
    println!("\n2. Filtering Based on Frequency Limits:");
    let stream = futures::stream::iter(transactions.clone());
    let frequency_filtered_stream = stream.stateful_filter_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|transaction: &Transaction| transaction.user_id.clone()),
        |transaction, state_access| {
            let transaction = transaction.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut frequency_data: HashMap<String, u64> = if state_bytes.is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                };

                // Get current frequency info
                let current_count = frequency_data.get("count").unwrap_or(&0);
                let window_start = frequency_data.get("window_start").unwrap_or(&transaction.timestamp);

                // Reset window if more than 300 seconds (5 minutes) have passed
                let window_duration = 300;
                let new_count = if transaction.timestamp - window_start > window_duration {
                    1
                } else {
                    current_count + 1
                };

                // Update frequency data
                frequency_data.insert("count".to_string(), new_count);
                if new_count == 1 {
                    frequency_data.insert("window_start".to_string(), transaction.timestamp);
                }

                // Save frequency data
                let state_bytes = serde_json::to_vec(&frequency_data).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Filter: Allow max 3 transactions per 5 minutes
                let should_allow = new_count <= 3;

                if should_allow {
                    println!("  ALLOWED: User: {} | Transaction: {} | Frequency: {}/{} per 5min",
                        transaction.user_id, transaction.transaction_id, new_count, 3);
                } else {
                    println!("  BLOCKED: User: {} | Transaction: {} | Frequency: {}/{} per 5min (RATE LIMITED)",
                        transaction.user_id, transaction.transaction_id, new_count, 3);
                }

                Ok(should_allow)
            })
        },
    );

    let frequency_results: Vec<Transaction> = frequency_filtered_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    println!(
        "  Transactions allowed after frequency filtering: {}",
        frequency_results.len()
    );

    // Example 3: Filter based on category spending limits
    println!("\n3. Filtering Based on Category Spending Limits:");
    let stream = futures::stream::iter(transactions.clone());
    let category_filtered_stream = stream.stateful_filter_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|transaction: &Transaction| transaction.user_id.clone()),
        |transaction, state_access| {
            let transaction = transaction.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut category_spending: HashMap<String, f64> = if state_bytes.is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                };

                // Get current spending for this category
                let current_spending = category_spending.get(&transaction.category).unwrap_or(&0.0);
                let new_spending = current_spending + transaction.amount;

                // Update category spending
                category_spending.insert(transaction.category.clone(), new_spending);

                // Save category spending data
                let state_bytes = serde_json::to_vec(&category_spending).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Define category limits
                let category_limits: HashMap<&str, f64> = [
                    ("luxury", 2000.0),
                    ("electronics", 1000.0),
                    ("food", 500.0),
                    ("entertainment", 300.0),
                ].iter().cloned().collect();

                let limit = category_limits.get(transaction.category.as_str()).unwrap_or(&1000.0);
                let should_allow = new_spending <= *limit;

                if should_allow {
                    println!("  ALLOWED: User: {} | Category: {} | Amount: ${:.2} | Total: ${:.2}/${:.2}",
                        transaction.user_id, transaction.category, transaction.amount, new_spending, limit);
                } else {
                    println!("  BLOCKED: User: {} | Category: {} | Amount: ${:.2} | Total: ${:.2}/{:.2} (LIMIT EXCEEDED)",
                        transaction.user_id, transaction.category, transaction.amount, new_spending, limit);
                }

                Ok(should_allow)
            })
        },
    );

    let category_results: Vec<Transaction> = category_filtered_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    println!(
        "  Transactions allowed after category filtering: {}",
        category_results.len()
    );

    // Example 4: Filter based on time-based patterns
    println!("\n4. Filtering Based on Time-Based Patterns:");
    let stream = futures::stream::iter(transactions.clone());
    let time_filtered_stream = stream.stateful_filter_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|transaction: &Transaction| transaction.user_id.clone()),
        |transaction, state_access| {
            let transaction = transaction.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut time_patterns: HashMap<String, u64> = if state_bytes.is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                };

                // Get last transaction time
                let last_time_val = *time_patterns.get("last_transaction").unwrap_or(&0);
                let time_gap = if last_time_val > 0 {
                    transaction.timestamp - last_time_val
                } else {
                    0
                };

                // Update time patterns
                time_patterns.insert("last_transaction".to_string(), transaction.timestamp);

                // Save time patterns
                let state_bytes = serde_json::to_vec(&time_patterns).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Filter: Block transactions that are too close together (potential fraud)
                let min_gap = 30; // 30 seconds minimum between transactions
                let should_allow = time_gap >= min_gap || last_time_val == 0;

                if should_allow {
                    println!("  ALLOWED: User: {} | Transaction: {} | Time gap: {}s (min: {}s)",
                        transaction.user_id, transaction.transaction_id, time_gap, min_gap);
                } else {
                    println!("  BLOCKED: User: {} | Transaction: {} | Time gap: {}s (min: {}s) - SUSPICIOUS TIMING",
                        transaction.user_id, transaction.transaction_id, time_gap, min_gap);
                }

                Ok(should_allow)
            })
        },
    );

    let time_results: Vec<Transaction> = time_filtered_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    println!(
        "  Transactions allowed after time-based filtering: {}",
        time_results.len()
    );

    println!("\n=== Stateful Filter Example Complete ===");
}
