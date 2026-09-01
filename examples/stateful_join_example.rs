//! Joining two streams on a key within a time window.
//!
//! `stateful_join_rs2` buffers each side per key and emits a joined result
//! whenever an arrival on one side matches a buffered item on the other, as
//! long as they fall inside `window_duration`.
//!
//! Run with: `cargo run --example stateful_join_example`

use futures_util::stream::StreamExt;
use rs2_stream::state::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    customer: String,
    total: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payment {
    order_id: String,
    amount: f64,
    method: String,
}

#[tokio::main]
async fn main() {
    let orders = vec![
        Order { order_id: "A1".into(), customer: "alice".into(), total: 25.0 },
        Order { order_id: "B2".into(), customer: "bob".into(), total: 40.0 },
        Order { order_id: "C3".into(), customer: "carol".into(), total: 15.0 },
    ];

    // Note C3 has no payment: an unmatched item simply never produces a row.
    let payments = vec![
        Payment { order_id: "A1".into(), amount: 25.0, method: "card".into() },
        Payment { order_id: "B2".into(), amount: 40.0, method: "paypal".into() },
    ];

    println!("=== joining orders to payments on order_id ===");

    let joined = futures_util::stream::iter(orders)
        .stateful_join_rs2(
            Box::pin(futures_util::stream::iter(payments)),
            StateConfig::default(),
            CustomKeyExtractor::new(|o: &Order| o.order_id.clone()),
            CustomKeyExtractor::new(|p: &Payment| p.order_id.clone()),
            Duration::from_secs(300),
            |order, payment, _state| {
                Box::pin(async move {
                    let settled = (order.total - payment.amount).abs() < f64::EPSILON;
                    Ok(format!(
                        "{} {} paid {:.2} by {} [{}]",
                        order.order_id,
                        order.customer,
                        payment.amount,
                        payment.method,
                        if settled { "settled" } else { "MISMATCH" }
                    ))
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    // A1 and B2 have matching payments; C3 has none, so it produces no row.
    assert_eq!(joined.iter().filter(|r| r.is_ok()).count(), 2);

    for row in joined.iter().filter_map(|r| r.as_ref().ok()) {
        println!("  {}", row);
    }

    println!("\n  C3 produced no row: a join only emits when both sides arrive");
    println!("  within the window. Widen `window_duration` if late arrivals");
    println!("  should still match; items outside it are dropped from the buffers.");
}
