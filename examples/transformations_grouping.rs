use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use tokio::runtime::Runtime;

// Define our User type for the example
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User {
                id: 1,
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                active: true,
                role: "admin".to_string(),
            },
            User {
                id: 2,
                name: "Bob".to_string(),
                email: "bob@example.com".to_string(),
                active: true,
                role: "user".to_string(),
            },
            User {
                id: 3,
                name: "Charlie".to_string(),
                email: "charlie@example.com".to_string(),
                active: false,
                role: "user".to_string(),
            },
            User {
                id: 4,
                name: "Diana".to_string(),
                email: "diana@example.com".to_string(),
                active: true,
                role: "moderator".to_string(),
            },
            User {
                id: 5,
                name: "Eve".to_string(),
                email: "eve@example.com".to_string(),
                active: true,
                role: "user".to_string(),
            },
        ];

        // Group users by role
        let users_by_role = from_iter(users.clone())
            .group_by_rs2(|user| user.role.clone())
            .collect::<Vec<_>>()
            .await;

        for (role, role_users) in users_by_role {
            println!("Role: {}, Count: {}", role, role_users.len());
            for user in role_users {
                println!("  - {}", user.name);
            }
        }

        // Create a stream of status updates
        let status_updates = vec![
            "online", "online", "online", "away", "away", "online", "online", "offline",
        ];

        // Group adjacent identical status updates
        let grouped_statuses = from_iter(status_updates)
            .group_adjacent_by_rs2(|&status| status)
            .collect::<Vec<_>>()
            .await;

        for (status, occurrences) in grouped_statuses {
            println!(
                "Status '{}' occurred {} consecutive times",
                status,
                occurrences.len()
            );
        }

        // Filter out consecutive duplicate status updates
        let unique_statuses = from_iter(vec![
            "online", "online", "away", "away", "online", "offline",
        ])
        .distinct_until_changed_rs2()
        .collect::<Vec<_>>()
        .await;

        println!("Unique status transitions: {:?}", unique_statuses); // ["online", "away", "online", "offline"]

        // Use custom equality function to detect significant changes
        #[derive(Clone)]
        struct ServerMetrics {
            cpu: f64,
            memory: f64,
            connections: usize,
        }

        let metrics = vec![
            ServerMetrics {
                cpu: 10.5,
                memory: 45.0,
                connections: 100,
            },
            ServerMetrics {
                cpu: 11.0,
                memory: 46.0,
                connections: 102,
            }, // Small change
            ServerMetrics {
                cpu: 50.0,
                memory: 80.0,
                connections: 150,
            }, // Big change
            ServerMetrics {
                cpu: 51.0,
                memory: 81.0,
                connections: 155,
            }, // Small change
            ServerMetrics {
                cpu: 20.0,
                memory: 40.0,
                connections: 90,
            }, // Big change
        ];

        // Only emit metrics when there's a significant change
        let significant_changes = from_iter(metrics)
            .distinct_until_changed_by_rs2(|prev, curr| {
                // Consider it the same if CPU and memory changes are less than 20%
                (curr.cpu - prev.cpu).abs() < 20.0 && (curr.memory - prev.memory).abs() < 20.0
            })
            .collect::<Vec<_>>()
            .await;

        println!(
            "Number of significant metric changes: {}",
            significant_changes.len()
        ); // 3
    });
}
