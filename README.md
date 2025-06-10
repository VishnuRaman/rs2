# RS2: Rust Streaming Library

RS2 is a powerful, functional streaming library for Rust inspired by FS2 (Functional Streams for Scala). It provides a comprehensive set of tools for working with asynchronous streams in a functional programming style, with built-in support for backpressure handling, resource management, and error handling.

## Features

- **Functional API**: Chain operations together in a fluent, functional style
- **Backpressure Handling**: Built-in support for handling backpressure with configurable strategies
- **Resource Management**: Safe resource acquisition and release with bracket patterns
- **Error Handling**: Comprehensive error handling with retry policies
- **Parallel Processing**: Process stream elements in parallel with bounded concurrency
- **Time-based Operations**: Throttling, debouncing, sampling, and timeouts
- **Transformations**: Rich set of stream transformation operations

## Installation

Add RS2 to your `Cargo.toml`:

```toml
[dependencies]
rs2 = "0.1.0"
```

## Basic Usage

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream from an iterator
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply transformations
        let result = stream
            .filter_rs2(|&x| x % 2 == 0)  // Keep only even numbers
            .map_rs2(|x| x * 2)           // Double each number
            .collect::<Vec<_>>()          // Collect into a Vec
            .await;

        println!("Result: {:?}", result);  // Output: Result: [4, 8]
    });
}
```

## Real-World Example: Processing a Stream of Users

Let's build a more complex example that processes a stream of users, demonstrating several RS2 features:

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::error::Error;

// Define our User type
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
}

// Simulate a database query that returns users
async fn fetch_users() -> Vec<User> {
    // In a real application, this would query a database
    vec![
        User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
        User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
        User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string() },
        User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
    ]
}

// Simulate sending an email to a user
async fn send_email(user: &User, message: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Sending email to {}: {}", user.email, message);
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

// Simulate updating a user in the database
async fn update_user(user: User) -> Result<User, Box<dyn Error + Send + Sync>> {
    println!("Updating user: {}", user.name);
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(user)
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = eval(fetch_users()).flat_map(|users| from_iter(users));

        // Apply backpressure to avoid overwhelming downstream systems
        let users_with_backpressure = users.auto_backpressure_rs2();

        // Process active users only
        let active_users = users_with_backpressure
            .filter_rs2(|user| user.active)
            .prefetch_rs2(2);  // Prefetch to improve performance

        // Group users by role
        let users_by_role = active_users
            .group_by_rs2(|user| user.role.clone())
            .collect::<Vec<_>>()
            .await;

        // Print users by role
        for (role, users) in users_by_role {
            println!("Role: {}, Count: {}", role, users.len());
            for user in users {
                println!("  - {} ({})", user.name, user.email);
            }
        }

        // Process users in parallel with bounded concurrency
        let processed_users = active_users
            .par_eval_map_rs2(3, |mut user| async move {
                // Simulate some processing
                user.name = format!("{} (Processed)", user.name);

                // Update the user in the database
                match update_user(user.clone()).await {
                    Ok(updated_user) => updated_user,
                    Err(_) => user,
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("\nProcessed {} users", processed_users.len());

        // Send emails to users with timeout
        let email_results = active_users
            .eval_map_rs2(|user| async move {
                // Add timeout to email sending
                match tokio::time::timeout(
                    Duration::from_millis(200),
                    send_email(&user, "Welcome to our platform!")
                ).await {
                    Ok(Ok(_)) => (user.id, true),
                    _ => (user.id, false),
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("\nEmail results:");
        for (user_id, success) in email_results {
            println!("User {}: {}", user_id, if success { "Email sent" } else { "Failed to send email" });
        }
    });
}
```

This example demonstrates:
- Creating a stream of users
- Applying backpressure to avoid overwhelming downstream systems
- Filtering for active users only
- Grouping users by role
- Processing users in parallel with bounded concurrency
- Adding timeouts to operations
- Collecting results

## API Overview

### Stream Creation

- `emit(item)` - Create a stream that emits a single element
- `empty()` - Create an empty stream
- `from_iter(iter)` - Create a stream from an iterator
- `eval(future)` - Evaluate a Future and emit its output
- `repeat(item)` - Create a stream that repeats a value
- `emit_after(item, duration)` - Create a stream that emits a value after a delay
- `unfold(init, f)` - Create a stream by repeatedly applying a function

#### Examples

##### Stream Creation with `emit`, `empty`, and `from_iter`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream with a single user
        let user = User {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            active: true,
            role: "admin".to_string(),
        };

        let single_user_stream = emit(user.clone());
        let first_user = single_user_stream.collect::<Vec<_>>().await;
        println!("Single user: {:?}", first_user[0].name);

        // Create an empty stream
        let empty_stream: RS2Stream<User> = empty();
        let empty_result = empty_stream.collect::<Vec<_>>().await;
        println!("Empty stream length: {}", empty_result.len()); // 0

        // Create a stream from an iterator
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        let users_stream = from_iter(users);
        let all_users = users_stream.collect::<Vec<_>>().await;
        println!("All users: {}", all_users.len()); // 3
    });
}
```

##### Async Stream Creation with `eval` and `emit_after`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::{Duration, Instant};

// Simulate fetching a user from a database
async fn fetch_user(id: u64) -> User {
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(50)).await;

    User {
        id,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
        role: "admin".to_string(),
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream by evaluating a future
        let user_stream = eval(fetch_user(1));
        let user = user_stream.collect::<Vec<_>>().await;
        println!("Fetched user: {}", user[0].name);

        // Create a stream that emits a value after a delay
        let start = Instant::now();
        let delayed_user = emit_after(
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            Duration::from_millis(100)
        );

        let user = delayed_user.collect::<Vec<_>>().await;
        let elapsed = start.elapsed();

        println!("Delayed user: {} (after {}ms)", user[0].name, elapsed.as_millis());
    });
}
```

##### Creating Infinite Streams with `repeat` and `unfold`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream that repeats a notification
        let notification = "New message!";
        let notification_stream = repeat(notification).take(3); // Limit to 3 notifications

        let notifications = notification_stream.collect::<Vec<_>>().await;
        println!("Notifications: {:?}", notifications);

        // Create a stream of user IDs using unfold
        let user_id_stream = unfold(
            1, // Start with user ID 1
            |id| async move {
                if id <= 5 {
                    // Generate the next user ID
                    Some((id, id + 1))
                } else {
                    None // End the stream after user ID 5
                }
            }
        );

        let user_ids = user_id_stream.collect::<Vec<_>>().await;
        println!("User IDs: {:?}", user_ids); // [1, 2, 3, 4, 5]
    });
}
```

### Stream Transformation

- `map_rs2(f)` - Map elements with a function
- `filter_rs2(predicate)` - Filter elements with a predicate
- `flat_map_rs2(f)` - Flat map elements with a function that returns a stream
- `eval_map_rs2(f)` - Map elements with an async function
- `take_rs2(n)` - Take the first n elements
- `drop_rs2(n)` / `skip_rs2(n)` - Skip the first n elements
- `take_while_rs2(predicate)` - Take elements while a predicate returns true
- `drop_while_rs2(predicate)` - Skip elements while a predicate returns true
- `zip_rs2(other)` - Zip with another stream
- `zip_with_rs2(other, f)` - Zip with another stream, applying a function to each pair
- `merge_rs2(other)` - Merge with another stream
- `either_rs2(other)` - Select between streams based on which produces a value first
- `group_by_rs2(key_fn)` - Group elements by a key function
- `group_adjacent_by_rs2(key_fn)` - Group consecutive elements by a key function
- `distinct_until_changed_rs2()` - Filter out consecutive duplicate elements
- `distinct_until_changed_by_rs2(eq)` - Filter out consecutive duplicate elements using a custom equality function

#### Examples

##### Basic Transformations with `map_rs2`, `filter_rs2`, and `flat_map_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        let users_stream = from_iter(users);

        // Map: Transform each user to just their name
        let names_stream = users_stream
            .map_rs2(|user| user.name);

        let names = names_stream.collect::<Vec<_>>().await;
        println!("User names: {:?}", names); // ["Alice", "Bob", "Charlie"]

        // Create a new stream for filtering
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        // Filter: Keep only active users
        let active_users_stream = from_iter(users)
            .filter_rs2(|user| user.active);

        let active_users = active_users_stream.collect::<Vec<_>>().await;
        println!("Active users: {}", active_users.len()); // 2

        // Create a new stream for flat_map
        let departments = vec![
            ("Engineering", vec!["Alice", "Bob"]),
            ("Marketing", vec!["Charlie", "Diana"]),
        ];

        // Flat map: Convert departments to individual employees
        let employees_stream = from_iter(departments)
            .flat_map_rs2(|(dept, employees)| {
                // Create a stream of employees with their department
                from_iter(employees.into_iter().map(move |name| (name, dept)))
            });

        let employees = employees_stream.collect::<Vec<_>>().await;
        println!("Employees: {:?}", employees); 
        // [("Alice", "Engineering"), ("Bob", "Engineering"), ("Charlie", "Marketing"), ("Diana", "Marketing")]
    });
}
```

##### Async Transformations with `eval_map_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;

// Simulate fetching user details from a database
async fn fetch_user_details(id: u64) -> User {
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(50)).await;

    User {
        id,
        name: format!("User {}", id),
        email: format!("user{}@example.com", id),
        active: true,
        role: "user".to_string(),
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of user IDs
        let user_ids = from_iter(vec![1, 2, 3]);

        // Use eval_map_rs2 to asynchronously fetch user details for each ID
        let users_stream = user_ids
            .eval_map_rs2(|id| async move {
                fetch_user_details(id).await
            });

        let users = users_stream.collect::<Vec<_>>().await;

        for user in users {
            println!("Fetched user: {} ({})", user.name, user.email);
        }
    });
}
```

##### Slicing Streams with `take_rs2`, `drop_rs2`, `take_while_rs2`, and `drop_while_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string() },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
        ];

        // Take the first 2 users
        let first_two_users = from_iter(users.clone())
            .take_rs2(2)
            .collect::<Vec<_>>()
            .await;

        println!("First two users: {} and {}", first_two_users[0].name, first_two_users[1].name);

        // Skip the first 3 users
        let last_two_users = from_iter(users.clone())
            .drop_rs2(3) // or .skip_rs2(3)
            .collect::<Vec<_>>()
            .await;

        println!("Last two users: {} and {}", last_two_users[0].name, last_two_users[1].name);

        // Take users while they are active
        let initial_active_users = from_iter(users.clone())
            .take_while_rs2(|user| async move { user.active })
            .collect::<Vec<_>>()
            .await;

        println!("Initial active users: {}", initial_active_users.len()); // 2 (stops at Charlie who is inactive)

        // Skip users while they have specific roles
        let non_standard_users = from_iter(users.clone())
            .drop_while_rs2(|user| async move { 
                user.role == "admin" || user.role == "user" 
            })
            .collect::<Vec<_>>()
            .await;

        println!("Non-standard role users: {}", non_standard_users.len()); // 3 (Diana, Eve)
    });
}
```

##### Combining Streams with `zip_rs2`, `zip_with_rs2`, `merge_rs2`, and `either_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create two streams of users
        let admins = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "admin".to_string() },
        ];

        let regular_users = vec![
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
        ];

        // Zip the streams to pair admins with users they manage
        let admin_user_pairs = from_iter(admins.clone())
            .zip_rs2(from_iter(regular_users.clone()))
            .collect::<Vec<_>>()
            .await;

        for (admin, user) in admin_user_pairs {
            println!("Admin {} manages user {}", admin.name, user.name);
        }

        // Use zip_with to create management assignments
        let assignments = from_iter(admins.clone())
            .zip_with_rs2(from_iter(regular_users.clone()), |admin, user| {
                format!("{} is responsible for {}'s onboarding", admin.name, user.name)
            })
            .collect::<Vec<_>>()
            .await;

        for assignment in assignments {
            println!("{}", assignment);
        }

        // Merge streams to get all users in a single stream
        let all_users = from_iter(admins.clone())
            .merge_rs2(from_iter(regular_users.clone()))
            .collect::<Vec<_>>()
            .await;

        println!("Total users after merge: {}", all_users.len()); // 5

        // Create two streams with different timing
        let fast_stream = stream! {
            yield "Fast response";
            tokio::time::sleep(Duration::from_millis(50)).await;
            yield "Fast again";
        }.boxed();

        let slow_stream = stream! {
            tokio::time::sleep(Duration::from_millis(20)).await;
            yield "Slow response";
            tokio::time::sleep(Duration::from_millis(100)).await;
            yield "Slow again";
        }.boxed();

        // Use either to select whichever stream produces a value first
        let results = fast_stream
            .either_rs2(slow_stream)
            .collect::<Vec<_>>()
            .await;

        println!("Results from either: {:?}", results);
        // Should contain "Fast response", "Slow response", "Fast again", "Slow again"
    });
}
```

##### Grouping and Deduplication with `group_by_rs2`, `group_adjacent_by_rs2`, and `distinct_until_changed_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string() },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
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
            "online", "online", "online", "away", "away", "online", "online", "offline"
        ];

        // Group adjacent identical status updates
        let grouped_statuses = from_iter(status_updates)
            .group_adjacent_by_rs2(|&status| status)
            .collect::<Vec<_>>()
            .await;

        for (status, occurrences) in grouped_statuses {
            println!("Status '{}' occurred {} consecutive times", status, occurrences.len());
        }

        // Filter out consecutive duplicate status updates
        let unique_statuses = from_iter(vec!["online", "online", "away", "away", "online", "offline"])
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
            ServerMetrics { cpu: 10.5, memory: 45.0, connections: 100 },
            ServerMetrics { cpu: 11.0, memory: 46.0, connections: 102 }, // Small change
            ServerMetrics { cpu: 50.0, memory: 80.0, connections: 150 }, // Big change
            ServerMetrics { cpu: 51.0, memory: 81.0, connections: 155 }, // Small change
            ServerMetrics { cpu: 20.0, memory: 40.0, connections: 90 },  // Big change
        ];

        // Only emit metrics when there's a significant change
        let significant_changes = from_iter(metrics)
            .distinct_until_changed_by_rs2(|prev, curr| {
                // Consider it the same if CPU and memory changes are less than 20%
                (curr.cpu - prev.cpu).abs() < 20.0 && 
                (curr.memory - prev.memory).abs() < 20.0
            })
            .collect::<Vec<_>>()
            .await;

        println!("Number of significant metric changes: {}", significant_changes.len()); // 3
    });
}

### Accumulation

- `fold_rs2(init, f)` - Accumulate a value over a stream
- `scan_rs2(init, f)` - Apply a function to each element and emit intermediate accumulated values
- `for_each_rs2(f)` - Apply a function to each element without accumulating a result
- `collect_rs2::<B>()` - Collect all items into a collection

#### Examples

##### Accumulating Values with `fold_rs2` and `scan_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of user activity events
        let user_activities = vec![
            ("Alice", 120), // User and time spent in seconds
            ("Bob", 45),
            ("Alice", 60),
            ("Charlie", 90),
            ("Bob", 30),
            ("Alice", 75),
        ];

        // Use fold_rs2 to calculate total time spent by each user
        let total_time_by_user = from_iter(user_activities.clone())
            .fold_rs2(
                std::collections::HashMap::new(),
                |mut acc, (user, time)| async move {
                    *acc.entry(user).or_insert(0) += time;
                    acc
                }
            )
            .await;

        println!("Total time spent by each user:");
        for (user, time) in total_time_by_user {
            println!("  - {}: {} seconds", user, time);
        }

        // Use scan_rs2 to calculate running total of time spent
        let running_total = from_iter(user_activities)
            .scan_rs2(0, |acc, (user, time)| {
                println!("Processing activity: {} spent {} seconds", user, time);
                acc + time
            })
            .collect::<Vec<_>>()
            .await;

        println!("Running total of time spent:");
        for (i, total) in running_total.iter().enumerate() {
            println!("  After activity {}: {} seconds", i + 1, total);
        }
    });
}
```

##### Processing Elements with `for_each_rs2` and `collect_rs2`

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::collections::{HashSet, BTreeMap};
use std::sync::Arc;
use tokio::sync::Mutex;

// Simulate sending a notification
async fn send_notification(user: &str, message: &str) {
    println!("Sending to {}: {}", user, message);
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users to notify
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        // Use for_each_rs2 to send notifications to each user
        let notification_tracker = Arc::new(Mutex::new(Vec::new()));
        let tracker_clone = notification_tracker.clone();

        from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .for_each_rs2(move |user| {
                let tracker = tracker_clone.clone();
                async move {
                    // Send notification
                    send_notification(&user.name, "Your account has been updated").await;

                    // Track that we sent the notification
                    let mut guard = tracker.lock().await;
                    guard.push(user.id);
                }
            })
            .await;

        let notified_users = notification_tracker.lock().await;
        println!("Notified {} users", notified_users.len());

        // Use collect_rs2 to gather users into different collections

        // Collect into a HashSet (removes duplicates)
        let unique_roles = from_iter(users.clone())
            .map_rs2(|user| user.role)
            .collect_rs2::<HashSet<_>>()
            .await;

        println!("Unique roles: {:?}", unique_roles);

        // Collect into a BTreeMap (sorted by key)
        let users_by_id = from_iter(users)
            .collect_rs2::<BTreeMap<_, _>>()
            .await;

        println!("Users by ID:");
        for (id, user) in users_by_id {
            println!("  {}: {}", id, user.name);
        }
    });
}
```

### Parallel Processing

- `par_eval_map_rs2(concurrency, f)` - Process elements in parallel with bounded concurrency, preserving order
- `par_eval_map_unordered_rs2(concurrency, f)` - Process elements in parallel without preserving order
- `par_join_rs2(concurrency)` - Run multiple streams concurrently and combine their outputs

### Time-based Operations

- `throttle_rs2(duration)` - Emit at most one element per duration
- `debounce_rs2(duration)` - Emit an element after a quiet period
- `sample_rs2(interval)` - Sample at regular intervals
- `timeout_rs2(duration)` - Add timeout to operations

### Backpressure and Resource Management

- `auto_backpressure_rs2()` - Apply automatic backpressure with default configuration
- `auto_backpressure_with_rs2(config)` - Apply automatic backpressure with custom configuration
- `prefetch_rs2(count)` - Prefetch elements ahead of consumption
- `interrupt_when_rs2(signal)` - Interrupt the stream when a signal is received

### Error Handling

- `recover_rs2(f)` - Recover from errors with a function
- `on_error_resume_next_rs2(f)` - Resume with a new stream on error
- `retry_rs2(max_retries, f)` - Retry on error
- `map_error(f)` - Map errors to a different type
- `or_else(f)` - Replace errors with fallback values
- `collect_ok()` - Collect only successful values
- `collect_err()` - Collect only errors
- `retry_with_policy(policy, f)` - Retry with a specific policy

## Advanced Usage Patterns

### Resource Management with Bracket

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::fs::File;
use std::io::{self, BufReader, BufRead};

// Acquire a resource
async fn acquire_resource() -> io::Result<BufReader<File>> {
    let file = File::open("data.txt")?;
    Ok(BufReader::new(file))
}

// Release a resource
async fn release_resource(_reader: BufReader<File>) {
    println!("Resource released");
}

// Use a resource
fn use_resource(reader: BufReader<File>) -> RS2Stream<String> {
    let lines = reader.lines().filter_map(Result::ok);
    from_iter(lines)
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Use bracket to ensure resource is released
        let result = bracket(
            acquire_resource(),
            |reader| use_resource(reader),
            release_resource
        )
        .collect::<Vec<_>>()
        .await;

        println!("Lines: {:?}", result);
    });
}
```

### Custom Backpressure Strategy

```rust
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream that produces elements faster than they can be consumed
        let fast_stream = repeat(1).take(1000);

        // Configure custom backpressure
        let config = BackpressureConfig {
            strategy: BackpressureStrategy::DropNewest,
            buffer_size: 10,
            low_watermark: Some(3),
            high_watermark: Some(8),
        };

        // Apply custom backpressure
        let controlled_stream = fast_stream.auto_backpressure_with_rs2(config);

        // Process elements with a delay to simulate slow consumption
        let result = controlled_stream
            .eval_map_rs2(|x| async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                x
            })
            .collect::<Vec<_>>()
            .await;

        println!("Processed {} elements with backpressure", result.len());
    });
}
```
## Pipe: Stream Transformation Functions

A Pipe represents a stream transformation from one type to another. It's a function from Stream[I] to Stream[O] that can be composed with other pipes to create complex stream processing pipelines.

### Pipe Methods

- `Pipe::new(f)` - Create a new pipe from a function
- `apply(input)` - Apply this pipe to a stream
- `compose(other)` - Compose this pipe with another pipe

### Utility Functions

- `map(f)` - Create a pipe that applies the given function to each element
- `filter(predicate)` - Create a pipe that filters elements based on the predicate
- `compose(p1, p2)` - Compose two pipes together
- `identity()` - Identity pipe that doesn't transform the stream

### Examples

#### Basic Pipe Usage

```rust
use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Create a pipe that doubles each number
        let double_pipe = map(|x: i32| x * 2);

        // Apply the pipe to the stream
        let doubled = double_pipe.apply(numbers);

        // Collect the results
        let result = doubled.collect::<Vec<_>>().await;
        println!("Doubled: {:?}", result); // [2, 4, 6, 8, 10]
    });
}
```

#### Composing Pipes

```rust
use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6]);

        // Create pipes for different transformations
        let even_filter = filter(|&x: &i32| x % 2 == 0);
        let double_map = map(|x: i32| x * 2);

        // Compose the pipes using the compose function
        let even_then_double = compose(even_filter, double_map);

        // Apply the composed pipe to the stream
        let result_stream = even_then_double.apply(numbers);

        // Collect the results
        let result = result_stream.collect::<Vec<_>>().await;
        println!("Even numbers doubled: {:?}", result); // [4, 8, 12]

        // Alternatively, use the compose method on the Pipe
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6]);
        let even_filter = filter(|&x: &i32| x % 2 == 0);
        let double_map = map(|x: i32| x * 2);

        // Use the compose method
        let even_then_double = even_filter.compose(double_map);

        // Apply the composed pipe to the stream
        let result_stream = even_then_double.apply(numbers);

        // Collect the results
        let result = result_stream.collect::<Vec<_>>().await;
        println!("Even numbers doubled (using compose method): {:?}", result); // [4, 8, 12]
    });
}
```

#### Real-World Example: User Data Processing Pipeline

```rust
use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;

// Define our User type
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
    login_count: u32,
}

// Define a UserStats type
#[derive(Debug, Clone, PartialEq)]
struct UserStats {
    id: u64,
    name: String,
    role: String,
    is_active: bool,
    login_frequency: &'static str,
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string(), login_count: 120 },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string(), login_count: 45 },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string(), login_count: 5 },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string(), login_count: 80 },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string(), login_count: 30 },
        ];

        // Create pipes for different transformations

        // 1. Filter active users
        let active_filter = filter(|user: &User| user.active);

        // 2. Transform User to UserStats
        let stats_transform = map(|user: User| {
            let frequency = match user.login_count {
                0..=10 => "Low",
                11..=50 => "Medium",
                _ => "High",
            };

            UserStats {
                id: user.id,
                name: user.name,
                role: user.role,
                is_active: user.active,
                login_frequency: frequency,
            }
        });

        // 3. Compose the pipes
        let process_users = active_filter.compose(stats_transform);

        // Apply the processing pipeline to the stream
        let user_stream = from_iter(users);
        let stats_stream = process_users.apply(user_stream);

        // Collect the results
        let user_stats = stats_stream.collect::<Vec<_>>().await;

        // Print the results
        println!("User Statistics:");
        for stats in user_stats {
            println!("  - {} ({}): {} usage", stats.name, stats.role, stats.login_frequency);
        }

        // Group users by login frequency using another pipe
        let group_by_frequency = map(|stats: UserStats| {
            (stats.login_frequency, stats)
        });

        // Apply the grouping pipe to the stats stream
        let user_stream = from_iter(users);
        let stats_stream = process_users.apply(user_stream);
        let grouped_stream = group_by_frequency.apply(stats_stream);

        // Collect and organize by frequency
        let mut frequency_groups: HashMap<&str, Vec<UserStats>> = HashMap::new();
        let grouped_stats = grouped_stream.collect::<Vec<_>>().await;

        for (frequency, stats) in grouped_stats {
            frequency_groups.entry(frequency).or_insert_with(Vec::new).push(stats);
        }

        // Print the grouped results
        println!("\nUsers by Login Frequency:");
        for (frequency, users) in &frequency_groups {
            println!("  {} Usage ({} users):", frequency, users.len());
            for user in users {
                println!("    - {} ({})", user.name, user.role);
            }
        }
    });
}
```

## Queue: Concurrent Queue with Stream Interface

A Queue represents a concurrent queue with a Stream interface for dequeuing and async methods for enqueuing. It supports both bounded and unbounded queues.

### Queue Types

- `Queue::bounded(capacity)` - Create a new bounded queue with the given capacity
- `Queue::unbounded()` - Create a new unbounded queue

### Queue Methods

- `enqueue(item)` - Enqueue an item into the queue
- `try_enqueue(item)` - Try to enqueue an item without blocking
- `dequeue()` - Get a stream for dequeuing items
- `close()` - Close the queue, preventing further enqueues
- `capacity()` - Get the capacity of the queue (None for unbounded)
- `is_empty()` - Check if the queue is empty
- `len()` - Get the current number of items in the queue

### Examples

#### Basic Queue Usage

```rust
use rs2::queue::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a bounded queue with capacity 5
        let queue = Queue::bounded(5);

        // Enqueue some items
        for i in 1..=3 {
            queue.enqueue(i).await.unwrap();
            println!("Enqueued: {}", i);
        }

        // Get the current queue length
        let len = queue.len().await;
        println!("Queue length: {}", len); // 3

        // Get a stream for dequeuing
        let mut dequeue_stream = queue.dequeue();

        // Dequeue and process items
        while let Some(item) = dequeue_stream.next().await {
            println!("Dequeued: {}", item);
        }
    });
}
```

#### Producer-Consumer Pattern

```rust
use rs2::queue::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use std::sync::Arc;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a shared queue
        let queue = Arc::new(Queue::bounded(10));

        // Clone for producer and consumer
        let producer_queue = Arc::clone(&queue);
        let consumer_queue = Arc::clone(&queue);

        // Spawn producer task
        let producer = tokio::spawn(async move {
            for i in 1..=20 {
                // Simulate some work
                sleep(Duration::from_millis(100)).await;

                // Enqueue item
                match producer_queue.enqueue(i).await {
                    Ok(_) => println!("Producer: Enqueued {}", i),
                    Err(e) => println!("Producer: Failed to enqueue {}: {:?}", i, e),
                }
            }

            // Close the queue when done
            producer_queue.close().await;
            println!("Producer: Done, queue closed");
        });

        // Spawn consumer task
        let consumer = tokio::spawn(async move {
            // Get dequeue stream
            let mut items = consumer_queue.dequeue();

            // Process items as they arrive
            while let Some(item) = items.next().await {
                println!("Consumer: Processing {}", item);

                // Simulate slower processing
                sleep(Duration::from_millis(200)).await;

                println!("Consumer: Finished processing {}", item);
            }

            println!("Consumer: Queue exhausted");
        });

        // Wait for both tasks to complete
        let _ = tokio::join!(producer, consumer);
    });
}
```

#### Real-World Example: Message Processing System

```rust
use rs2::queue::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use std::error::Error;

// Define our Message type
#[derive(Debug, Clone)]
struct Message {
    id: u64,
    content: String,
    priority: Priority,
    timestamp: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Priority {
    Low,
    Medium,
    High,
}

// Simulate message processing
async fn process_message(msg: Message) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Processing message {}: '{}'", msg.id, msg.content);

    // Simulate processing time based on priority
    let delay = match msg.priority {
        Priority::High => 50,
        Priority::Medium => 100,
        Priority::Low => 200,
    };

    sleep(Duration::from_millis(delay)).await;
    println!("Completed message {}", msg.id);
    Ok(())
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create queues for different priority levels
        let high_priority_queue = Arc::new(Queue::bounded(5));
        let medium_priority_queue = Arc::new(Queue::bounded(10));
        let low_priority_queue = Arc::new(Queue::bounded(20));

        // Create some test messages
        let messages = vec![
            Message { id: 1, content: "Critical system alert".to_string(), priority: Priority::High, timestamp: 1000 },
            Message { id: 2, content: "User login".to_string(), priority: Priority::Medium, timestamp: 1001 },
            Message { id: 3, content: "Log rotation".to_string(), priority: Priority::Low, timestamp: 1002 },
            Message { id: 4, content: "Security breach detected".to_string(), priority: Priority::High, timestamp: 1003 },
            Message { id: 5, content: "New user registration".to_string(), priority: Priority::Medium, timestamp: 1004 },
            Message { id: 6, content: "Daily report".to_string(), priority: Priority::Low, timestamp: 1005 },
        ];

        // Distribute messages to appropriate queues
        for msg in messages {
            let queue = match msg.priority {
                Priority::High => Arc::clone(&high_priority_queue),
                Priority::Medium => Arc::clone(&medium_priority_queue),
                Priority::Low => Arc::clone(&low_priority_queue),
            };

            println!("Enqueueing message {}: '{}' with {:?} priority", 
                     msg.id, msg.content, msg.priority);
            queue.enqueue(msg).await.unwrap();
        }

        // Process messages from queues with priority
        let high_stream = high_priority_queue.dequeue();
        let medium_stream = medium_priority_queue.dequeue();
        let low_stream = low_priority_queue.dequeue();

        // Create a prioritized stream by merging the queues
        // High priority messages are processed first, then medium, then low
        let prioritized_stream = high_stream
            .chain(medium_stream)
            .chain(low_stream);

        // Process messages with bounded concurrency
        let results = prioritized_stream
            .par_eval_map_rs2(2, |msg| async move {
                let result = process_message(msg.clone()).await;
                (msg, result)
            })
            .collect::<Vec<_>>()
            .await;

        // Report results
        println!("\nProcessing Summary:");
        println!("Total messages processed: {}", results.len());

        let successes = results.iter().filter(|(_, result)| result.is_ok()).count();
        let failures = results.len() - successes;

        println!("Successful: {}", successes);
        println!("Failed: {}", failures);
    });
}
```

## Connectors: Integration with External Systems

RS2 provides a powerful connector system for integrating with external systems like Kafka, Redis, and more. Connectors allow you to create streams from external sources and send streams to external sinks.

### Core Concepts

#### StreamConnector Trait

The `StreamConnector` trait is the core abstraction for all connectors in RS2. It defines the methods that all connectors must implement:

```rust
#[async_trait]
pub trait StreamConnector<T>: Send + Sync
where
    T: Send + 'static,
{
    type Config: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    type Metadata: Send + Sync;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<T>, Self::Error>;
    async fn to_sink(&self, stream: RS2Stream<T>, config: Self::Config) -> Result<Self::Metadata, Self::Error>;
    async fn health_check(&self) -> Result<bool, Self::Error>;
    async fn metadata(&self) -> Result<Self::Metadata, Self::Error>;
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
}
```

#### Kafka Connector Example

RS2 includes a Kafka connector that allows you to create streams from Kafka topics and send streams to Kafka topics:

```rust
use rs2::connectors::{KafkaConnector, CommonConfig};
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a Kafka connector
        let connector = KafkaConnector::new("localhost:9092")
            .with_consumer_group("my-consumer-group");

        // Create a Kafka configuration
        let config = KafkaConfig {
            topic: "my-topic".to_string(),
            partition: None, // All partitions
            key: None,
            headers: HashMap::new(),
            auto_commit: true,
            auto_offset_reset: "earliest".to_string(),
            common: CommonConfig::default(),
        };

        // Check if the connector is healthy
        let healthy = connector.health_check().await.unwrap();
        if !healthy {
            println!("Kafka connector is not healthy!");
            return;
        }

        // Create a stream from Kafka
        let stream = connector.from_source(config).await.unwrap();

        // Process the stream with RS2 transformations
        let processed_stream = stream
            .map_rs2(|msg| {
                println!("Received message: {}", msg);
                format!("Processed: {}", msg)
            })
            .filter_rs2(|msg| !msg.contains("ignore"))
            .throttle_rs2(Duration::from_millis(100));

        // Send the processed stream back to a different Kafka topic
        let sink_config = KafkaConfig {
            topic: "output-topic".to_string(),
            partition: Some(0),
            key: Some("processed".to_string()),
            headers: HashMap::new(),
            auto_commit: true,
            auto_offset_reset: "latest".to_string(),
            common: CommonConfig {
                batch_size: 100,
                timeout_ms: 30000,
                retry_attempts: 3,
                compression: true,
            },
        };

        // Send to sink
        let metadata = connector.to_sink(processed_stream, sink_config).await.unwrap();
        println!("Processed messages sent to Kafka: {:?}", metadata);
    });
}
```

### Creating Custom Connectors

You can create your own connectors by implementing the `StreamConnector` trait:

```rust
use rs2::connectors::{ConnectorError, StreamConnector, CommonConfig};
use rs2::rs2::*;
use async_trait::async_trait;

// Custom connector for a hypothetical message queue
struct MyQueueConnector {
    connection_string: String,
}

// Custom configuration for the connector
#[derive(Clone)]
struct MyQueueConfig {
    queue_name: String,
    common: CommonConfig,
}

// Custom metadata for the connector
#[derive(Debug, Clone)]
struct MyQueueMetadata {
    queue_name: String,
    messages_processed: usize,
}

impl MyQueueConnector {
    fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
        }
    }
}

#[async_trait]
impl StreamConnector<String> for MyQueueConnector {
    type Config = MyQueueConfig;
    type Error = ConnectorError;
    type Metadata = MyQueueMetadata;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<String>, Self::Error> {
        // In a real implementation, you would connect to your message queue
        // and create a stream of messages
        println!("Connecting to {} with queue {}", self.connection_string, config.queue_name);

        // For this example, we'll just return a stream of mock messages
        let messages = vec![
            "Message 1".to_string(),
            "Message 2".to_string(),
            "Message 3".to_string(),
        ];

        Ok(from_iter(messages))
    }

    async fn to_sink(&self, stream: RS2Stream<String>, config: Self::Config) -> Result<Self::Metadata, Self::Error> {
        // In a real implementation, you would send each message in the stream
        // to your message queue
        println!("Sending to {} with queue {}", self.connection_string, config.queue_name);

        // For this example, we'll just count the messages
        let messages: Vec<String> = stream.collect().await;
        let count = messages.len();

        Ok(MyQueueMetadata {
            queue_name: config.queue_name,
            messages_processed: count,
        })
    }

    async fn health_check(&self) -> Result<bool, Self::Error> {
        // In a real implementation, you would check the health of your connection
        Ok(true)
    }

    async fn metadata(&self) -> Result<Self::Metadata, Self::Error> {
        // In a real implementation, you would return metadata about your connection
        Ok(MyQueueMetadata {
            queue_name: "default".to_string(),
            messages_processed: 0,
        })
    }

    fn name(&self) -> &'static str {
        "my-queue-connector"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Create a custom connector
        let connector = MyQueueConnector::new("my-queue-server:1234");

        // Create a configuration
        let config = MyQueueConfig {
            queue_name: "my-queue".to_string(),
            common: CommonConfig::default(),
        };

        // Create a stream from the connector
        let stream = connector.from_source(config.clone()).await.unwrap();

        // Process the stream
        let processed_stream = stream
            .map_rs2(|msg| format!("Processed: {}", msg));

        // Send the processed stream back to the connector
        let metadata = connector.to_sink(processed_stream, config).await.unwrap();

        println!("Processed {} messages for queue {}", 
                 metadata.messages_processed, metadata.queue_name);
    });
}
```

## **Feature Comparison with Other Rust Streaming Libraries**

| **Feature** | **futures-rs** | **tokio-stream** | **async-stream** | **async-std** | **RS2** |
|-------------|----------------|------------------|------------------|---------------|---------|
| **Backpressure Strategies** | ❌ None | ⚠️ Basic buffering | ❌ None | ❌ None | ✅ **4 strategies**: Block, DropOldest, DropNewest, Error |
| **Parallel Processing** | ⚠️ `buffer_unordered` only | ⚠️ Limited buffering | ❌ None | ⚠️ Basic | ✅ **Advanced**: `par_eval_map`, `par_join`, ordered/unordered |
| **Error Recovery** | ⚠️ Manual `Result` handling | ⚠️ Manual | ⚠️ Manual | ⚠️ Manual | ✅ **Automatic**: `recover`, `retry_with_policy`, `on_error_resume_next` |
| **Time Operations** | ❌ None | ⚠️ `throttle`, `timeout` | ❌ None | ⚠️ Basic intervals | ✅ **Rich set**: `debounce`, `sample`, `sliding_window`, `emit_after` |
| **Resource Management** | ❌ Manual | ❌ Manual | ❌ Manual | ❌ Manual | ✅ **Bracket patterns**: Guaranteed cleanup on success/failure |
| **Stream Combinators** | ✅ Standard set | ✅ Extended set | ⚠️ Manual creation | ✅ Standard set | ✅ **Enhanced**: All standard + advanced grouping |
| **Prefetching & Buffering** | ⚠️ Basic `buffered` | ⚠️ Basic buffering | ❌ None | ⚠️ Basic | ✅ **Intelligent**: `prefetch`, `rate_limit_backpressure` |
| **Stream Creation** | ✅ `iter`, `once`, `empty` | ✅ Extended creation | ✅ `stream!` macro | ✅ Standard | ✅ **Rich**: `eval`, `unfold`, `emit_after`, `repeat` |
| **Metrics & Monitoring** | ❌ None | ❌ None | ❌ None | ❌ None | ✅ **Built-in**: `with_metrics`, throughput tracking |
| **Cancellation Safety** | ⚠️ Manual | ⚠️ Manual | ⚠️ Manual | ⚠️ Manual | ✅ **Interrupt-aware**: `interrupt_when` |
| **Memory Efficiency** | ✅ Good | ✅ Good | ✅ Good | ✅ Good | ✅ **Optimized**: Constant memory with backpressure |
| **Functional Style** | ⚠️ Partial | ⚠️ Partial | ❌ Imperative | ⚠️ Partial | ✅ **Pure functional**: Inspired by FS2 |
| **External Connectors** | ❌ None | ❌ None | ❌ None | ❌ None | ✅ **Built-in**: Kafka, custom connectors |

## Pipe: Stream Transformation Functions

A Pipe represents a stream transformation from one type to another. It's a function from Stream[I] to Stream[O] that can be composed with other pipes to create complex stream processing pipelines.

### Pipe Methods

- `Pipe::new(f)` - Create a new pipe from a function
- `apply(input)` - Apply this pipe to a stream
- `compose(other)` - Compose this pipe with another pipe

### Utility Functions

- `map(f)` - Create a pipe that applies the given function to each element
- `filter(predicate)` - Create a pipe that filters elements based on the predicate
- `compose(p1, p2)` - Compose two pipes together
- `identity()` - Identity pipe that doesn't transform the stream

### Examples

#### Basic Pipe Usage

```rust
use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Create a pipe that doubles each number
        let double_pipe = map(|x: i32| x * 2);

        // Apply the pipe to the stream
        let doubled = double_pipe.apply(numbers);

        // Collect the results
        let result = doubled.collect::<Vec<_>>().await;
        println!("Doubled: {:?}", result); // [2, 4, 6, 8, 10]
    });
}
```

#### Composing Pipes

```rust
use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6]);

        // Create pipes for different transformations
        let even_filter = filter(|&x: &i32| x % 2 == 0);
        let double_map = map(|x: i32| x * 2);

        // Compose the pipes using the compose function
        let even_then_double = compose(even_filter, double_map);

        // Apply the composed pipe to the stream
        let result_stream = even_then_double.apply(numbers);

        // Collect the results
        let result = result_stream.collect::<Vec<_>>().await;
        println!("Even numbers doubled: {:?}", result); // [4, 8, 12]

        // Alternatively, use the compose method on the Pipe
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6]);
        let even_filter = filter(|&x: &i32| x % 2 == 0);
        let double_map = map(|x: i32| x * 2);

        // Use the compose method
        let even_then_double = even_filter.compose(double_map);

        // Apply the composed pipe to the stream
        let result_stream = even_then_double.apply(numbers);

        // Collect the results
        let result = result_stream.collect::<Vec<_>>().await;
        println!("Even numbers doubled (using compose method): {:?}", result); // [4, 8, 12]
    });
}
```

#### Real-World Example: User Data Processing Pipeline

```rust
use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;

// Define our User type
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
    login_count: u32,
}

// Define a UserStats type
#[derive(Debug, Clone, PartialEq)]
struct UserStats {
    id: u64,
    name: String,
    role: String,
    is_active: bool,
    login_frequency: &'static str,
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string(), login_count: 120 },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string(), login_count: 45 },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string(), login_count: 5 },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string(), login_count: 80 },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string(), login_count: 30 },
        ];

        // Create pipes for different transformations

        // 1. Filter active users
        let active_filter = filter(|user: &User| user.active);

        // 2. Transform User to UserStats
        let stats_transform = map(|user: User| {
            let frequency = match user.login_count {
                0..=10 => "Low",
                11..=50 => "Medium",
                _ => "High",
            };

            UserStats {
                id: user.id,
                name: user.name,
                role: user.role,
                is_active: user.active,
                login_frequency: frequency,
            }
        });

        // 3. Compose the pipes
        let process_users = active_filter.compose(stats_transform);

        // Apply the processing pipeline to the stream
        let user_stream = from_iter(users);
        let stats_stream = process_users.apply(user_stream);

        // Collect the results
        let user_stats = stats_stream.collect::<Vec<_>>().await;

        // Print the results
        println!("User Statistics:");
        for stats in user_stats {
            println!("  - {} ({}): {} usage", stats.name, stats.role, stats.login_frequency);
        }

        // Group users by login frequency using another pipe
        let group_by_frequency = map(|stats: UserStats| {
            (stats.login_frequency, stats)
        });

        // Apply the grouping pipe to the stats stream
        let user_stream = from_iter(users);
        let stats_stream = process_users.apply(user_stream);
        let grouped_stream = group_by_frequency.apply(stats_stream);

        // Collect and organize by frequency
        let mut frequency_groups: HashMap<&str, Vec<UserStats>> = HashMap::new();
        let grouped_stats = grouped_stream.collect::<Vec<_>>().await;

        for (frequency, stats) in grouped_stats {
            frequency_groups.entry(frequency).or_insert_with(Vec::new).push(stats);
        }

        // Print the grouped results
        println!("\nUsers by Login Frequency:");
        for (frequency, users) in &frequency_groups {
            println!("  {} Usage ({} users):", frequency, users.len());
            for user in users {
                println!("    - {} ({})", user.name, user.role);
            }
        }
    });
}
```

## Queue: Concurrent Queue with Stream Interface

A Queue represents a concurrent queue with a Stream interface for dequeuing and async methods for enqueuing. It supports both bounded and unbounded queues.

### Queue Types

- `Queue::bounded(capacity)` - Create a new bounded queue with the given capacity
- `Queue::unbounded()` - Create a new unbounded queue

### Queue Methods

- `enqueue(item)` - Enqueue an item into the queue
- `try_enqueue(item)` - Try to enqueue an item without blocking
- `dequeue()` - Get a stream for dequeuing items
- `close()` - Close the queue, preventing further enqueues
- `capacity()` - Get the capacity of the queue (None for unbounded)
- `is_empty()` - Check if the queue is empty
- `len()` - Get the current number of items in the queue

### Examples

#### Basic Queue Usage

```rust
use rs2::queue::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a bounded queue with capacity 5
        let queue = Queue::bounded(5);

        // Enqueue some items
        for i in 1..=3 {
            queue.enqueue(i).await.unwrap();
            println!("Enqueued: {}", i);
        }

        // Get the current queue length
        let len = queue.len().await;
        println!("Queue length: {}", len); // 3

        // Get a stream for dequeuing
        let mut dequeue_stream = queue.dequeue();

        // Dequeue and process items
        while let Some(item) = dequeue_stream.next().await {
            println!("Dequeued: {}", item);
        }
    });
}
```

#### Producer-Consumer Pattern

```rust
use rs2::queue::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use std::sync::Arc;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a shared queue
        let queue = Arc::new(Queue::bounded(10));

        // Clone for producer and consumer
        let producer_queue = Arc::clone(&queue);
        let consumer_queue = Arc::clone(&queue);

        // Spawn producer task
        let producer = tokio::spawn(async move {
            for i in 1..=20 {
                // Simulate some work
                sleep(Duration::from_millis(100)).await;

                // Enqueue item
                match producer_queue.enqueue(i).await {
                    Ok(_) => println!("Producer: Enqueued {}", i),
                    Err(e) => println!("Producer: Failed to enqueue {}: {:?}", i, e),
                }
            }

            // Close the queue when done
            producer_queue.close().await;
            println!("Producer: Done, queue closed");
        });

        // Spawn consumer task
        let consumer = tokio::spawn(async move {
            // Get dequeue stream
            let mut items = consumer_queue.dequeue();

            // Process items as they arrive
            while let Some(item) = items.next().await {
                println!("Consumer: Processing {}", item);

                // Simulate slower processing
                sleep(Duration::from_millis(200)).await;

                println!("Consumer: Finished processing {}", item);
            }

            println!("Consumer: Queue exhausted");
        });

        // Wait for both tasks to complete
        let _ = tokio::join!(producer, consumer);
    });
}
```

#### Real-World Example: Message Processing System

```rust
use rs2::queue::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use std::error::Error;

// Define our Message type
#[derive(Debug, Clone)]
struct Message {
    id: u64,
    content: String,
    priority: Priority,
    timestamp: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Priority {
    Low,
    Medium,
    High,
}

// Simulate message processing
async fn process_message(msg: Message) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Processing message {}: '{}'", msg.id, msg.content);

    // Simulate processing time based on priority
    let delay = match msg.priority {
        Priority::High => 50,
        Priority::Medium => 100,
        Priority::Low => 200,
    };

    sleep(Duration::from_millis(delay)).await;
    println!("Completed message {}", msg.id);
    Ok(())
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create queues for different priority levels
        let high_priority_queue = Arc::new(Queue::bounded(5));
        let medium_priority_queue = Arc::new(Queue::bounded(10));
        let low_priority_queue = Arc::new(Queue::bounded(20));

        // Create some test messages
        let messages = vec![
            Message { id: 1, content: "Critical system alert".to_string(), priority: Priority::High, timestamp: 1000 },
            Message { id: 2, content: "User login".to_string(), priority: Priority::Medium, timestamp: 1001 },
            Message { id: 3, content: "Log rotation".to_string(), priority: Priority::Low, timestamp: 1002 },
            Message { id: 4, content: "Security breach detected".to_string(), priority: Priority::High, timestamp: 1003 },
            Message { id: 5, content: "New user registration".to_string(), priority: Priority::Medium, timestamp: 1004 },
            Message { id: 6, content: "Daily report".to_string(), priority: Priority::Low, timestamp: 1005 },
        ];

        // Distribute messages to appropriate queues
        for msg in messages {
            let queue = match msg.priority {
                Priority::High => Arc::clone(&high_priority_queue),
                Priority::Medium => Arc::clone(&medium_priority_queue),
                Priority::Low => Arc::clone(&low_priority_queue),
            };

            println!("Enqueueing message {}: '{}' with {:?} priority", 
                     msg.id, msg.content, msg.priority);
            queue.enqueue(msg).await.unwrap();
        }

        // Process messages from queues with priority
        let high_stream = high_priority_queue.dequeue();
        let medium_stream = medium_priority_queue.dequeue();
        let low_stream = low_priority_queue.dequeue();

        // Create a prioritized stream by merging the queues
        // High priority messages are processed first, then medium, then low
        let prioritized_stream = high_stream
            .chain(medium_stream)
            .chain(low_stream);

        // Process messages with bounded concurrency
        let results = prioritized_stream
            .par_eval_map_rs2(2, |msg| async move {
                let result = process_message(msg.clone()).await;
                (msg, result)
            })
            .collect::<Vec<_>>()
            .await;

        // Report results
        println!("\nProcessing Summary:");
        println!("Total messages processed: {}", results.len());

        let successes = results.iter().filter(|(_, result)| result.is_ok()).count();
        let failures = results.len() - successes;

        println!("Successful: {}", successes);
        println!("Failed: {}", failures);
    });
}
```
