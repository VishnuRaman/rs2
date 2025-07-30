use tokio::runtime::Runtime;
use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("=== RS2 Infinite Stream Creation Example ===\n");

        // 1. Create a stream that repeats a notification
        println!("1. Repeating Notification Stream:");
        let notification = "🔔 New message!";
        let notification_stream = repeat_rs2(notification).take_rs2(3); // Limit to 3 notifications

        let notifications = notification_stream.collect_rs2().await;
        println!("   Notifications received:");
        for (i, notif) in notifications.iter().enumerate() {
            println!("   {}. {}", i + 1, notif);
        }

        // 2. Create a stream of user IDs using unfold
        println!("\n2. User ID Generation Stream:");
        let user_id_stream = unfold_rs2(
            1, // Start with user ID 1
            |id| async move {
                if id <= 5 {
                    // Simulate some async work for ID generation
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    // Generate the next user ID
                    Some((id, id + 1))
                } else {
                    None // End the stream after user ID 5
                }
            },
        );

        let user_ids = user_id_stream.collect_rs2().await;
        println!("   Generated User IDs:");
        for (i, id) in user_ids.iter().enumerate() {
            println!("   User {}: ID-{:03}", i + 1, id);
        }

        // 3. Create a fibonacci sequence stream using unfold
        println!("\n3. Fibonacci Sequence Stream:");
        let fibonacci_stream = unfold_rs2(
            (0, 1), // Start with fibonacci seeds (0, 1)
            |(current, next)| async move {
                if current < 100 { // Stop when we reach 100
                    let new_next = current + next;
                    Some((current, (next, new_next)))
                } else {
                    None
                }
            },
        );

        let fibonacci_numbers = fibonacci_stream.collect_rs2().await;
        println!("   Fibonacci sequence (< 100):");
        for (i, num) in fibonacci_numbers.iter().enumerate() {
            print!("   {}", num);
            if i < fibonacci_numbers.len() - 1 {
                print!(", ");
            }
        }
        println!();

        // 4. Create a counter stream with custom increment
        println!("\n4. Custom Counter Stream:");
        let counter_stream = unfold_rs2(
            (0, 2), // Start at 0, increment by 2
            |(count, increment)| async move {
                if count < 20 {
                    Some((count, (count + increment, increment)))
                } else {
                    None
                }
            },
        );

        let counter_values = counter_stream.collect_rs2().await;
        println!("   Even numbers (0 to 18):");
        for (i, num) in counter_values.iter().enumerate() {
            print!("   {}", num);
            if i < counter_values.len() - 1 {
                print!(", ");
            }
        }
        println!();

        println!("\n=== Infinite Stream Creation Example Complete ===");
        println!("\nKey Features Demonstrated:");
        println!("1. Repeating stream creation with repeat_rs2()");
        println!("2. Stream limiting with take_rs2()");
        println!("3. Custom stream generation with unfold_rs2()");
        println!("4. Stateful stream creation (fibonacci, counters)");
        println!("5. Stream collection with collect_rs2()");
    });
}
