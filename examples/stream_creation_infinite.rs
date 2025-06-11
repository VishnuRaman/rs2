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