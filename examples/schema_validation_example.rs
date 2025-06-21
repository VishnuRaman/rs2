//! Example demonstrating JSON Schema validation with RS2 streams
//!
//! This example shows how to:
//! 1. Create JSON schemas for different data types
//! 2. Set up schema validators
//! 3. Validate data against schemas
//! 4. Handle validation errors gracefully

use rs2_stream::schema_validation::{JsonSchemaValidator, SchemaValidator};
use serde_json::json;

async fn run_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RS2 JSON Schema Validation Example");
    println!("=====================================\n");

    // Create JSON schemas for different data types
    let user_schema = json!({
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "minLength": 1},
            "event_type": {"type": "string", "enum": ["login", "logout", "purchase", "view"]},
            "timestamp": {"type": "integer", "minimum": 0},
            "data": {"type": "object"}
        },
        "required": ["user_id", "event_type", "timestamp"]
    });

    let order_schema = json!({
        "type": "object",
        "properties": {
            "order_id": {"type": "string", "pattern": "^ORD-[0-9]{6}$"},
            "customer_id": {"type": "string", "minLength": 1},
            "amount": {"type": "number", "minimum": 0.01},
            "items": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "status": {"type": "string", "enum": ["pending", "confirmed", "shipped", "delivered"]}
        },
        "required": ["order_id", "customer_id", "amount", "items", "status"]
    });

    let sensor_schema = json!({
        "type": "object",
        "properties": {
            "sensor_id": {"type": "string", "pattern": "^SENSOR-[A-Z0-9]{4}$"},
            "temperature": {"type": "number", "minimum": -50, "maximum": 100},
            "humidity": {"type": "number", "minimum": 0, "maximum": 100},
            "timestamp": {"type": "integer", "minimum": 0}
        },
        "required": ["sensor_id", "temperature", "humidity", "timestamp"]
    });

    // Create validators
    let user_validator = JsonSchemaValidator::new("user_events", user_schema);
    let order_validator = JsonSchemaValidator::new("order_events", order_schema);
    let sensor_validator = JsonSchemaValidator::new("sensor_data", sensor_schema);

    println!("✅ Created validators for:");
    println!("   - User Events (ID: {})", user_validator.get_schema_id());
    println!(
        "   - Order Events (ID: {})",
        order_validator.get_schema_id()
    );
    println!(
        "   - Sensor Data (ID: {})",
        sensor_validator.get_schema_id()
    );
    println!();

    // Test 1: Valid user events
    println!("📋 Test 1: Validating User Events");
    println!("----------------------------------");

    let valid_user_events = vec![
        json!({
            "user_id": "user123",
            "event_type": "login",
            "timestamp": 1640995200,
            "data": {"ip": "192.168.1.1", "user_agent": "Mozilla/5.0"}
        }),
        json!({
            "user_id": "user456",
            "event_type": "purchase",
            "timestamp": 1640995300,
            "data": {"product_id": "prod789", "price": 29.99}
        }),
        json!({
            "user_id": "user789",
            "event_type": "logout",
            "timestamp": 1640995400,
            "data": {}
        }),
    ];

    for (i, event) in valid_user_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        match user_validator.validate(&event_bytes).await {
            Ok(()) => println!("   ✅ Event {}: Valid user event", i + 1),
            Err(e) => println!("   ❌ Event {}: {}", i + 1, e),
        }
    }
    println!();

    // Test 2: Invalid user events
    println!("📋 Test 2: Invalid User Events (Error Handling)");
    println!("------------------------------------------------");

    let invalid_user_events = vec![
        json!({
            "user_id": "",  // Empty user_id (minLength violation)
            "event_type": "login",
            "timestamp": 1640995200
        }),
        json!({
            "user_id": "user123",
            "event_type": "invalid_event",  // Not in enum
            "timestamp": 1640995200
        }),
        json!({
            "user_id": "user123",
            "event_type": "login",
            "timestamp": -1  // Negative timestamp
        }),
        json!({
            "user_id": "user123"
            // Missing required fields
        }),
    ];

    for (i, event) in invalid_user_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        match user_validator.validate(&event_bytes).await {
            Ok(()) => println!("   ✅ Event {}: Valid (unexpected)", i + 1),
            Err(e) => println!("   ❌ Event {}: {}", i + 1, e),
        }
    }
    println!();

    // Test 3: Order validation
    println!("📋 Test 3: Order Event Validation");
    println!("----------------------------------");

    let order_events = vec![
        json!({
            "order_id": "ORD-123456",
            "customer_id": "cust789",
            "amount": 99.99,
            "items": ["item1", "item2", "item3"],
            "status": "confirmed"
        }),
        json!({
            "order_id": "INVALID-123",  // Wrong pattern
            "customer_id": "cust789",
            "amount": 0.0,  // Below minimum
            "items": [],
            "status": "invalid_status"
        }),
    ];

    for (i, event) in order_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        match order_validator.validate(&event_bytes).await {
            Ok(()) => println!("   ✅ Order {}: Valid order", i + 1),
            Err(e) => println!("   ❌ Order {}: {}", i + 1, e),
        }
    }
    println!();

    // Test 4: Sensor data validation
    println!("📋 Test 4: Sensor Data Validation");
    println!("----------------------------------");

    let sensor_events = vec![
        json!({
            "sensor_id": "SENSOR-A123",
            "temperature": 25.5,
            "humidity": 60.0,
            "timestamp": 1640995200
        }),
        json!({
            "sensor_id": "INVALID-SENSOR",  // Wrong pattern
            "temperature": 150.0,  // Above maximum
            "humidity": 150.0,  // Above maximum
            "timestamp": 1640995200
        }),
    ];

    for (i, event) in sensor_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        match sensor_validator.validate(&event_bytes).await {
            Ok(()) => println!("   ✅ Sensor {}: Valid sensor data", i + 1),
            Err(e) => println!("   ❌ Sensor {}: {}", i + 1, e),
        }
    }
    println!();

    // Test 5: Multi-validator validation
    println!("📋 Test 5: Multi-Validator Validation");
    println!("-------------------------------------");

    let mixed_events = vec![
        // Valid user event
        json!({
            "user_id": "user123",
            "event_type": "login",
            "timestamp": 1640995200,
            "data": {"ip": "192.168.1.1"}
        }),
        // Invalid user event (missing timestamp)
        json!({
            "user_id": "user456",
            "event_type": "login"
        }),
        // Valid order event
        json!({
            "order_id": "ORD-654321",
            "customer_id": "cust123",
            "amount": 149.99,
            "items": ["laptop", "mouse"],
            "status": "pending"
        }),
        // Invalid order event (wrong pattern)
        json!({
            "order_id": "WRONG-123",
            "customer_id": "cust123",
            "amount": 149.99,
            "items": ["laptop"],
            "status": "pending"
        }),
    ];

    let mut valid_count = 0;
    let mut invalid_count = 0;

    for (i, event) in mixed_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;

        // Try user validator first
        match user_validator.validate(&event_bytes).await {
            Ok(()) => {
                println!("   ✅ Event {}: Valid user event", i + 1);
                valid_count += 1;
            }
            Err(_) => {
                // Try order validator
                match order_validator.validate(&event_bytes).await {
                    Ok(()) => {
                        println!("   ✅ Event {}: Valid order event", i + 1);
                        valid_count += 1;
                    }
                    Err(e) => {
                        println!("   ❌ Event {}: Invalid - {}", i + 1, e);
                        invalid_count += 1;
                    }
                }
            }
        }
    }

    println!("\n📊 Validation Summary:");
    println!("   - Total events processed: {}", mixed_events.len());
    println!("   - Valid events: {}", valid_count);
    println!("   - Invalid events: {}", invalid_count);

    // Test 6: Error handling demonstration
    println!("\n📋 Test 6: Error Handling Demonstration");
    println!("----------------------------------------");

    let problematic_events = vec![
        // Valid event
        json!({
            "user_id": "user999",
            "event_type": "view",
            "timestamp": 1640995500,
            "data": {"page": "/home"}
        }),
        // Invalid event (missing required field)
        json!({
            "user_id": "user888",
            "event_type": "purchase"
            // Missing timestamp
        }),
        // Valid event after invalid one
        json!({
            "user_id": "user777",
            "event_type": "logout",
            "timestamp": 1640995600,
            "data": {}
        }),
    ];

    let mut success_count = 0;
    let mut error_count = 0;

    for (i, event) in problematic_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;

        match user_validator.validate(&event_bytes).await {
            Ok(()) => {
                println!("   ✅ Event {}: Successfully validated", i + 1);
                success_count += 1;
            }
            Err(e) => {
                println!("   ❌ Event {}: Validation failed - {}", i + 1, e);
                error_count += 1;
            }
        }
    }

    println!("\n📊 Error Handling Summary:");
    println!("   - Successfully processed: {}", success_count);
    println!("   - Errors encountered: {}", error_count);

    println!("\n🎉 Schema validation example completed successfully!");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run_example().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
