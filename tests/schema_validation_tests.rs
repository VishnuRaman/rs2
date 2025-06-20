use rs2_stream::schema_validation::{JsonSchemaValidator, SchemaError, SchemaValidator};
use serde_json::json;

#[tokio::test]
async fn test_valid_json_passes_schema() {
    let schema = json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "value": {"type": "integer"}
        },
        "required": ["id", "value"]
    });
    let validator = JsonSchemaValidator::new("test-schema", schema);
    let valid = json!({"id": "abc", "value": 42});
    let result = validator
        .validate(&serde_json::to_vec(&valid).unwrap())
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_invalid_json_fails_schema() {
    let schema = json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "value": {"type": "integer"}
        },
        "required": ["id", "value"]
    });
    let validator = JsonSchemaValidator::new("test-schema", schema);
    let invalid = json!({"id": "abc"}); // missing 'value'
    let result = validator
        .validate(&serde_json::to_vec(&invalid).unwrap())
        .await;
    assert!(matches!(result, Err(SchemaError::ValidationFailed(_))));
}

#[tokio::test]
async fn test_wrong_type_fails_schema() {
    let schema = json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "value": {"type": "integer"}
        },
        "required": ["id", "value"]
    });
    let validator = JsonSchemaValidator::new("test-schema", schema);
    let wrong_type = json!({"id": "abc", "value": "not-an-integer"});
    let result = validator
        .validate(&serde_json::to_vec(&wrong_type).unwrap())
        .await;
    assert!(matches!(result, Err(SchemaError::ValidationFailed(_))));
}

#[tokio::test]
async fn test_parse_error() {
    let schema = json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"}
        },
        "required": ["id"]
    });
    let validator = JsonSchemaValidator::new("test-schema", schema);
    let not_json = b"not a json";
    let result = validator.validate(not_json).await;
    assert!(matches!(result, Err(SchemaError::ParseError(_))));
}

#[tokio::test]
async fn test_get_schema_id() {
    let schema = json!({"type": "object"});
    let validator = JsonSchemaValidator::new("my-schema-id", schema);
    assert_eq!(validator.get_schema_id(), "my-schema-id");
}
