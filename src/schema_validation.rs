//! Schema validation system for RS2 streams

use async_trait::async_trait;
use jsonschema::{validator_for, Validator};
use serde_json::Value;
use std::sync::Arc;

pub type ValidationResult = Result<(), SchemaError>;

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Validation failed: {0}")]
    ValidationFailed(String),
    #[error("Missing schema: {0}")]
    MissingSchema(String),
    #[error("Parse error: {0}")]
    ParseError(String),
}

#[async_trait]
pub trait SchemaValidator: Send + Sync + Clone {
    async fn validate(&self, data: &[u8]) -> Result<(), SchemaError>;
    fn get_schema_id(&self) -> String;
}

/// Production-ready JSON Schema validator for RS2 streams.
#[derive(Clone)]
pub struct JsonSchemaValidator {
    schema_id: String,
    compiled: Arc<Validator>,
}

impl JsonSchemaValidator {
    /// Create a new validator from a JSON schema value.
    pub fn new(schema_id: &str, schema: Value) -> Self {
        let compiled = validator_for(&schema).expect("Invalid JSON schema");
        Self {
            schema_id: schema_id.to_string(),
            compiled: Arc::new(compiled),
        }
    }
}

#[async_trait]
impl SchemaValidator for JsonSchemaValidator {
    async fn validate(&self, data: &[u8]) -> Result<(), SchemaError> {
        let value: Value =
            serde_json::from_slice(data).map_err(|e| SchemaError::ParseError(e.to_string()))?;
        if let Err(error) = self.compiled.validate(&value) {
            return Err(SchemaError::ValidationFailed(error.to_string()));
        }
        Ok(())
    }
    fn get_schema_id(&self) -> String {
        self.schema_id.clone()
    }
}
