mod executor;

pub use executor::execute_mappings;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingConfig {
    pub mappings: Vec<TopicMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMapping {
    /// Human-readable name for this mapping
    pub name: String,

    /// MQTT topic pattern (supports + and # wildcards)
    pub topic_pattern: String,

    /// Target database table
    pub table: String,

    /// Insert mode: insert, upsert, or update
    #[serde(default = "default_mode")]
    pub mode: InsertMode,

    /// Key field for upsert/update operations
    pub key: Option<String>,

    /// Field mappings
    #[serde(default)]
    pub fields: HashMap<String, FieldMapping>,

    /// Configuration for expanding JSON into multiple rows
    pub expand_numeric_fields: Option<ExpandConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum InsertMode {
    Insert,
    Upsert,
    Update,
}

fn default_mode() -> InsertMode {
    InsertMode::Insert
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Source of the data: json, topic, current_time, constant
    pub source: FieldSource,

    /// For JSON source: JSONPath expression (e.g., "temperature" or "metadata.location")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// For topic source: regex to extract value (e.g., "device/(.+)/data")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extract: Option<String>,

    /// For constant source: the constant value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Target column name (defaults to field name if not specified)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,

    /// Data type conversion
    #[serde(default = "default_field_type")]
    pub r#type: FieldType,

    /// Default value if source is missing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldSource {
    Json,
    Topic,
    CurrentTime,
    Constant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Number,
    Integer,
    Boolean,
    Timestamp,
}

fn default_field_type() -> FieldType {
    FieldType::String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandConfig {
    /// Whether to expand numeric fields into separate rows
    #[serde(default)]
    pub enabled: bool,

    /// JSON keys to exclude from expansion
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Column name for the JSON key (e.g., "sensor_name")
    pub sensor_name_from: String,

    /// Column name for the JSON value (e.g., "value")
    pub value_from: String,

    /// Additional fields to include in each expanded row
    #[serde(default)]
    pub include_fields: Vec<String>,
}

impl MappingConfig {
    pub fn load(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read mappings file: {}", path))?;

        let config: MappingConfig = serde_yaml::from_str(&contents)
            .with_context(|| "Failed to parse mappings YAML")?;

        Ok(config)
    }

    /// Find a mapping that matches the given topic
    pub fn find_mapping(&self, topic: &str) -> Option<&TopicMapping> {
        self.mappings.iter().find(|m| topic_matches(&m.topic_pattern, topic))
    }
}

impl Default for MappingConfig {
    fn default() -> Self {
        Self {
            mappings: vec![
                TopicMapping {
                    name: "organ_bath_telemetry".to_string(),
                    topic_pattern: "device/organ_bath/+".to_string(),
                    table: "telemetry".to_string(),
                    mode: InsertMode::Insert,
                    key: None,
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert(
                            "device_id".to_string(),
                            FieldMapping {
                                source: FieldSource::Topic,
                                path: None,
                                extract: Some(r"device/organ_bath/(.+)".to_string()),
                                value: None,
                                target: Some("device_id".to_string()),
                                r#type: FieldType::String,
                                default: Some("unknown".to_string()),
                            },
                        );
                        fields.insert(
                            "timestamp".to_string(),
                            FieldMapping {
                                source: FieldSource::Json,
                                path: Some("timestamp".to_string()),
                                extract: None,
                                value: None,
                                target: Some("timestamp".to_string()),
                                r#type: FieldType::Timestamp,
                                default: Some("now".to_string()),
                            },
                        );
                        fields.insert(
                            "topic".to_string(),
                            FieldMapping {
                                source: FieldSource::Topic,
                                path: None,
                                extract: None,
                                value: None,
                                target: Some("topic".to_string()),
                                r#type: FieldType::String,
                                default: None,
                            },
                        );
                        fields
                    },
                    expand_numeric_fields: Some(ExpandConfig {
                        enabled: true,
                        exclude: vec!["timestamp".to_string(), "device_id".to_string(), "ts".to_string()],
                        sensor_name_from: "sensor_name".to_string(),
                        value_from: "value".to_string(),
                        include_fields: vec!["device_id".to_string(), "timestamp".to_string(), "topic".to_string()],
                    }),
                },
            ],
        }
    }
}

/// Check if an MQTT topic matches a pattern with wildcards
/// Supports: + (single level) and # (multi level)
fn topic_matches(pattern: &str, topic: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let topic_parts: Vec<&str> = topic.split('/').collect();

    let mut pattern_idx = 0;
    let mut topic_idx = 0;

    while pattern_idx < pattern_parts.len() && topic_idx < topic_parts.len() {
        let pattern_part = pattern_parts[pattern_idx];

        if pattern_part == "#" {
            // Multi-level wildcard matches everything remaining
            return true;
        } else if pattern_part == "+" {
            // Single-level wildcard matches one level
            pattern_idx += 1;
            topic_idx += 1;
        } else if pattern_part == topic_parts[topic_idx] {
            // Exact match
            pattern_idx += 1;
            topic_idx += 1;
        } else {
            // No match
            return false;
        }
    }

    // Both should be exhausted for a match (unless pattern ends with #)
    pattern_idx == pattern_parts.len() && topic_idx == topic_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matching() {
        assert!(topic_matches("device/organ_bath/+", "device/organ_bath/ob1"));
        assert!(topic_matches("device/+/status", "device/ob1/status"));
        assert!(topic_matches("device/#", "device/organ_bath/ob1/status"));
        assert!(!topic_matches("device/+/status", "device/ob1/data"));
        assert!(!topic_matches("device/organ_bath/+", "device/organ_bath/ob1/extra"));
    }
}
