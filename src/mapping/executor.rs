use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use tokio_postgres::Client as PgClient;
use tracing::{debug, warn};

use super::{FieldMapping, FieldSource, FieldType, MappingConfig, TopicMapping};
use crate::db::RawMessage;

/// Execute mappings on an MQTT message and insert into database
pub async fn execute_mappings(
    topic: &str,
    payload: &[u8],
    mappings: &MappingConfig,
    db_client: &PgClient,
) -> Result<()> {
    // Convert payload to string
    let payload_str = match String::from_utf8(payload.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode payload as UTF-8: {}", e);
            return Ok(());
        }
    };

    // Always store raw message for audit trail
    let raw_msg = RawMessage {
        topic: topic.to_string(),
        payload: payload_str.clone(),
        timestamp: Utc::now(),
    };
    raw_msg.insert(db_client).await?;

    // Try to parse as JSON
    let json_value = match serde_json::from_str::<Value>(&payload_str) {
        Ok(v) => v,
        Err(_) => {
            debug!("Payload is not valid JSON, only storing raw message");
            return Ok(());
        }
    };

    // Find matching mapping
    let mapping = match mappings.find_mapping(topic) {
        Some(m) => m,
        None => {
            debug!("No mapping found for topic: {}", topic);
            return Ok(());
        }
    };

    debug!("Using mapping '{}' for topic '{}'", mapping.name, topic);

    // Check if we should expand numeric fields
    if let Some(expand_config) = &mapping.expand_numeric_fields {
        if expand_config.enabled {
            return execute_expanded_mapping(topic, &json_value, mapping, expand_config, db_client)
                .await;
        }
    }

    // Standard mapping (one row)
    execute_standard_mapping(topic, &json_value, mapping, db_client).await
}

/// Execute mapping that expands numeric fields into multiple rows
async fn execute_expanded_mapping(
    topic: &str,
    json: &Value,
    mapping: &TopicMapping,
    expand_config: &super::ExpandConfig,
    db_client: &PgClient,
) -> Result<()> {
    // Extract base fields that will be included in each row
    let mut base_fields = HashMap::new();
    for (field_name, field_mapping) in &mapping.fields {
        if expand_config.include_fields.contains(field_name) {
            if let Some(value) = extract_field_value(topic, json, field_mapping)? {
                let target = field_mapping.target.as_ref().unwrap_or(field_name);
                base_fields.insert(target.clone(), value);
            }
        }
    }

    // Find numeric fields to expand
    let obj = json.as_object().context("JSON is not an object")?;
    let mut row_count = 0;

    for (key, value) in obj {
        // Skip excluded fields
        if expand_config.exclude.contains(&key.to_string()) {
            continue;
        }

        // Only expand numeric values
        if let Some(num) = value.as_f64() {
            let mut row_data = base_fields.clone();
            row_data.insert(
                expand_config.sensor_name_from.clone(),
                FieldValue::String(key.clone()),
            );
            row_data.insert(
                expand_config.value_from.clone(),
                FieldValue::Number(num),
            );

            // Insert the row
            insert_row(&mapping.table, &row_data, db_client).await?;
            row_count += 1;
        }
    }

    debug!(
        "Expanded {} numeric fields into {} rows for mapping '{}'",
        row_count, row_count, mapping.name
    );

    Ok(())
}

/// Execute standard mapping (one message -> one row)
async fn execute_standard_mapping(
    topic: &str,
    json: &Value,
    mapping: &TopicMapping,
    db_client: &PgClient,
) -> Result<()> {
    let mut row_data = HashMap::new();

    for (field_name, field_mapping) in &mapping.fields {
        if let Some(value) = extract_field_value(topic, json, field_mapping)? {
            let target = field_mapping.target.as_ref().unwrap_or(field_name);
            row_data.insert(target.clone(), value);
        }
    }

    insert_row(&mapping.table, &row_data, db_client).await?;

    debug!(
        "Inserted 1 row using mapping '{}' into table '{}'",
        mapping.name, mapping.table
    );

    Ok(())
}

/// Extract a field value based on its mapping configuration
fn extract_field_value(
    topic: &str,
    json: &Value,
    mapping: &FieldMapping,
) -> Result<Option<FieldValue>> {
    let raw_value = match mapping.source {
        FieldSource::Json => {
            let path = mapping
                .path
                .as_ref()
                .context("JSON source requires 'path' field")?;
            extract_json_path(json, path)
        }
        FieldSource::Topic => {
            if let Some(extract_pattern) = &mapping.extract {
                extract_from_topic(topic, extract_pattern)?
            } else {
                Some(Value::String(topic.to_string()))
            }
        }
        FieldSource::CurrentTime => Some(Value::String(Utc::now().to_rfc3339())),
        FieldSource::Constant => {
            if let Some(v) = &mapping.value {
                Some(Value::String(v.clone()))
            } else {
                return Err(anyhow!("Constant source requires 'value' field"));
            }
        }
    };

    // Use default if no value found
    let value = match raw_value {
        Some(v) => v,
        None => {
            if let Some(default) = &mapping.default {
                if default == "now" {
                    Value::String(Utc::now().to_rfc3339())
                } else {
                    Value::String(default.clone())
                }
            } else {
                return Ok(None);
            }
        }
    };

    // Convert to target type
    let converted = convert_value(value, &mapping.r#type)?;
    Ok(Some(converted))
}

/// Extract value from JSON using a simple path (supports dot notation)
fn extract_json_path(json: &Value, path: &str) -> Option<Value> {
    if path == "." {
        return Some(json.clone());
    }

    let parts: Vec<&str> = path.split('.').collect();
    let mut current = json;

    for part in parts {
        current = current.get(part)?;
    }

    Some(current.clone())
}

/// Extract value from topic using regex
fn extract_from_topic(topic: &str, pattern: &str) -> Result<Option<Value>> {
    let re = Regex::new(pattern).context("Invalid regex pattern")?;

    if let Some(captures) = re.captures(topic) {
        if captures.len() > 1 {
            let extracted = captures.get(1).unwrap().as_str();
            return Ok(Some(Value::String(extracted.to_string())));
        }
    }

    Ok(None)
}

/// Convert JSON value to target type
fn convert_value(value: Value, target_type: &FieldType) -> Result<FieldValue> {
    match target_type {
        FieldType::String => {
            let s = match value {
                Value::String(s) => s,
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => value.to_string(),
            };
            Ok(FieldValue::String(s))
        }
        FieldType::Number => {
            let num = value
                .as_f64()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .context("Cannot convert to number")?;
            Ok(FieldValue::Number(num))
        }
        FieldType::Integer => {
            let int = value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .context("Cannot convert to integer")?;
            Ok(FieldValue::Integer(int))
        }
        FieldType::Boolean => {
            let bool = value
                .as_bool()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                .context("Cannot convert to boolean")?;
            Ok(FieldValue::Boolean(bool))
        }
        FieldType::Timestamp => {
            let ts_str = value.as_str().context("Timestamp must be a string")?;

            // Try parsing as ISO8601/RFC3339
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                return Ok(FieldValue::Timestamp(dt.with_timezone(&Utc)));
            }

            // Try parsing as Unix timestamp
            if let Ok(ts_num) = ts_str.parse::<i64>() {
                // Check if milliseconds
                let (secs, nsecs) = if ts_num > 4102444800 {
                    (ts_num / 1000, ((ts_num % 1000) * 1_000_000) as u32)
                } else {
                    (ts_num, 0)
                };

                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    return Ok(FieldValue::Timestamp(dt));
                }
            }

            Err(anyhow!("Cannot parse timestamp: {}", ts_str))
        }
    }
}

/// Represents a typed field value for database insertion
#[derive(Debug, Clone)]
pub enum FieldValue {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Timestamp(chrono::DateTime<Utc>),
}

/// Insert a row into the database
async fn insert_row(
    table: &str,
    data: &HashMap<String, FieldValue>,
    db_client: &PgClient,
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    // Build SQL query
    let columns: Vec<String> = data.keys().cloned().collect();
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        columns.join(", "),
        placeholders.join(", ")
    );

    // Convert values to postgres types
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
    let values: Vec<_> = columns
        .iter()
        .map(|col| data.get(col).unwrap())
        .collect();

    // We need to hold these in memory for the lifetime of the query
    let string_values: Vec<String> = values
        .iter()
        .filter_map(|v| match v {
            FieldValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    let number_values: Vec<f64> = values
        .iter()
        .filter_map(|v| match v {
            FieldValue::Number(n) => Some(*n),
            _ => None,
        })
        .collect();
    let integer_values: Vec<i64> = values
        .iter()
        .filter_map(|v| match v {
            FieldValue::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    let boolean_values: Vec<bool> = values
        .iter()
        .filter_map(|v| match v {
            FieldValue::Boolean(b) => Some(*b),
            _ => None,
        })
        .collect();
    let timestamp_values: Vec<chrono::DateTime<Utc>> = values
        .iter()
        .filter_map(|v| match v {
            FieldValue::Timestamp(ts) => Some(*ts),
            _ => None,
        })
        .collect();

    let mut string_idx = 0;
    let mut number_idx = 0;
    let mut integer_idx = 0;
    let mut boolean_idx = 0;
    let mut timestamp_idx = 0;

    for value in &values {
        match value {
            FieldValue::String(_) => {
                params.push(&string_values[string_idx]);
                string_idx += 1;
            }
            FieldValue::Number(_) => {
                params.push(&number_values[number_idx]);
                number_idx += 1;
            }
            FieldValue::Integer(_) => {
                params.push(&integer_values[integer_idx]);
                integer_idx += 1;
            }
            FieldValue::Boolean(_) => {
                params.push(&boolean_values[boolean_idx]);
                boolean_idx += 1;
            }
            FieldValue::Timestamp(_) => {
                params.push(&timestamp_values[timestamp_idx]);
                timestamp_idx += 1;
            }
        }
    }

    db_client
        .execute(&sql, &params)
        .await
        .with_context(|| format!("Failed to insert into table: {}", table))?;

    Ok(())
}
