use chrono::Utc;
use serde_json::Value;
use tracing::{debug, warn};

use crate::db::{RawMessage, TelemetryReading};

/// Parse MQTT message into database records
/// Handles simple JSON telemetry like {"temperature": 80, "ph": 2.4}
pub fn parse_message(topic: &str, payload: &[u8]) -> Vec<ParsedMessage> {
    let mut results = Vec::new();

    // Convert payload to string
    let payload_str = match String::from_utf8(payload.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode payload as UTF-8: {}", e);
            return results;
        }
    };

    // Always store raw message for audit trail
    results.push(ParsedMessage::RawMessage(RawMessage {
        topic: topic.to_string(),
        payload: payload_str.clone(),
        timestamp: Utc::now(),
    }));

    // Try to parse as JSON
    if let Ok(json) = serde_json::from_str::<Value>(&payload_str) {
        // Parse telemetry readings from flat JSON
        if let Some(readings) = parse_telemetry(topic, &json) {
            results.extend(readings.into_iter().map(ParsedMessage::TelemetryReading));
        }
    }

    debug!("Parsed {} records from topic {}", results.len(), topic);
    results
}

#[derive(Debug)]
pub enum ParsedMessage {
    TelemetryReading(TelemetryReading),
    RawMessage(RawMessage),
}

/// Parse telemetry readings from flat JSON
/// Extracts all numeric fields as separate sensor readings
/// Example: {"temperature": 80, "ph": 2.4} -> 2 readings
fn parse_telemetry(topic: &str, json: &Value) -> Option<Vec<TelemetryReading>> {
    let mut readings = Vec::new();

    // Extract device_id from topic or JSON
    let device_id = extract_device_id(topic, json);

    // Extract timestamp from JSON or use current time
    let timestamp = extract_timestamp(json);

    // Handle flat JSON with numeric values
    if let Some(obj) = json.as_object() {
        for (key, value) in obj {
            // Skip special fields
            if key == "timestamp" || key == "device_id" || key == "ts" {
                continue;
            }

            // Extract numeric values as sensor readings
            if let Some(num) = value.as_f64() {
                readings.push(TelemetryReading {
                    device_id: device_id.clone(),
                    sensor_name: key.clone(),
                    value: num,
                    topic: topic.to_string(),
                    timestamp,
                });
            }
        }
    }

    if readings.is_empty() {
        None
    } else {
        Some(readings)
    }
}

/// Extract device_id from topic or JSON
/// Topic format expected: device/<category>/<device_id> or similar
fn extract_device_id(topic: &str, json: &Value) -> String {
    // Try to get from JSON first
    if let Some(id) = json
        .get("device_id")
        .or_else(|| json.get("deviceId"))
        .or_else(|| json.get("device"))
        .and_then(|v| v.as_str())
    {
        return id.to_string();
    }

    // Extract from topic: device/organ_bath/ob1 -> ob1
    let parts: Vec<&str> = topic.split('/').collect();

    // For topics like "device/<category>/<device_id>", take the last part
    if parts.len() >= 3 && parts[0] == "device" {
        return parts[2].to_string();
    }

    // For other formats, try to find the most specific part
    if parts.len() >= 2 {
        return parts[parts.len() - 1].to_string();
    }

    "unknown".to_string()
}

/// Extract timestamp from JSON or use current time
fn extract_timestamp(json: &Value) -> chrono::DateTime<Utc> {
    if let Some(ts) = json.get("timestamp").or_else(|| json.get("ts")) {
        // Try to parse as ISO8601 string
        if let Some(ts_str) = ts.as_str() {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                return dt.with_timezone(&Utc);
            }
        }

        // Try to parse as Unix timestamp (seconds or milliseconds)
        if let Some(ts_num) = ts.as_i64() {
            // Check if this looks like milliseconds (> year 2100 in seconds)
            if ts_num > 4102444800 {
                let secs = ts_num / 1000;
                let nsecs = ((ts_num % 1000) * 1_000_000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    return dt;
                }
            } else {
                // Seconds
                if let Some(dt) = chrono::DateTime::from_timestamp(ts_num, 0) {
                    return dt;
                }
            }
        }
    }

    Utc::now()
}
