use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct FuncRequest {
    #[serde(default)]
    pub Data: HashMap<String, Value>,
    #[serde(default)]
    #[allow(dead_code)]
    pub Metadata: HashMap<String, Value>,
}

pub fn request_id(req: &FuncRequest) -> Option<String> {
    req.Metadata.get("Id").map(|value| match value {
        Value::String(id) => id.clone(),
        other => other.to_string(),
    })
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
pub struct OutResponse {
    pub Outputs: Option<Value>,
    pub Logs: Option<Vec<String>>,
    pub ReturnValue: Option<String>,
}

/// Queue payloads may arrive as an object or as (possibly repeatedly) stringified JSON.
pub fn normalize_payload(value: Value) -> anyhow::Result<Value> {
    let mut v = value;
    for _ in 0..5 {
        match v {
            Value::String(s) => v = serde_json::from_str(&s)?,
            _ => break,
        }
    }
    Ok(v)
}

/// The Data key is the binding name; don't depend on it, take the first entry.
pub fn first_payload(req: FuncRequest) -> anyhow::Result<Value> {
    let payload = req
        .Data
        .into_values()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no data in request body"))?;
    normalize_payload(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn handles_double_encoded_payload() {
        let inner = json!({
            "startdatetime": "20261016T07:00-0000",
            "get_next_until": "20261017T07:00-0000"
        })
        .to_string();
        let twice = serde_json::to_string(&inner).unwrap();
        let req = FuncRequest {
            Data: HashMap::from([("rtprices".into(), Value::String(twice))]),
            Metadata: HashMap::new(),
        };
        let v = first_payload(req).unwrap();
        assert_eq!(v["startdatetime"], "20261016T07:00-0000");
    }

    #[test]
    fn reads_metadata_id() {
        let req = FuncRequest {
            Data: HashMap::new(),
            Metadata: HashMap::from([("Id".into(), Value::String("invocation-123".into()))]),
        };
        assert_eq!(request_id(&req).as_deref(), Some("invocation-123"));
    }
}
