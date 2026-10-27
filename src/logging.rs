use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct InvocationLog {
    invocation_id: Option<String>,
    lines: Arc<Mutex<Vec<String>>>,
}

impl InvocationLog {
    pub fn new(invocation_id: Option<String>) -> Self {
        Self {
            invocation_id,
            lines: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn push(&self, message: impl AsRef<str>) {
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let line = match &self.invocation_id {
            Some(id) => format!("{timestamp} invocation_id={id}\n{}", message.as_ref()),
            None => format!("{timestamp}\n{}", message.as_ref()),
        };
        eprintln!("{line}");
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(line);
    }

    pub fn json<T: Serialize>(label: &str, value: &T) -> anyhow::Result<String> {
        Ok(format!("{label} {}", serde_json::to_string(value)?))
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

pub fn write_startup(message: impl AsRef<str>) {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    eprintln!("{timestamp} {}", message.as_ref());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_lines_include_timestamp_and_invocation_id() {
        let log = InvocationLog::new(Some("invocation-123".into()));
        log.push("started");
        let line = log.lines().pop().unwrap();
        assert!(line.starts_with("20"));
        assert!(line.contains("T"));
        assert!(line.contains("Z invocation_id=invocation-123 started"));
    }

    #[test]
    fn structs_are_logged_as_json() {
        let value = serde_json::json!({"table": "ciso.rtprices"});
        assert_eq!(
            InvocationLog::json("job", &value).unwrap(),
            "job {\"table\":\"ciso.rtprices\"}"
        );
    }
}
