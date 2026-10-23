use crate::config::Config;
use anyhow::{Result, bail};
use chrono::{Months, NaiveDate};
use serde_json::Value;
use std::collections::HashSet;

pub use dkplus::repack::Job;

pub fn qualify_unqualified_table(payload: &mut Value) {
    let Some(table) = payload
        .as_object_mut()
        .and_then(|object| object.get_mut("table"))
    else {
        return;
    };
    let Some(name) = table.as_str() else {
        return;
    };
    if !name.contains('.') {
        *table = Value::String(format!("{}.{}", super::SCHEMA, name));
    }
}

pub fn queued_job(table: &str, begin_date: NaiveDate, end_date: NaiveDate) -> Job {
    let (schema, name) = table.split_once('.').unwrap_or((super::SCHEMA, table));
    let mut table = dkplus::Table::default();
    table.schema = schema.into();
    table.table = name.into();
    table
        .repack(begin_date, end_date)
        .with_expire_snapshot(dkplus::repack::ExpireSnapshot::Never)
}

pub fn validate_request(job: &Job) -> Result<()> {
    if [&job.table.schema, &job.table.table].iter().any(|part| {
        part.is_empty() || !part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    }) {
        bail!("table components must use letters, digits and underscores");
    }
    if job.begin_date >= job.end_date {
        bail!("begin_date must precede end_date (exclusive)");
    }
    job.begin_date
        .checked_add_months(Months::new(1))
        .ok_or_else(|| anyhow::anyhow!("date is out of range"))?;
    if let Some(unique) = &job.unique {
        if unique.is_empty() {
            bail!("unique must contain at least one column");
        }
        let mut seen = HashSet::new();
        for name in unique {
            if name.is_empty() || !seen.insert(name) {
                bail!("unique columns must be nonempty and distinct");
            }
        }
    }
    Ok(())
}

pub async fn run(cfg: &Config, job: &Job, log: &mut Vec<String>) -> Result<()> {
    let mut job = job.clone();
    let library_log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured_log = library_log.clone();
    let mut lake_config = cfg.lake();
    lake_config.log = std::sync::Arc::new(move |message| {
        eprintln!("{message}");
        match captured_log.lock() {
            Ok(mut messages) => messages.push(message.to_owned()),
            Err(error) => eprintln!("could not capture dkplus log message: {error}"),
        }
    });
    job.table = super::resolve_table(&lake_config, &job.table.to_string()).await?;
    let result = async {
        job.validate()?;
        job.run().await
    }
    .await;
    match library_log.lock() {
        Ok(mut messages) => log.append(&mut messages),
        Err(error) => eprintln!("could not collect dkplus logs: {error}"),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unqualified_repack_tables_default_to_ciso() {
        let mut payload = json!({"table": "rtprices"});
        qualify_unqualified_table(&mut payload);
        assert_eq!(payload["table"], "ciso.rtprices");

        let mut payload = json!({"table": "other.manual_table"});
        qualify_unqualified_table(&mut payload);
        assert_eq!(payload["table"], "other.manual_table");
    }

    #[test]
    fn validates_queue_job_fields_before_catalog_resolution() {
        let job = queued_job(
            "ciso.rtprices",
            "2026-10-16".parse().unwrap(),
            "2026-10-17".parse().unwrap(),
        );
        validate_request(&job).unwrap();

        let invalid = queued_job(
            "ciso.rtprices",
            "2026-10-17".parse().unwrap(),
            "2026-10-16".parse().unwrap(),
        );
        assert!(validate_request(&invalid).is_err());
    }
}
