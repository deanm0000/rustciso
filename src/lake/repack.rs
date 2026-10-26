use crate::config::Config;
use anyhow::{Context, Result, bail};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::Los_Angeles;
use dkplus::maintenance::PotentialRepack;
use dkplus::repack::{Bound, Strictness};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashSet;

pub use dkplus::repack::Job;

#[derive(Debug, Clone)]
pub enum RepackPayload {
    Job(Job),
    Maintenance(PotentialRepack),
}

impl Serialize for RepackPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Job(job) => job.serialize(serializer),
            Self::Maintenance(plan) => plan.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RepackPayload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let mut value = Value::deserialize(deserializer)?;
        let is_plan = value
            .as_object()
            .is_some_and(|object| object.contains_key("level"));
        if is_plan {
            serde_json::from_value(value)
                .map(Self::Maintenance)
                .map_err(serde::de::Error::custom)
        } else {
            normalize_legacy_dates(&mut value).map_err(serde::de::Error::custom)?;
            serde_json::from_value(value)
                .map(Self::Job)
                .map_err(serde::de::Error::custom)
        }
    }
}

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

fn normalize_legacy_dates(payload: &mut Value) -> Result<()> {
    let Some(object) = payload.as_object_mut() else {
        bail!("repack payload must be an object");
    };
    for (legacy, current) in [("begin_date", "begin"), ("end_date", "end")] {
        let Some(value) = object.remove(legacy) else {
            continue;
        };
        if object.contains_key(current) {
            bail!("{legacy} cannot be combined with {current}");
        }
        let date: NaiveDate = serde_json::from_value(value)
            .with_context(|| format!("{legacy} must be a date in YYYY-MM-DD format"))?;
        let local_midnight = date.and_time(NaiveTime::MIN);
        let bound = Los_Angeles
            .from_local_datetime(&local_midnight)
            .earliest()
            .with_context(|| format!("no Los Angeles midnight for {date}"))?
            .with_timezone(&Utc);
        object.insert(current.into(), serde_json::to_value(Bound::Tz(bound))?);
    }
    Ok(())
}

pub fn queued_job(table: &str, begin_date: NaiveDate, end_date: NaiveDate) -> Result<Job> {
    let (schema, name) = table.split_once('.').unwrap_or((super::SCHEMA, table));
    let mut table = dkplus::Table::default();
    table.schema = schema.into();
    table.table = name.into();
    let bound = |date: NaiveDate| {
        Los_Angeles
            .from_local_datetime(&date.and_time(NaiveTime::MIN))
            .earliest()
            .map(|time| Bound::Tz(time.with_timezone(&Utc)))
            .with_context(|| format!("no Los Angeles midnight for {date}"))
    };
    Ok(table
        .repack_between(bound(begin_date)?, bound(end_date)?)
        .with_expire_snapshot(dkplus::repack::ExpireSnapshot::Never))
}

pub fn validate_request(job: &Job) -> Result<()> {
    if [&job.table.schema, &job.table.table].iter().any(|part| {
        part.is_empty() || !part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    }) {
        bail!("table components must use letters, digits and underscores");
    }
    if job.begin.utc() >= job.end.utc() {
        bail!("begin must precede end (exclusive)");
    }
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

pub fn validate_plan(plan: &PotentialRepack) -> Result<()> {
    let (schema, name) = plan
        .table
        .split_once('.')
        .ok_or_else(|| anyhow::anyhow!("maintenance table must be schema-qualified"))?;
    if [schema, name]
        .iter()
        .any(|part| part.is_empty() || part.contains(['\0', '.']))
    {
        bail!("maintenance table components must be nonempty and contain no NUL or dot");
    }
    if plan.begin.utc() >= plan.end.utc() {
        bail!("begin must precede end (exclusive)");
    }
    if plan.files.is_empty() {
        bail!("maintenance plan must include at least one source file");
    }
    let mut seen = HashSet::new();
    if plan
        .files
        .iter()
        .any(|file| file.is_empty() || !seen.insert(file))
    {
        bail!("maintenance source files must be nonempty and distinct");
    }
    Ok(())
}

pub async fn run(cfg: &Config, payload: &RepackPayload, log: &mut Vec<String>) -> Result<()> {
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
    let result = async {
        match payload {
            RepackPayload::Job(job) => {
                let mut job = job.clone();
                job.table = super::resolve_table(&lake_config, &job.table.to_string()).await?;
                job.validate()?;
                job.run().await
            }
            RepackPayload::Maintenance(plan) => {
                let table = lake_config.table(&plan.table).await?;
                match dkplus::maintenance::do_repack(&table, plan, Strictness::Raise).await? {
                    dkplus::maintenance::RepackOutcome::Repacked => {
                        log.push(format!("maintenance repack completed for {}", plan.table));
                        Ok(())
                    }
                    dkplus::maintenance::RepackOutcome::Skipped(reason) => {
                        log.push(format!(
                            "maintenance repack skipped for {}: {reason}",
                            plan.table
                        ));
                        Ok(())
                    }
                }
            }
        }
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
    fn legacy_date_payloads_become_los_angeles_bounds() {
        let payload: RepackPayload = serde_json::from_value(json!({
            "table": "rtprices",
            "begin_date": "2026-03-08",
            "end_date": "2026-03-09"
        }))
        .unwrap();
        let RepackPayload::Job(job) = payload else {
            panic!("expected a regular repack job");
        };
        assert_eq!(job.table.to_string(), "main.rtprices");
        assert_eq!(job.begin.utc().to_rfc3339(), "2026-03-08T08:00:00+00:00");
        assert_eq!(job.end.utc().to_rfc3339(), "2026-03-09T07:00:00+00:00");
    }

    #[test]
    fn canonical_and_legacy_bound_fields_cannot_be_mixed() {
        assert!(
            serde_json::from_value::<RepackPayload>(json!({
                "table": "ciso.rtprices",
                "begin_date": "2026-01-01",
                "begin": {"tz": "2026-01-01T08:00:00Z"},
                "end_date": "2026-01-02"
            }))
            .is_err()
        );
    }

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
        )
        .unwrap();
        validate_request(&job).unwrap();

        let invalid = queued_job(
            "ciso.rtprices",
            "2026-10-17".parse().unwrap(),
            "2026-10-16".parse().unwrap(),
        )
        .unwrap();
        assert!(validate_request(&invalid).is_err());
    }
}
