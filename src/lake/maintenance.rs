//! Finds dkplus maintenance plans and delivers them to the repack worker.

use crate::config::Config;
use crate::logging::InvocationLog;
use crate::queue::QueueClient;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use dkplus::connect;
use dkplus::maintenance::{CleanupOptions, CleanupScope, PotentialRepack};
use std::time::Duration;

const TABLES_SQL: &str = "
    SELECT s.schema_name, t.table_name
    FROM ducklake.ducklake_table t
    JOIN ducklake.ducklake_schema s USING (schema_id)
    WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL
    ORDER BY s.schema_name, t.table_name";

fn cleanup_options(now: DateTime<Utc>) -> CleanupOptions {
    CleanupOptions {
        scope: CleanupScope::Catalog,
        older_than: now - chrono::Duration::hours(12),
        dry_run: false,
    }
}

pub async fn cleanup(cfg: &Config, log: &InvocationLog) -> Result<()> {
    let report = dkplus::maintenance::cleanup_expired_files(
        &cfg.lake(log.clone()),
        &cleanup_options(Utc::now()),
    )
    .await
    .context("cleaning up expired catalog files")?;
    log.push(format!(
        "expired-file cleanup: {} candidates, {} deleted, {} already missing",
        report.files.len(),
        report.deleted,
        report.already_missing
    ));
    Ok(())
}

async fn enqueue(
    queue: &QueueClient,
    plans: &[PotentialRepack],
    log: &InvocationLog,
) -> Result<()> {
    for plan in plans {
        let plan_json = serde_json::to_string(plan)?;
        queue
            .send(&plan_json, Duration::ZERO)
            .await
            .with_context(|| format!("enqueueing maintenance repack {plan_json}"))?;
        let message = format!(
            "queued maintenance repack of {} [{}, {})",
            plan.table, plan.begin, plan.end
        );
        log.push(message);
    }
    Ok(())
}

fn skip_missing_settings<T>(
    result: dkplus::Result<T>,
    log: &InvocationLog,
) -> dkplus::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error @ dkplus::Error::MissingRequiredSetting { .. }) => {
            let message = format!("skipping maintenance table: {error}");
            log.push(message);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub async fn run(cfg: &Config, queue: &QueueClient, log: &InvocationLog) -> Result<()> {
    let lake_config = cfg.lake(log.clone());
    let client = connect(&lake_config).await?;
    let tables = client
        .query(TABLES_SQL, &[])
        .await
        .context("listing active catalog tables")?;
    drop(client);
    let mut count = 0;
    for row in tables {
        let schema: String = row.try_get(0)?;
        let name: String = row.try_get(1)?;
        let table_name = format!("{schema}.{name}");
        let Some(table) = skip_missing_settings(lake_config.table(&table_name).await, log)
            .with_context(|| format!("resolving maintenance table {table_name}"))?
        else {
            continue;
        };
        let Some(plans) = skip_missing_settings(table.find_potential_repacks().await, log)
            .with_context(|| format!("finding maintenance repacks for {table_name}"))?
        else {
            continue;
        };
        log.push(format!(
            "{}: found {} maintenance repacks",
            table,
            plans.len()
        ));
        enqueue(queue, &plans, log).await?;
        count += plans.len();
    }
    log.push(format!("maintenance queued {count} repack jobs"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageAccount;
    use axum::{Router, routing::post};
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use dkplus::maintenance::{Granularity, Interval, RepackLevel};
    use dkplus::repack::Bound;
    use serde_json::json;

    fn plan(table: &str, begin: &str, end: &str, file: &str) -> PotentialRepack {
        PotentialRepack {
            table: table.into(),
            level: RepackLevel::new(1),
            granularity: Granularity::Fixed(Interval::minutes(5)),
            begin: Bound::Tz(begin.parse().unwrap()),
            end: Bound::Tz(end.parse().unwrap()),
            files: vec![file.into()],
        }
    }

    #[test]
    fn missing_settings_are_logged_and_skipped() {
        for setting in ["time_column", "timezone", "sort_cols", "granularity"] {
            let log = InvocationLog::default();
            let result = skip_missing_settings::<()>(
                Err(dkplus::Error::MissingRequiredSetting {
                    table: "abb.energy".into(),
                    setting,
                }),
                &log,
            )
            .unwrap();
            assert_eq!(result, None);
            assert!(log.lines()[0].contains(&format!(
                "skipping maintenance table: abb.energy: missing required setting {setting}"
            )));
            assert_eq!(skip_missing_settings(Ok(42), &log).unwrap(), Some(42));
            assert_eq!(log.lines().len(), 1);
        }
    }

    #[test]
    fn other_errors_remain_failures_with_context() {
        let errors = [
            dkplus::Error::InvalidConfiguration {
                table: "abb.energy".into(),
                reason: "configured time column does not exist".into(),
            },
            dkplus::Error::InvalidData {
                reason: "missing required statistics".into(),
            },
            dkplus::Error::Io {
                operation: "connecting to catalog".into(),
                source: std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "connection refused",
                ),
            },
        ];
        for error in errors {
            let expected = error.to_string();
            let log = InvocationLog::default();
            let error = skip_missing_settings::<()>(Err(error), &log)
                .context("resolving maintenance table abb.energy")
                .unwrap_err();
            assert!(log.lines().is_empty());
            assert_eq!(error.to_string(), "resolving maintenance table abb.energy");
            assert_eq!(
                error.downcast_ref::<dkplus::Error>().unwrap().to_string(),
                expected
            );
        }
    }

    #[test]
    fn planner_runs_hourly_at_ten_after_without_startup_execution() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../../planRepacks/function.json")).unwrap();
        assert_eq!(
            config["bindings"],
            json!([{
                "name": "planRepacks",
                "type": "timerTrigger",
                "direction": "in",
                "schedule": "0 10 * * * *",
                "runOnStartup": false,
                "useMonitor": true
            }])
        );
    }

    #[test]
    fn cleanup_runs_daily_at_eight_thirty_utc_with_twelve_hour_cutoff() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../../cleanupExpiredFiles/function.json")).unwrap();
        assert_eq!(
            config["bindings"],
            json!([{
                "name": "cleanupExpiredFiles",
                "type": "timerTrigger",
                "direction": "in",
                "schedule": "0 30 8 * * *",
                "runOnStartup": false,
                "useMonitor": true
            }])
        );
        let now = "2026-10-26T08:30:00Z".parse().unwrap();
        let options = cleanup_options(now);
        assert!(matches!(options.scope, CleanupScope::Catalog));
        assert!(!options.dry_run);
        assert_eq!(options.older_than, now - chrono::Duration::hours(12));
    }

    #[test]
    fn maintenance_worker_uses_its_dedicated_queue() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../../maintenanceRepack/function.json")).unwrap();
        assert_eq!(
            config["bindings"],
            json!([{
                "name": "maintenanceRepack",
                "type": "queueTrigger",
                "direction": "in",
                "queueName": "maintenance-repack",
                "connection": "AzureWebJobsStorage"
            }])
        );
    }

    #[tokio::test]
    async fn enqueues_maintenance_plans_and_surfaces_queue_errors() {
        let plans = [
            plan(
                "ciso.daprices",
                "2026-01-01T08:00:00Z",
                "2026-02-01T08:00:00Z",
                "da-month.parquet",
            ),
            plan(
                "ciso.rtprices",
                "2026-03-01T08:00:00Z",
                "2026-04-01T07:00:00Z",
                "rt-month.parquet",
            ),
            plan(
                "ciso.fmprices",
                "2026-11-01T07:00:00Z",
                "2026-12-01T08:00:00Z",
                "fm-month.parquet",
            ),
        ];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/maintenance-repack/messages",
            post(move |uri: axum::http::Uri, body: String| {
                let send = send.clone();
                async move {
                    send.send((uri, body)).unwrap();
                    axum::http::StatusCode::CREATED
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let account = StorageAccount {
            name: "test".into(),
            key: B64.encode("key"),
            queue_endpoint: format!("http://{address}"),
        };
        let queue = QueueClient::new(
            reqwest::Client::new(),
            account.clone(),
            crate::MAINTENANCE_REPACK_QUEUE_NAME,
        );
        let log = InvocationLog::default();
        enqueue(&queue, &[], &log).await.unwrap();
        assert!(receive.try_recv().is_err());
        enqueue(&queue, &plans, &log).await.unwrap();
        for expected in &plans {
            let (uri, body) = receive.try_recv().unwrap();
            assert_eq!(uri.query(), Some("messagettl=-1&visibilitytimeout=0"));
            let encoded = body
                .strip_prefix("<QueueMessage><MessageText>")
                .unwrap()
                .strip_suffix("</MessageText></QueueMessage>")
                .unwrap();
            let decoded = B64.decode(encoded).unwrap();
            let payload: crate::lake::repack::RepackPayload =
                serde_json::from_slice(&decoded).unwrap();
            let crate::lake::repack::RepackPayload::Maintenance(actual) = payload else {
                panic!("expected a maintenance plan");
            };
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
        assert_eq!(log.lines().len(), plans.len());
        assert!(receive.try_recv().is_err());
        let failed_queue = QueueClient::new(reqwest::Client::new(), account, "missing");
        let error = enqueue(&failed_queue, &plans, &log).await.unwrap_err();
        assert!(format!("{error:#}").contains("HTTP 404"));
        assert_eq!(log.lines().len(), plans.len());
        server.abort();
    }
}
