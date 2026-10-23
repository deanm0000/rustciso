//! Finds dkplus maintenance plans and delivers them to the repack worker.

use crate::config::Config;
use crate::queue::QueueClient;
use crate::report::REPORTS;
use anyhow::{Context, Result};
use dkplus::connect;
use dkplus::maintenance::PotentialRepack;
use std::time::Duration;
use tokio_postgres::GenericClient;

async fn table_plans(
    client: &impl GenericClient,
    table: &dkplus::Table,
) -> Result<Vec<PotentialRepack>> {
    dkplus::maintenance::find_potential_repacks(client, table).await
}

async fn enqueue(
    queue: &QueueClient,
    plans: &[PotentialRepack],
    log: &mut Vec<String>,
) -> Result<()> {
    for plan in plans {
        queue
            .send(&serde_json::to_string(plan)?, Duration::ZERO)
            .await
            .with_context(|| format!("enqueueing maintenance repack {plan:?}"))?;
        let message = format!(
            "queued maintenance repack of {} [{}, {})",
            plan.table, plan.begin, plan.end
        );
        eprintln!("{message}");
        log.push(message);
    }
    Ok(())
}

pub async fn run(cfg: &Config, queue: &QueueClient, log: &mut Vec<String>) -> Result<()> {
    let lake_config = cfg.lake();
    let mut client = connect(&lake_config).await?;
    let mut plans = Vec::new();
    for report in REPORTS {
        let table = super::resolve_table(&lake_config, &report.table()).await?;
        let tx = client.transaction().await?;
        let found = table_plans(&tx, &table).await?;
        log.push(format!(
            "{}: found {} maintenance repacks",
            report.table(),
            found.len()
        ));
        plans.extend(found);
        tx.commit()
            .await
            .context("closing maintenance planning transaction")?;
    }
    enqueue(queue, &plans, log).await?;
    log.push(format!("maintenance queued {} repack jobs", plans.len()));
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
    fn timer_runs_every_thirty_minutes_without_startup_execution() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../../maintenance/function.json")).unwrap();
        assert_eq!(
            config["bindings"],
            json!([{
                "name": "maintenance",
                "type": "timerTrigger",
                "direction": "in",
                "schedule": "0 */30 * * * *",
                "runOnStartup": false,
                "useMonitor": true
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
            "/repack/messages",
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
        let queue = QueueClient::new(reqwest::Client::new(), account.clone(), "repack");
        let mut log = Vec::new();
        enqueue(&queue, &[], &mut log).await.unwrap();
        assert!(receive.try_recv().is_err());
        enqueue(&queue, &plans, &mut log).await.unwrap();
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
        assert_eq!(log.len(), plans.len());
        assert!(receive.try_recv().is_err());
        let failed_queue = QueueClient::new(reqwest::Client::new(), account, "missing");
        let error = enqueue(&failed_queue, &plans, &mut log).await.unwrap_err();
        assert!(format!("{error:#}").contains("HTTP 404"));
        assert_eq!(log.len(), plans.len());
        server.abort();
    }
}
