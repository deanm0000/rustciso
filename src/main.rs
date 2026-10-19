mod azure;
mod config;
mod lake;
mod oasis;
mod queue;
mod transform;

use crate::azure::{FuncRequest, OutResponse, first_payload};
use crate::config::Config;
use crate::oasis::OasisResult;
use crate::queue::QueueClient;
use anyhow::{Context, Result};
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const QUEUE_NAME: &str = "rtprices";
const NOT_READY_RETRY: std::time::Duration = std::time::Duration::from_secs(60);
const ERROR_RETRY: std::time::Duration = std::time::Duration::from_secs(60);
const BACKLOG_ERROR_RETRY: std::time::Duration = std::time::Duration::from_secs(3600);
const BACKLOG_THRESHOLD: u64 = 5;
/// Grace period after an hour closes before OASIS is expected to have it.
const READY_LAG: Duration = Duration::minutes(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    startdatetime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    get_next: Option<bool>,
}

struct AppState {
    cfg: Config,
    http: reqwest::Client,
    queue: QueueClient,
}

enum Outcome {
    Loaded,
    NotReady(String),
}

type HandlerResponse = (StatusCode, Json<OutResponse>);

fn respond(status: StatusCode, log: Vec<String>) -> HandlerResponse {
    for line in &log {
        eprintln!("{line}");
    }
    (
        status,
        Json(OutResponse {
            Outputs: None,
            Logs: Some(log),
            ReturnValue: None,
        }),
    )
}

async fn load_hour(
    state: &AppState,
    start: DateTime<Utc>,
    log: &mut Vec<String>,
) -> Result<Outcome> {
    let csvs = match oasis::fetch(&state.http, start, log).await? {
        OasisResult::NotReady(msg) => return Ok(Outcome::NotReady(msg)),
        OasisResult::Csvs(csvs) => csvs,
    };
    log.push(format!(
        "downloaded {} csv files ({} bytes)",
        csvs.len(),
        csvs.iter().map(|(_, b)| b.len()).sum::<usize>()
    ));
    let downloaded_at = Utc::now();
    let batches = transform::pivot(&csvs, downloaded_at)
        .await
        .context("pivoting csvs")?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    log.push(format!("pivoted to {rows} rows"));
    let r = lake::replace_window(&state.cfg, start, start + oasis::WINDOW, batches).await?;
    log.push(format!(
        "{}: snapshot {} added file {} {} ({} rows, {} bytes); retired files {:?} ({} rows){}",
        lake::TABLE,
        r.snapshot_id,
        r.data_file_id,
        r.path,
        r.rows,
        r.file_size_bytes,
        r.retired_files,
        r.retired_rows,
        r.sql_deleted
            .map(|n| format!("; pg_ducklake deleted {n} rows"))
            .unwrap_or_default()
    ));
    Ok(Outcome::Loaded)
}

/// Delay until the hour after `next_start` has closed plus a grace period, capped at 7 days.
fn next_visibility(next_start: DateTime<Utc>, now: DateTime<Utc>) -> std::time::Duration {
    let ready_at = next_start + oasis::WINDOW + READY_LAG;
    (ready_at - now)
        .to_std()
        .unwrap_or_default()
        .min(queue::MAX_VISIBILITY)
}

async fn requeue(
    state: &AppState,
    job: &Job,
    delay: std::time::Duration,
    log: &mut Vec<String>,
) -> Result<()> {
    state
        .queue
        .send(&serde_json::to_string(job)?, delay)
        .await?;
    log.push(format!(
        "requeued {} visible in {}s",
        job.startdatetime,
        delay.as_secs()
    ));
    Ok(())
}

async fn handle(state: &AppState, job: Job, log: &mut Vec<String>) -> Result<()> {
    let start = oasis::parse_oasis_time(&job.startdatetime)?;
    match load_hour(state, start, log).await {
        Ok(Outcome::Loaded) => {
            if job.get_next.unwrap_or(false) {
                let next_start = start + oasis::WINDOW;
                let next = Job {
                    startdatetime: oasis::format_oasis_time(next_start),
                    get_next: Some(true),
                };
                let delay = next_visibility(next_start, Utc::now());
                state
                    .queue
                    .send(&serde_json::to_string(&next)?, delay)
                    .await?;
                log.push(format!(
                    "queued next {} visible in {}s",
                    next.startdatetime,
                    delay.as_secs()
                ));
            }
        }
        Ok(Outcome::NotReady(msg)) => {
            log.push(format!("data not ready ({msg})"));
            requeue(state, &job, NOT_READY_RETRY, log).await?;
        }
        Err(e) => {
            log.push(format!("error loading {}: {e:#}", job.startdatetime));
            let backlog = match state.queue.approximate_count().await {
                Ok(n) => n,
                Err(qe) => {
                    log.push(format!("could not read queue length: {qe:#}"));
                    0
                }
            };
            log.push(format!("queue has ~{backlog} messages"));
            let delay = if backlog >= BACKLOG_THRESHOLD {
                BACKLOG_ERROR_RETRY
            } else {
                ERROR_RETRY
            };
            requeue(state, &job, delay, log).await?;
        }
    }
    Ok(())
}

async fn rtprices_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let mut log = Vec::new();
    let job: Result<Job> = req
        .map_err(|e| anyhow::anyhow!("{e}"))
        .and_then(|Json(r)| first_payload(r))
        .and_then(|v| {
            serde_json::from_value(v).context(
                "payload must look like {\"startdatetime\":\"20261016T07:00-0000\",\"get_next\":true}",
            )
        });
    let job = match job {
        Ok(j) => j,
        Err(e) => {
            log.push(format!("bad request: {e:#}"));
            return respond(StatusCode::BAD_REQUEST, log);
        }
    };
    log.push(format!("job {job:?}"));
    match handle(&state, job, &mut log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            // Couldn't write to the queue: fail so the Functions host redelivers the original.
            log.push(format!("failed: {e:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

async fn repack_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let job = req
        .map_err(|e| anyhow::anyhow!("{e}"))
        .and_then(|Json(r)| first_payload(r))
        .and_then(|v| {
            serde_json::from_value::<lake::repack::Job>(v)
                .context("payload must contain table, begin_date and end_date (YYYY-MM-DD)")
        })
        .and_then(|job| {
            job.validate()?;
            Ok(job)
        });
    let job = match job {
        Ok(job) => job,
        Err(e) => {
            return respond(
                StatusCode::BAD_REQUEST,
                vec![format!("bad repack request: {e:#}")],
            );
        }
    };
    let mut log = vec![format!("repack job {job:?}")];
    match lake::repack::run(&state.cfg, &job, &mut log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            log.push(format!("repack failed: {e:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cfg = Config::from_env()?;
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(180))
        .build()?;
    let queue = QueueClient::new(http.clone(), cfg.queue_account.clone(), QUEUE_NAME);
    let state = Arc::new(AppState { cfg, http, queue });

    let app = Router::new()
        .route("/rtprices", post(rtprices_handler))
        .route("/repack", post(repack_handler))
        .with_state(state);
    let port: u16 = std::env::var("FUNCTIONS_CUSTOMHANDLER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    eprintln!("rustciso listening on {port}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_visibility_waits_for_hour_close() {
        let next = oasis::parse_oasis_time("20261016T08:00-0000").unwrap();
        let now = oasis::parse_oasis_time("20261016T08:30-0000").unwrap();
        assert_eq!(next_visibility(next, now).as_secs(), 35 * 60);
        let much_later = oasis::parse_oasis_time("20261020T00:00-0000").unwrap();
        assert_eq!(next_visibility(next, much_later).as_secs(), 0);
        let long_before = oasis::parse_oasis_time("20261001T00:00-0000").unwrap();
        assert_eq!(next_visibility(next, long_before), queue::MAX_VISIBILITY);
    }

    #[test]
    fn job_roundtrip() {
        let j: Job = serde_json::from_str(r#"{"startdatetime":"20261016T07:00-0000"}"#).unwrap();
        assert_eq!(j.get_next, None);
        assert_eq!(
            serde_json::to_string(&j).unwrap(),
            r#"{"startdatetime":"20261016T07:00-0000"}"#
        );
    }
}
