mod azure;
mod config;
mod lake;
mod oasis;
mod prices;
mod queue;
mod report;
mod transform;

use crate::azure::{FuncRequest, OutResponse, first_payload};
use crate::config::Config;
use crate::queue::QueueClient;
use crate::report::{REPORTS, Report};
use anyhow::{Context, Result};
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use std::collections::HashMap;
use std::sync::Arc;

const REPACK_QUEUE_NAME: &str = "repack";

struct AppState {
    cfg: Config,
    http: reqwest::Client,
    /// One queue per report, named after it.
    queues: HashMap<&'static str, QueueClient>,
    repack_queue: QueueClient,
}

impl AppState {
    fn queue(&self, report: &Report) -> Result<&QueueClient> {
        self.queues
            .get(report.name)
            .with_context(|| format!("no queue configured for {}", report.name))
    }
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

async fn repack_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let job = req
        .map_err(|e| anyhow::anyhow!("{e}"))
        .and_then(|Json(r)| first_payload(r))
        .and_then(|mut v| {
            lake::repack::qualify_unqualified_table(&mut v);
            serde_json::from_value::<queue::Job<lake::repack::Job>>(v)
                .context("payload must contain table, begin_date and end_date (YYYY-MM-DD)")
        })
        .and_then(|job| {
            lake::repack::validate_request(&job.payload)?;
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
    eprintln!("{}", log[0]);
    match lake::repack::run(&state.cfg, &job.payload, &mut log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            log.push(format!("repack failed: {e:#}"));
            match state
                .repack_queue
                .retry_or_poison(
                    &job,
                    job.retry,
                    std::time::Duration::from_secs(60),
                    &format!("repack failed: {e:#}"),
                    &mut log,
                )
                .await
            {
                Ok(()) => respond(StatusCode::OK, log),
                Err(error) => {
                    log.push(format!("failed to retry or poison repack: {error:#}"));
                    respond(StatusCode::INTERNAL_SERVER_ERROR, log)
                }
            }
        }
    }
}

async fn maintenance_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    if let Err(e) = req {
        return respond(
            StatusCode::BAD_REQUEST,
            vec![format!("bad maintenance request: {e}")],
        );
    }
    let mut log = vec!["starting table maintenance".to_string()];
    match lake::maintenance::run(&state.cfg, &state.repack_queue, &mut log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            log.push(format!("maintenance failed: {e:#}"));
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
    let queues = REPORTS
        .iter()
        .map(|m| {
            (
                m.name,
                QueueClient::new(http.clone(), cfg.queue_account.clone(), m.name),
            )
        })
        .collect();
    let repack_queue = QueueClient::new(http.clone(), cfg.queue_account.clone(), REPACK_QUEUE_NAME);
    let state = Arc::new(AppState {
        cfg,
        http,
        queues,
        repack_queue,
    });

    let mut app = Router::new()
        .route("/repack", post(repack_handler))
        .route("/maintenance", post(maintenance_handler));
    for report in REPORTS {
        app = app.route(&format!("/{}", report.name), prices::route(report));
    }
    let app = app.with_state(state);
    let port: u16 = std::env::var("FUNCTIONS_CUSTOMHANDLER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    eprintln!("rustciso listening on {port}");
    axum::serve(listener, app).await?;
    Ok(())
}
