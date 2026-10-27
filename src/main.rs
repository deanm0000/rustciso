mod azure;
mod config;
mod lake;
mod logging;
mod oasis;
mod prices;
mod queue;
mod report;
mod transform;

use crate::azure::{FuncRequest, OutResponse, first_payload, request_id};
use crate::config::Config;
use crate::logging::InvocationLog;
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
const MAINTENANCE_REPACK_QUEUE_NAME: &str = "maintenance-repack";

struct AppState {
    cfg: Config,
    http: reqwest::Client,
    /// One queue per report, named after it.
    queues: HashMap<&'static str, QueueClient>,
    repack_queue: QueueClient,
    maintenance_repack_queue: QueueClient,
}

impl AppState {
    fn queue(&self, report: &Report) -> Result<&QueueClient> {
        self.queues
            .get(report.name)
            .with_context(|| format!("no queue configured for {}", report.name))
    }
}

type HandlerResponse = (StatusCode, Json<OutResponse>);

fn respond(status: StatusCode, log: InvocationLog) -> HandlerResponse {
    (
        status,
        Json(OutResponse {
            Outputs: None,
            Logs: Some(log.lines()),
            ReturnValue: None,
        }),
    )
}

async fn repack_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let log = InvocationLog::new(
        req.as_ref()
            .ok()
            .and_then(|Json(request)| request_id(request)),
    );
    let job = req
        .map_err(|e| anyhow::anyhow!("{e}"))
        .and_then(|Json(r)| first_payload(r))
        .and_then(|mut v| {
            lake::repack::qualify_unqualified_table(&mut v);
            serde_json::from_value::<queue::Job<lake::repack::RepackPayload>>(v)
                .context("payload must contain a table and either begin/end bounds or legacy begin_date/end_date")
        })
        .and_then(|job| {
            match &job.payload {
                lake::repack::RepackPayload::Job(payload) => {
                    lake::repack::validate_request(payload)?
                }
                lake::repack::RepackPayload::Maintenance(plan) => {
                    lake::repack::validate_plan(plan)?
                }
            }
            Ok(job)
        });
    let job = match job {
        Ok(job) => job,
        Err(e) => {
            return respond(StatusCode::BAD_REQUEST, {
                log.push(format!("bad repack request: {e:#}"));
                log
            });
        }
    };
    execute_repack(&state, job, &state.repack_queue, log).await
}

fn maintenance_job(req: FuncRequest) -> Result<queue::Job<lake::repack::RepackPayload>> {
    let job: queue::Job<dkplus::maintenance::PotentialRepack> =
        serde_json::from_value(first_payload(req)?).context("payload must be a PotentialRepack")?;
    lake::repack::validate_plan(&job.payload)?;
    Ok(queue::Job {
        payload: lake::repack::RepackPayload::Maintenance(job.payload),
        retry: job.retry,
    })
}

async fn maintenance_repack_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let log = InvocationLog::new(
        req.as_ref()
            .ok()
            .and_then(|Json(request)| request_id(request)),
    );
    let job = req
        .map_err(|e| anyhow::anyhow!("{e}"))
        .and_then(|Json(req)| maintenance_job(req));
    match job {
        Ok(job) => execute_repack(&state, job, &state.maintenance_repack_queue, log).await,
        Err(e) => {
            log.push(format!("bad maintenance repack request: {e:#}"));
            respond(StatusCode::BAD_REQUEST, log)
        }
    }
}

async fn execute_repack(
    state: &AppState,
    job: queue::Job<lake::repack::RepackPayload>,
    queue: &QueueClient,
    log: InvocationLog,
) -> HandlerResponse {
    match InvocationLog::json("repack job", &job) {
        Ok(message) => log.push(message),
        Err(error) => {
            log.push(format!(
                "failed to serialize repack job for logging: {error:#}"
            ));
            return respond(StatusCode::INTERNAL_SERVER_ERROR, log);
        }
    }
    match lake::repack::run(&state.cfg, &job.payload, &log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            if matches!(
                e.downcast_ref::<dkplus::Error>(),
                Some(dkplus::Error::LockUnavailable { .. })
            ) {
                match requeue_contended_job(queue, &job, &log).await {
                    Ok(()) => {
                        return respond(StatusCode::OK, log);
                    }
                    Err(error) => {
                        log.push(format!(
                            "failed to requeue repack after lock contention: {error:#}"
                        ));
                        return respond(StatusCode::INTERNAL_SERVER_ERROR, log);
                    }
                }
            }
            log.push(format!("repack failed: {e:#}"));
            match queue
                .retry_or_poison(
                    &job,
                    job.retry,
                    std::time::Duration::from_secs(60),
                    &format!("repack failed: {e:#}"),
                    &log,
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

async fn requeue_contended_job(
    queue: &QueueClient,
    job: &impl serde::Serialize,
    log: &InvocationLog,
) -> Result<()> {
    let payload = serde_json::to_string(job)?;
    queue
        .send(&payload, std::time::Duration::from_secs(600))
        .await?;
    log.push("repack lock is busy; requeued the same job for 600 seconds");
    Ok(())
}

async fn plan_repacks_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let log = InvocationLog::new(
        req.as_ref()
            .ok()
            .and_then(|Json(request)| request_id(request)),
    );
    if let Err(e) = req {
        log.push(format!("bad maintenance request: {e}"));
        return respond(StatusCode::BAD_REQUEST, log);
    }
    log.push("starting table maintenance");
    match lake::maintenance::run(&state.cfg, &state.maintenance_repack_queue, &log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            log.push(format!("maintenance failed: {e:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

async fn cleanup_expired_files_handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let log = InvocationLog::new(
        req.as_ref()
            .ok()
            .and_then(|Json(request)| request_id(request)),
    );
    if let Err(e) = req {
        log.push(format!("bad expired-file cleanup request: {e}"));
        return respond(StatusCode::BAD_REQUEST, log);
    }
    log.push("starting expired-file cleanup");
    match lake::maintenance::cleanup(&state.cfg, &log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            log.push(format!("expired-file cleanup failed: {e:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

fn router(state: Arc<AppState>) -> Router {
    let mut app = Router::new()
        .route("/repack", post(repack_handler))
        .route("/planRepacks", post(plan_repacks_handler))
        .route("/maintenanceRepack", post(maintenance_repack_handler))
        .route("/cleanupExpiredFiles", post(cleanup_expired_files_handler));
    for report in REPORTS {
        app = app.route(&format!("/{}", report.name), prices::route(report));
    }
    app.with_state(state)
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
    let maintenance_repack_queue = QueueClient::new(
        http.clone(),
        cfg.queue_account.clone(),
        MAINTENANCE_REPACK_QUEUE_NAME,
    );
    let state = Arc::new(AppState {
        cfg,
        http,
        queues,
        repack_queue,
        maintenance_repack_queue,
    });

    let app = router(state);
    let port: u16 = std::env::var("FUNCTIONS_CUSTOMHANDLER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    crate::logging::write_startup(format!("rustciso listening on {port}"));
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use dkplus::maintenance::{Granularity, Interval, PotentialRepack, RepackLevel};
    use dkplus::repack::Bound;
    use serde_json::{Value, json};

    fn plan() -> PotentialRepack {
        PotentialRepack {
            table: "weather.hourly-readings".into(),
            level: RepackLevel::new(1),
            granularity: Granularity::Fixed(Interval::hours(1)),
            begin: Bound::Tz("2026-10-25T00:00:00Z".parse().unwrap()),
            end: Bound::Tz("2026-10-26T00:00:00Z".parse().unwrap()),
            files: vec!["one.parquet".into(), "two.parquet".into()],
        }
    }

    fn invocation(payload: Value) -> FuncRequest {
        FuncRequest {
            Data: HashMap::from([("anyBindingName".into(), payload)]),
            Metadata: HashMap::new(),
        }
    }

    #[test]
    fn maintenance_worker_parses_plans_and_preserves_retry_state() {
        let mut payload = serde_json::to_value(plan()).unwrap();
        payload["retries"] = json!(2);
        payload["max_retries"] = json!(5);
        let encoded = Value::String(serde_json::to_string(&payload.to_string()).unwrap());
        let job = maintenance_job(invocation(encoded)).unwrap();
        assert_eq!(job.retry.retries, Some(2));
        assert_eq!(job.retry.max_retries, Some(5));
        let lake::repack::RepackPayload::Maintenance(actual) = job.payload else {
            panic!("expected a maintenance plan");
        };
        assert_eq!(actual, plan());

        for change in [
            json!({"table": "unqualified"}),
            json!({"table": "weather."}),
            json!({"files": []}),
            json!({"files": ["one.parquet", "one.parquet"]}),
            json!({"end": {"tz": "2026-10-25T00:00:00Z"}}),
            json!({"retries": -1}),
        ] {
            let mut invalid = payload.clone();
            for (key, value) in change.as_object().unwrap() {
                invalid[key] = value.clone();
            }
            assert!(maintenance_job(invocation(invalid)).is_err());
        }
        assert!(
            maintenance_job(invocation(json!({
                "table": "ciso.rtprices",
                "begin_date": "2026-10-25",
                "end_date": "2026-10-26"
            })))
            .is_err()
        );
    }

    #[tokio::test]
    async fn trigger_routes_return_structured_errors_and_retry_on_the_correct_queue() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let account = config::StorageAccount {
            name: "test".into(),
            key: B64.encode("key"),
            queue_endpoint: format!("http://{address}"),
        };
        let http = reqwest::Client::new();
        // A bound but non-listening local port guarantees catalog connection failure.
        let unavailable = tokio::net::TcpSocket::new_v4().unwrap();
        unavailable.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = unavailable.local_addr().unwrap().port();
        let mut postgres = tokio_postgres::Config::new();
        postgres.host("127.0.0.1").port(port).user("test");
        let lake_cfg = dkplus::Table::default().cfg;
        let state = Arc::new(AppState {
            cfg: Config {
                postgres,
                lake_account: account.clone(),
                queue_account: account.clone(),
                repack_run_bytes: lake_cfg.repack_run_bytes,
                repack_node_bytes: lake_cfg.repack_node_bytes,
            },
            http: http.clone(),
            queues: HashMap::new(),
            repack_queue: QueueClient::new(http.clone(), account.clone(), REPACK_QUEUE_NAME),
            maintenance_repack_queue: QueueClient::new(
                http.clone(),
                account,
                MAINTENANCE_REPACK_QUEUE_NAME,
            ),
        });
        let contended_queue = state.maintenance_repack_queue.clone();
        let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let mut app = router(state);
        for name in [REPACK_QUEUE_NAME, MAINTENANCE_REPACK_QUEUE_NAME] {
            let send = send.clone();
            app = app.route(
                &format!("/{name}/messages"),
                post(move |uri: axum::http::Uri, body: String| {
                    let send = send.clone();
                    async move {
                        send.send((uri, body)).unwrap();
                        StatusCode::CREATED
                    }
                }),
            );
        }
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        for path in [
            "planRepacks",
            "maintenanceRepack",
            "cleanupExpiredFiles",
            "repack",
            "rtprices",
            "fmprices",
            "daprices",
        ] {
            let response = http
                .post(format!("http://{address}/{path}"))
                .header("content-type", "application/json")
                .body("{")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
            assert!(body["Outputs"].is_null());
            assert!(body["ReturnValue"].is_null());
            assert!(!body["Logs"].as_array().unwrap().is_empty());
        }
        let response = http
            .post(format!("http://{address}/maintenance"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        for path in ["planRepacks", "cleanupExpiredFiles"] {
            let response = http
                .post(format!("http://{address}/{path}"))
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        }
        for (path, queue, payload) in [
            (
                "maintenanceRepack",
                MAINTENANCE_REPACK_QUEUE_NAME,
                serde_json::to_value(plan()).unwrap(),
            ),
            (
                "repack",
                REPACK_QUEUE_NAME,
                json!({
                    "table": "ciso.rtprices",
                    "begin_date": "2026-10-25",
                    "end_date": "2026-10-26"
                }),
            ),
        ] {
            let invocation_id = format!("invocation-{path}");
            let response = http
                .post(format!("http://{address}/{path}"))
                .header("content-type", "application/json")
                .body(
                    json!({
                        "Data": {"input": payload},
                        "Metadata": {"Id": invocation_id.clone()}
                    })
                    .to_string(),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let response_body: Value =
                serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
            let logs = response_body["Logs"].as_array().unwrap();
            assert!(logs.iter().any(|entry| {
                entry
                    .as_str()
                    .is_some_and(|line| line.contains(&format!("invocation_id={invocation_id}")))
            }));
            assert!(logs.iter().all(|entry| {
                entry.as_str().is_some_and(|line| {
                    line.split_whitespace()
                        .next()
                        .is_some_and(|timestamp| {
                            chrono::DateTime::parse_from_rfc3339(timestamp).is_ok()
                        })
                })
            }));
            let (uri, body) = receive.try_recv().unwrap();
            assert_eq!(uri.path(), format!("/{queue}/messages"));
            assert!(uri.query().unwrap().contains("visibilitytimeout=60"));
            let encoded = body
                .strip_prefix("<QueueMessage><MessageText>")
                .unwrap()
                .strip_suffix("</MessageText></QueueMessage>")
                .unwrap();
            let retried: Value = serde_json::from_slice(&B64.decode(encoded).unwrap()).unwrap();
            assert_eq!(retried["retries"], 1);
            assert_eq!(retried["table"], payload["table"]);
            if path == "maintenanceRepack" {
                let actual = maintenance_job(invocation(retried)).unwrap();
                let lake::repack::RepackPayload::Maintenance(actual) = actual.payload else {
                    panic!("expected a retried maintenance plan");
                };
                assert_eq!(actual, plan());
            }
        }
        assert!(receive.try_recv().is_err());
        let deferred = json!({"table": "ciso.rtprices", "retries": 2, "max_retries": 5});
        let deferred_log = InvocationLog::new(Some("invocation-lock-busy".into()));
        requeue_contended_job(&contended_queue, &deferred, &deferred_log)
            .await
            .unwrap();
        let (uri, body) = receive.try_recv().unwrap();
        assert_eq!(uri.path(), "/maintenance-repack/messages");
        assert!(uri.query().unwrap().contains("visibilitytimeout=600"));
        let encoded = body
            .strip_prefix("<QueueMessage><MessageText>")
            .unwrap()
            .strip_suffix("</MessageText></QueueMessage>")
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&B64.decode(encoded).unwrap()).unwrap(),
            deferred
        );
        assert!(deferred_log.lines()[0].contains("invocation_id=invocation-lock-busy"));
        server.abort();
    }
}
