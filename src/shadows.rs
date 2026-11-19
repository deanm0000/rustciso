//! Queue ingestion for the two partitioned shadow-price tables.

#[cfg(test)]
mod tests;
mod transform;

use crate::azure::{FuncRequest, first_payload};
use crate::logging::InvocationLog;
use crate::oasis::{self, OasisResult, month_start, shift_month};
use crate::{AppState, lake, queue, respond};
use anyhow::{Context, Result, bail, ensure};
use axum::Json;
use axum::extract::{State, rejection::JsonRejection};
use axum::http::StatusCode;
use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Report {
    Nomogram,
    Intertie,
}

pub const REPORTS: [Report; 2] = [Report::Nomogram, Report::Intertie];

pub fn is_table(schema: &str, name: &str) -> bool {
    schema == lake::SCHEMA && REPORTS.iter().any(|report| report.name() == name)
}

impl Report {
    pub fn name(self) -> &'static str {
        match self {
            Self::Nomogram => "nomogram_shadows",
            Self::Intertie => "intertie_shadows",
        }
    }

    pub fn queue_name(self) -> &'static str {
        match self {
            Self::Nomogram => "nomogram-shadows",
            Self::Intertie => "intertie-shadows",
        }
    }

    fn table(self) -> String {
        format!("ciso.{}", self.name())
    }

    fn url(self, market: Market, window: Window) -> String {
        let (query, version, filters) = match (self, market) {
            (Self::Nomogram, Market::Interval) => ("PRC_RTM_NOMOGRAM", "1", "nomogram_id=ALL"),
            (Self::Nomogram, _) => ("PRC_NOMOGRAM", "13", "nomogram_id=ALL"),
            (Self::Intertie, Market::Interval) => {
                ("PRC_RTM_FLOWGATE", "12", "ti_id=ALL&ti_direction=ALL")
            }
            (Self::Intertie, _) => ("PRC_CNSTR", "13", "ti_id=ALL&ti_direction=ALL"),
        };
        format!(
            "{}?resultformat=6&queryname={query}&version={version}&market_run_id={}&{filters}&startdatetime={}&enddatetime={}",
            oasis::OASIS_URL,
            market.upstream(),
            oasis::format_oasis_time(window.start),
            oasis::format_oasis_time(window.end),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", try_from = "String")]
pub enum Market {
    Dam,
    Hasp,
    Rtm,
    Ruc,
    Interval,
}

impl TryFrom<String> for Market {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "dam" => Ok(Self::Dam),
            "hasp" => Ok(Self::Hasp),
            "rtm" => Ok(Self::Rtm),
            "ruc" => Ok(Self::Ruc),
            "interval" => Ok(Self::Interval),
            _ => bail!("invalid market_run_id {value:?}; expected dam, hasp, rtm, ruc or interval"),
        }
    }
}

impl Market {
    fn name(self) -> &'static str {
        match self {
            Self::Dam => "dam",
            Self::Hasp => "hasp",
            Self::Rtm => "rtm",
            Self::Ruc => "ruc",
            Self::Interval => "interval",
        }
    }

    fn upstream(self) -> &'static str {
        match self {
            Self::Dam => "DAM",
            Self::Hasp => "HASP",
            Self::Rtm | Self::Interval => "RTM",
            Self::Ruc => "RUC",
        }
    }

    fn tick(self) -> Duration {
        Duration::minutes(match self {
            Self::Rtm => 15,
            Self::Interval => 5,
            _ => 60,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    startdatetime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stopdatetime: Option<String>,
    market_run_id: Market,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    get_next_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    get_prev_until: Option<String>,
}

type Job = queue::Job<Payload>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Payload {
    fn window(&self) -> Result<Window> {
        let start = oasis::parse_oasis_time(&self.startdatetime)?;
        let end = match &self.stopdatetime {
            Some(stop) => oasis::parse_oasis_time(stop).context("invalid stopdatetime")?,
            None => shift_month(month_start(start)?, true)?,
        };
        ensure!(start < end, "stopdatetime must be after startdatetime");
        ensure!(
            end - start <= Duration::days(31),
            "query window exceeds 31 days"
        );
        let tick = self.market_run_id.tick().num_seconds();
        ensure!(
            start.timestamp().rem_euclid(tick) == 0 && end.timestamp().rem_euclid(tick) == 0,
            "startdatetime and stopdatetime must align to {}-minute intervals",
            self.market_run_id.tick().num_minutes()
        );
        for (name, bound) in [
            ("get_next_until", &self.get_next_until),
            ("get_prev_until", &self.get_prev_until),
        ] {
            if let Some(bound) = bound {
                oasis::parse_oasis_time(bound).with_context(|| format!("invalid {name}"))?;
            }
        }
        Ok(Window { start, end })
    }

    fn continuations(&self, window: Window, observed: Option<Window>) -> Result<Vec<Payload>> {
        let mut jobs = Vec::new();
        let covered = observed.unwrap_or(window);
        if let Some(bound) = &self.get_next_until {
            let next = covered.end;
            if next <= oasis::parse_oasis_time(bound)? {
                ensure!(next > window.start, "forward continuation did not advance");
                jobs.push(Payload {
                    startdatetime: oasis::format_oasis_time(next),
                    stopdatetime: None,
                    market_run_id: self.market_run_id,
                    get_next_until: Some(bound.clone()),
                    get_prev_until: None,
                });
            }
        }
        if let Some(bound) = &self.get_prev_until {
            let first = month_start(window.start)?;
            let (start, end) = if first == window.start {
                (shift_month(first, false)?, window.start)
            } else {
                (first, covered.start)
            };
            if start >= oasis::parse_oasis_time(bound)? && start < end {
                jobs.push(Payload {
                    startdatetime: oasis::format_oasis_time(start),
                    stopdatetime: Some(oasis::format_oasis_time(end)),
                    market_run_id: self.market_run_id,
                    get_next_until: None,
                    get_prev_until: Some(bound.clone()),
                });
            }
        }
        Ok(jobs)
    }
}

fn current_threshold(now: DateTime<Utc>) -> DateTime<Utc> {
    let lagged = now - Duration::minutes(5);
    lagged
        .with_minute(0)
        .and_then(|time| time.with_second(0))
        .and_then(|time| time.with_nanosecond(0))
        .expect("zero minute/second/nanosecond are valid")
}

fn visibility(start: DateTime<Utc>, now: DateTime<Utc>) -> std::time::Duration {
    (start + Duration::minutes(65) - now)
        .to_std()
        .unwrap_or_default()
        .min(queue::MAX_VISIBILITY)
}

async fn enqueue(
    state: &AppState,
    report: Report,
    job: &Job,
    delay: std::time::Duration,
    log: &InvocationLog,
) -> Result<()> {
    state
        .queue_named(report.name())?
        .send(&serde_json::to_string(job)?, delay)
        .await?;
    log.push(format!(
        "queued {} {} visible in {}s",
        report.name(),
        job.payload.startdatetime,
        delay.as_secs()
    ));
    Ok(())
}

async fn load_window(
    state: &AppState,
    report: Report,
    payload: &Payload,
    window: Window,
    log: &InvocationLog,
) -> Result<Option<Window>> {
    let csvs = match oasis::fetch_url(&state.http, &report.url(payload.market_run_id, window), log)
        .await?
    {
        OasisResult::NotReady(message) => {
            log.push(format!("no data: {message}"));
            return Ok(None);
        }
        OasisResult::Csvs(csvs) => csvs,
    };
    let data = transform::normalize(&csvs, report, payload.market_run_id, window, Utc::now())
        .await
        .context("normalizing shadow prices")?;
    let Some(observed) = data.observed else {
        log.push("no rows in downloaded shadow-price CSVs");
        return Ok(None);
    };
    let table = lake::resolve_table(&state.cfg.lake(log.clone()), &report.table()).await?;
    let loaded = table
        .replace_partition_window(data.batches, "market_type", payload.market_run_id.name())
        .await?;
    log.push(format!(
        "{} market {}: snapshot {}, {} rows, {} bytes, retired files {:?}, rewritten files {:?}",
        report.table(),
        payload.market_run_id.name(),
        loaded.snapshot_id,
        loaded.rows,
        loaded.file_size_bytes,
        loaded.retired_files,
        loaded.rewritten_files,
    ));
    Ok(Some(observed))
}

async fn handle(state: &AppState, report: Report, job: Job, log: &InvocationLog) -> Result<()> {
    let window = job.payload.window()?;
    match load_window(state, report, &job.payload, window, log).await {
        Ok(observed) => {
            complete_window(state, report, job, window, observed, Utc::now(), log).await
        }
        Err(error) => {
            let reason = format!("error loading {}: {error:#}", report.name());
            log.push(&reason);
            state
                .queue_named(report.name())?
                .retry_or_poison(
                    &job,
                    job.retry,
                    std::time::Duration::from_secs(60),
                    &reason,
                    log,
                )
                .await
        }
    }
}

async fn complete_window(
    state: &AppState,
    report: Report,
    job: Job,
    window: Window,
    observed: Option<Window>,
    now: DateTime<Utc>,
    log: &InvocationLog,
) -> Result<()> {
    if observed.is_none() && window.start > current_threshold(now) {
        log.push("current/future no-data window; retrying the same startdatetime");
        return enqueue(state, report, &job, std::time::Duration::from_secs(60), log).await;
    }
    if observed.is_none() {
        log.push("empty historical window; advancing using requested bounds");
    }
    for payload in job.payload.continuations(window, observed)? {
        let start = oasis::parse_oasis_time(&payload.startdatetime)?;
        let delay = visibility(start, now);
        enqueue(
            state,
            report,
            &Job {
                payload,
                retry: job.retry.fresh(),
            },
            delay,
            log,
        )
        .await?;
    }
    Ok(())
}

fn parse_job(req: Result<Json<FuncRequest>, JsonRejection>) -> Result<Job> {
    let job: Job = serde_json::from_value(first_payload(
        req.map_err(|error| anyhow::anyhow!("{error}"))?.0,
    )?)
    .context("expected startdatetime, optional stopdatetime and market_run_id")?;
    job.payload.window()?;
    Ok(job)
}

pub fn route(report: Report) -> axum::routing::MethodRouter<Arc<AppState>> {
    axum::routing::post(
        move |State(state): State<Arc<AppState>>, req: Result<Json<FuncRequest>, JsonRejection>| async move {
            let log = InvocationLog::new(
                req.as_ref()
                    .ok()
                    .and_then(|Json(req)| crate::azure::request_id(req)),
            );
            let job = match parse_job(req) {
                Ok(job) => job,
                Err(error) => {
                    log.push(format!("bad {} request: {error:#}", report.name()));
                    return respond(StatusCode::BAD_REQUEST, log);
                }
            };
            match InvocationLog::json(&format!("{} job", report.name()), &job) {
                Ok(message) => log.push(message),
                Err(error) => {
                    log.push(format!("could not serialize job: {error:#}"));
                    return respond(StatusCode::INTERNAL_SERVER_ERROR, log);
                }
            }
            match handle(&state, report, job, &log).await {
                Ok(()) => respond(StatusCode::OK, log),
                Err(error) => {
                    log.push(format!("failed: {error:#}"));
                    respond(StatusCode::INTERNAL_SERVER_ERROR, log)
                }
            }
        },
    )
}
