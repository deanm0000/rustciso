//! Shared queue-triggered loader for the OASIS LMP price tables (see `report::REPORTS`).

use crate::azure::{FuncRequest, first_payload};
use crate::oasis::{self, OasisResult};
use crate::report::{Report, local_date};
use crate::{AppState, HandlerResponse, lake, queue, respond, transform};
use anyhow::{Context, Result};
use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ERROR_RETRY: std::time::Duration = std::time::Duration::from_secs(60);
const BACKLOG_ERROR_RETRY: std::time::Duration = std::time::Duration::from_secs(3600);
const BACKLOG_THRESHOLD: u64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    startdatetime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    get_next_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    get_prev_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    repack_if_last: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    repack_if_first: Option<bool>,
    #[serde(flatten)]
    retry: queue::RetryState,
}

impl Job {
    fn continuations(&self, report: &Report, start: DateTime<Utc>) -> Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for (bound, forward) in [(&self.get_next_until, true), (&self.get_prev_until, false)] {
            if let Some(bound) = bound {
                let until = oasis::parse_oasis_time(bound).with_context(|| {
                    if forward {
                        "invalid get_next_until"
                    } else {
                        "invalid get_prev_until"
                    }
                })?;
                let next_start = if forward {
                    report.window_end(start)?
                } else {
                    report.prev_start(start)?
                };
                if (forward && next_start <= until) || (!forward && next_start >= until) {
                    jobs.push(Job {
                        startdatetime: oasis::format_oasis_time(next_start),
                        get_next_until: if forward { Some(bound.clone()) } else { None },
                        get_prev_until: if forward { None } else { Some(bound.clone()) },
                        repack_if_last: self.repack_if_last,
                        repack_if_first: self.repack_if_first,
                        retry: self.retry.fresh(),
                    });
                }
            }
        }
        Ok(jobs)
    }

    fn repack(&self, report: &Report, start: DateTime<Utc>) -> Result<Option<lake::repack::Job>> {
        let date = local_date(start);
        let first = local_date(report.prev_start(start)?) != date;
        let last = local_date(report.window_end(start)?) != date;
        if (self.repack_if_first == Some(true) && first)
            || (self.repack_if_last == Some(true) && last)
        {
            Ok(Some(lake::repack::queued_job(
                &report.table(),
                date,
                date.succ_opt().context("repack end date is out of range")?,
            )))
        } else {
            Ok(None)
        }
    }
}

enum Outcome {
    Loaded,
    NotReady(String),
}

async fn load_window(
    state: &AppState,
    report: &Report,
    start: DateTime<Utc>,
    log: &mut Vec<String>,
) -> Result<Outcome> {
    let end = report.window_end(start)?;
    let csvs = match oasis::fetch(&state.http, &report.query, start, end, log).await? {
        OasisResult::NotReady(msg) => return Ok(Outcome::NotReady(msg)),
        OasisResult::Csvs(csvs) => csvs,
    };
    log.push(format!(
        "downloaded {} csv files ({} bytes)",
        csvs.len(),
        csvs.iter().map(|(_, b)| b.len()).sum::<usize>()
    ));
    let downloaded_at = Utc::now();
    let batches = transform::pivot(&csvs, report.prefix, downloaded_at)
        .await
        .context("pivoting csvs")?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    log.push(format!("pivoted to {rows} rows"));
    let r = lake::replace_window(&state.cfg, &report.table(), batches).await?;
    log.push(format!(
        "{}: snapshot {} added file {} {} ({} rows, {} bytes); retired files {:?} ({} rows); rewritten files {:?}",
        report.table(),
        r.snapshot_id,
        r.data_file_id,
        r.path,
        r.rows,
        r.file_size_bytes,
        r.retired_files,
        r.retired_rows,
        r.rewritten_files
    ));
    Ok(Outcome::Loaded)
}

/// Delay until OASIS is expected to have the window starting at `next_start`, capped at 7 days.
fn next_visibility(
    report: &Report,
    next_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> std::time::Duration {
    (report.ready_at(next_start) - now)
        .to_std()
        .unwrap_or_default()
        .min(queue::MAX_VISIBILITY)
}

async fn enqueue(
    state: &AppState,
    report: &Report,
    job: &Job,
    delay: std::time::Duration,
    what: &str,
    log: &mut Vec<String>,
) -> Result<()> {
    state
        .queue(report)?
        .send(&serde_json::to_string(job)?, delay)
        .await?;
    log.push(format!(
        "{what} {} visible in {}s",
        job.startdatetime,
        delay.as_secs()
    ));
    Ok(())
}

async fn handle(state: &AppState, report: &Report, job: Job, log: &mut Vec<String>) -> Result<()> {
    let start = oasis::parse_oasis_time(&job.startdatetime)?;
    let continuations = job.continuations(report, start)?;
    let repack = job.repack(report, start)?;
    match load_window(state, report, start, log).await {
        Ok(Outcome::Loaded) => {
            for next in continuations {
                let next_start = oasis::parse_oasis_time(&next.startdatetime)?;
                let delay = next_visibility(report, next_start, Utc::now());
                enqueue(state, report, &next, delay, "queued continuation", log).await?;
            }
            if let Some(repack) = repack {
                state
                    .repack_queue
                    .send(
                        &serde_json::to_string(&queue::Job {
                            payload: &repack,
                            retry: job.retry.fresh(),
                        })?,
                        std::time::Duration::ZERO,
                    )
                    .await?;
                log.push(format!(
                    "queued repack of {} for {} America/Los_Angeles",
                    repack.table, repack.begin_date
                ));
            }
        }
        Ok(Outcome::NotReady(msg)) => {
            log.push(format!("data not ready ({msg})"));
            if is_historical(start, Utc::now()) {
                log.push(format!(
                    "stopping historical no-data job {} (America/Los_Angeles); no continuation queued",
                    job.startdatetime
                ));
            } else {
                enqueue(state, report, &job, report.not_ready_retry, "requeued", log).await?;
            }
        }
        Err(e) => {
            log.push(format!("error loading {}: {e:#}", job.startdatetime));
            let backlog = match state.queue(report)?.approximate_count().await {
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
            state
                .queue(report)?
                .retry_or_poison(
                    &job,
                    job.retry,
                    delay,
                    &format!("error loading {}: {e:#}", job.startdatetime),
                    log,
                )
                .await?;
        }
    }
    Ok(())
}

fn is_historical(start: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    local_date(start) < local_date(now)
}

fn parse_job(report: &Report, req: Result<Json<FuncRequest>, JsonRejection>) -> Result<Job> {
    let job: Job = req
        .map_err(|e| anyhow::anyhow!("{e}"))
        .and_then(|Json(r)| first_payload(r))
        .and_then(|v| {
            serde_json::from_value(v).context(
                "payload must look like {\"startdatetime\":\"20261016T07:00-0000\",\"get_next_until\":\"20261017T07:00-0000\",\"get_prev_until\":\"20261015T07:00-0000\",\"repack_if_last\":true,\"repack_if_first\":true}",
            )
        })?;
    report.validate_start(oasis::parse_oasis_time(&job.startdatetime)?)?;
    Ok(job)
}

pub async fn handler(
    state: Arc<AppState>,
    report: &'static Report,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let mut log = Vec::new();
    let job = match parse_job(report, req) {
        Ok(j) => j,
        Err(e) => {
            log.push(format!("bad {} request: {e:#}", report.name));
            return respond(StatusCode::BAD_REQUEST, log);
        }
    };
    log.push(format!("{} job {job:?}", report.name));
    match handle(&state, report, job, &mut log).await {
        Ok(()) => respond(StatusCode::OK, log),
        Err(e) => {
            // Couldn't write to the queue: fail so the Functions host redelivers the original.
            log.push(format!("failed: {e:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

pub fn route(report: &'static Report) -> axum::routing::MethodRouter<Arc<AppState>> {
    axum::routing::post(
        move |State(state): State<Arc<AppState>>, req: Result<Json<FuncRequest>, JsonRejection>| {
            handler(state, report, req)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{DAPRICES, FMPRICES, RTPRICES};
    use chrono::Duration;
    use chrono_tz::America::Los_Angeles;

    fn job(start: &str) -> Job {
        Job {
            startdatetime: start.to_string(),
            get_next_until: None,
            get_prev_until: None,
            repack_if_last: None,
            repack_if_first: None,
            retry: queue::RetryState::default(),
        }
    }

    #[test]
    fn continuation_bounds_are_inclusive_and_compare_instants() {
        let mut j = job("20261016T07:00-0000");
        let start = oasis::parse_oasis_time(&j.startdatetime).unwrap();
        assert!(j.continuations(&RTPRICES, start).unwrap().is_empty());

        j.get_next_until = Some("20261016T01:00-0700".into());
        j.get_prev_until = Some("20261015T23:00-0700".into());
        let children = j.continuations(&RTPRICES, start).unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].startdatetime, "20261016T08:00-0000");
        assert_eq!(children[1].startdatetime, "20261016T06:00-0000");
        for child in children {
            let child_start = oasis::parse_oasis_time(&child.startdatetime).unwrap();
            assert!(
                child
                    .continuations(&RTPRICES, child_start)
                    .unwrap()
                    .is_empty()
            );
        }

        j.get_next_until = Some("20261016T07:59-0000".into());
        j.get_prev_until = Some("20261016T06:01-0000".into());
        assert!(j.continuations(&RTPRICES, start).unwrap().is_empty());
        j.get_next_until = Some("20261015T07:00-0000".into());
        j.get_prev_until = Some("20261017T07:00-0000".into());
        assert!(j.continuations(&RTPRICES, start).unwrap().is_empty());
    }

    #[test]
    fn children_keep_direction_and_repack_flags_until_bounds() {
        let mut j = job("20261016T07:00-0000");
        j.get_next_until = Some("20261016T10:00-0000".into());
        j.get_prev_until = Some("20261016T04:00-0000".into());
        for report in [&RTPRICES, &FMPRICES] {
            for flags in [
                (Some(true), Some(false)),
                (Some(false), Some(true)),
                (Some(true), Some(true)),
            ] {
                j.repack_if_last = flags.0;
                j.repack_if_first = flags.1;
                let start = oasis::parse_oasis_time(&j.startdatetime).unwrap();
                let children = j.continuations(report, start).unwrap();
                assert_eq!(children.len(), 2);
                assert_eq!(children[0].get_next_until, j.get_next_until);
                assert_eq!(children[0].get_prev_until, None);
                assert_eq!(children[1].get_prev_until, j.get_prev_until);
                assert_eq!(children[1].get_next_until, None);
                for mut child in children {
                    for step in 1..=3 {
                        assert_eq!(child.repack_if_last, flags.0);
                        assert_eq!(child.repack_if_first, flags.1);
                        let encoded = serde_json::to_string(&child).unwrap();
                        child = serde_json::from_str(&encoded).unwrap();
                        let child_start = oasis::parse_oasis_time(&child.startdatetime).unwrap();
                        let mut next = child.continuations(report, child_start).unwrap();
                        if step == 3 {
                            assert!(next.is_empty());
                        } else {
                            assert_eq!(next.len(), 1);
                            child = next.pop().unwrap();
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn daily_continuations_step_local_days_across_dst() {
        let mut j = job("20260307T08:00-0000");
        j.get_next_until = Some("20260310T07:00-0000".into());
        let mut starts = Vec::new();
        let mut current = j;
        loop {
            starts.push(current.startdatetime.clone());
            let start = oasis::parse_oasis_time(&current.startdatetime).unwrap();
            DAPRICES.validate_start(start).unwrap();
            match current.continuations(&DAPRICES, start).unwrap().pop() {
                Some(next) => current = next,
                None => break,
            }
        }
        assert_eq!(
            starts,
            [
                "20260307T08:00-0000",
                "20260308T08:00-0000",
                "20260309T07:00-0000",
                "20260310T07:00-0000"
            ]
        );

        let mut j = job("20261102T08:00-0000");
        j.get_prev_until = Some("20261031T07:00-0000".into());
        let start = oasis::parse_oasis_time(&j.startdatetime).unwrap();
        let prev = j.continuations(&DAPRICES, start).unwrap();
        assert_eq!(prev[0].startdatetime, "20261101T07:00-0000");
    }

    #[test]
    fn daily_jobs_repack_their_own_day() {
        let start = oasis::parse_oasis_time("20261101T07:00-0000").unwrap();
        for (first, last, expected) in [
            (None, None, false),
            (Some(true), None, true),
            (None, Some(true), true),
            (Some(false), Some(false), false),
        ] {
            let mut j = job("20261101T07:00-0000");
            j.repack_if_first = first;
            j.repack_if_last = last;
            let repack = j.repack(&DAPRICES, start).unwrap();
            assert_eq!(repack.is_some(), expected);
            if let Some(repack) = repack {
                repack.validate().unwrap();
                assert_eq!(repack.table.to_string(), "ciso.daprices");
                assert_eq!(repack.begin_date.to_string(), "2026-11-01");
                assert_eq!(repack.end_date.to_string(), "2026-11-02");
            }
        }
    }

    #[test]
    fn invalid_continuation_bounds_are_errors() {
        let mut j = job("20261016T07:00-0000");
        let start = oasis::parse_oasis_time(&j.startdatetime).unwrap();
        j.get_next_until = Some("invalid".into());
        assert!(
            j.continuations(&RTPRICES, start)
                .unwrap_err()
                .to_string()
                .contains("get_next_until")
        );
        j.get_next_until = None;
        j.get_prev_until = Some("invalid".into());
        assert!(
            j.continuations(&RTPRICES, start)
                .unwrap_err()
                .to_string()
                .contains("get_prev_until")
        );
    }

    #[test]
    fn repack_uses_local_day_boundaries_including_dst() {
        for report in [&RTPRICES, &FMPRICES] {
            for (first, end, hours) in [
                ("20261016T07:00-0000", "20261017T07:00-0000", 24),
                ("20260308T08:00-0000", "20260309T07:00-0000", 23),
                ("20261101T07:00-0000", "20261102T08:00-0000", 25),
                ("20261231T08:00-0000", "20270101T08:00-0000", 24),
            ] {
                let start = oasis::parse_oasis_time(first).unwrap();
                let end = oasis::parse_oasis_time(end).unwrap();
                assert_eq!((end - start).num_hours(), hours);
                for first_flag in [None, Some(false), Some(true)] {
                    for last_flag in [None, Some(false), Some(true)] {
                        let mut j = job(first);
                        j.repack_if_first = first_flag;
                        j.repack_if_last = last_flag;
                        for hour in 0..hours {
                            let repack = j.repack(report, start + Duration::hours(hour)).unwrap();
                            let expected = (first_flag == Some(true) && hour == 0)
                                || (last_flag == Some(true) && hour == hours - 1);
                            assert_eq!(repack.is_some(), expected, "{first} hour {hour}");
                            if let Some(repack) = repack {
                                repack.validate().unwrap();
                                let payload = serde_json::to_value(&repack).unwrap();
                                assert_eq!(
                                    payload,
                                    serde_json::json!({
                                        "table": report.table(),
                                        "begin_date": start.with_timezone(&Los_Angeles).date_naive().to_string(),
                                        "end_date": end.with_timezone(&Los_Angeles).date_naive().to_string(),
                                    })
                                );
                                let decoded: lake::repack::Job =
                                    serde_json::from_value(payload).unwrap();
                                assert_eq!(decoded.table.to_string(), report.table());
                                assert_eq!(
                                    decoded.begin_date,
                                    start.with_timezone(&Los_Angeles).date_naive()
                                );
                                assert_eq!(
                                    decoded.end_date,
                                    end.with_timezone(&Los_Angeles).date_naive()
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn next_visibility_waits_for_publication() {
        let next = oasis::parse_oasis_time("20261016T08:00-0000").unwrap();
        let now = oasis::parse_oasis_time("20261016T08:30-0000").unwrap();
        assert_eq!(next_visibility(&RTPRICES, next, now).as_secs(), 35 * 60);
        assert_eq!(next_visibility(&FMPRICES, next, now).as_secs(), 35 * 60);
        let much_later = oasis::parse_oasis_time("20261020T00:00-0000").unwrap();
        assert_eq!(next_visibility(&RTPRICES, next, much_later).as_secs(), 0);
        let long_before = oasis::parse_oasis_time("20261001T00:00-0000").unwrap();
        assert_eq!(
            next_visibility(&RTPRICES, next, long_before),
            queue::MAX_VISIBILITY
        );

        let day = oasis::parse_oasis_time("20261017T07:00-0000").unwrap();
        let noon = oasis::parse_oasis_time("20261016T19:00-0000").unwrap();
        assert_eq!(next_visibility(&DAPRICES, day, noon).as_secs(), 2 * 3600);
    }

    #[test]
    fn rejects_daily_jobs_not_at_local_midnight() {
        let req = |start: &str| -> Result<Json<FuncRequest>, JsonRejection> {
            Ok(Json(
                serde_json::from_value(serde_json::json!({
                    "Data": {"daprices": serde_json::json!({"startdatetime": start}).to_string()},
                    "Metadata": {"sys": {"MethodName": "daprices", "UtcNow": "", "RandGuid": ""}}
                }))
                .unwrap(),
            ))
        };
        assert!(parse_job(&DAPRICES, req("20261016T07:00-0000")).is_ok());
        let err = parse_job(&DAPRICES, req("20261016T08:00-0000")).unwrap_err();
        assert!(err.to_string().contains("midnight"));
        assert!(parse_job(&FMPRICES, req("20261016T08:00-0000")).is_ok());
    }

    #[test]
    fn job_roundtrip() {
        let j: Job = serde_json::from_str(r#"{"startdatetime":"20261016T07:00-0000"}"#).unwrap();
        assert_eq!(j.get_next_until, None);
        assert_eq!(j.get_prev_until, None);
        assert_eq!(j.repack_if_last, None);
        assert_eq!(j.repack_if_first, None);
        assert_eq!(
            serde_json::to_string(&j).unwrap(),
            r#"{"startdatetime":"20261016T07:00-0000"}"#
        );
    }

    #[test]
    fn continuations_reset_retries_and_inherit_limit() {
        let mut job = job("20261016T07:00-0000");
        job.get_next_until = Some("20261017T07:00-0000".into());
        job.get_prev_until = Some("20261015T07:00-0000".into());
        job.retry = queue::RetryState {
            retries: Some(2),
            max_retries: Some(5),
        };
        let encoded = serde_json::to_value(&job).unwrap();
        assert_eq!(encoded["retries"], 2);
        assert_eq!(encoded["max_retries"], 5);
        let decoded: Job = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.retry, job.retry);
        let start = oasis::parse_oasis_time(&job.startdatetime).unwrap();
        for report in [&RTPRICES, &FMPRICES, &DAPRICES] {
            let children = job.continuations(report, start).unwrap();
            assert_eq!(children.len(), 2);
            for child in children {
                assert_eq!(child.retry.retries, None);
                assert_eq!(child.retry.max_retries, Some(5));
            }
        }
    }

    #[test]
    fn no_data_is_historical_only_before_current_pacific_date() {
        let now = oasis::parse_oasis_time("20261021T06:30-0000").unwrap();
        for (start, historical) in [
            ("20261020T06:00-0000", true),
            ("20261020T07:00-0000", false),
            ("20261021T06:00-0000", false),
            ("20261021T07:00-0000", false),
        ] {
            assert_eq!(
                is_historical(oasis::parse_oasis_time(start).unwrap(), now),
                historical
            );
        }
        let after_midnight = oasis::parse_oasis_time("20261021T07:00-0000").unwrap();
        assert!(is_historical(now, after_midnight));
    }
}
