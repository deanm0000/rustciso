use crate::azure::{FuncRequest, first_payload, request_id};
use crate::logging::InvocationLog;
use crate::{AppState, HandlerResponse, lake, queue, respond};
use anyhow::{Context, Result, ensure};
use axum::Json;
use axum::extract::{State, rejection::JsonRejection};
use axum::http::StatusCode;
use bytes::Bytes;
use chrono::{DateTime, LocalResult, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::America::Los_Angeles;
use datafusion::arrow::{
    array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Cursor, sync::Arc, time::Duration};

pub const NAME: &str = "supply";
const TABLE: &str = "ciso.supply";
const POLL_DELAY: Duration = Duration::from_secs(300);
const RETRY_DELAY: Duration = Duration::from_secs(60);
const COLUMNS: [&str; 13] = [
    "solar",
    "wind",
    "geothermal",
    "biomass",
    "biogas",
    "small_hydro",
    "coal",
    "nuclear",
    "natural_gas",
    "large_hydro",
    "batteries",
    "imports",
    "other",
];

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Payload {
    Backfill { date: NaiveDate, until: NaiveDate },
    Forward { date: NaiveDate },
}

type Job = queue::Job<Payload>;
type Rows = BTreeMap<i64, [Option<i32>; 13]>;
type Continuation = Option<(Payload, Duration)>;

impl Payload {
    fn plan(&self, today: NaiveDate) -> Result<(Vec<NaiveDate>, Continuation)> {
        let mut dates = Vec::new();
        match *self {
            Self::Backfill { date, until } => {
                ensure!(
                    until <= date && date < today,
                    "backfill requires until <= date < today"
                );
                let mut day = date;
                loop {
                    dates.push(day);
                    if day == until {
                        return Ok((dates, None));
                    }
                    day = day.pred_opt().context("backfill date underflow")?;
                    if dates.len() == 31 {
                        return Ok((
                            dates,
                            Some((Self::Backfill { date: day, until }, Duration::ZERO)),
                        ));
                    }
                }
            }
            Self::Forward { date } => {
                ensure!(date <= today, "forward date must not be in the future");
                let mut day = date;
                loop {
                    dates.push(day);
                    if day == today {
                        let yesterday = today.pred_opt().context("forward date underflow")?;
                        return Ok((dates, Some((Self::Forward { date: yesterday }, POLL_DELAY))));
                    }
                    day = day.succ_opt().context("forward date overflow")?;
                    if dates.len() == 31 {
                        return Ok((dates, Some((Self::Forward { date: day }, Duration::ZERO))));
                    }
                }
            }
        }
    }
}

fn url(date: NaiveDate, today: NaiveDate) -> String {
    let directory = if date == today {
        "current".to_owned()
    } else {
        format!("history/{}", date.format("%Y%m%d"))
    };
    format!("https://www.caiso.com/outlook/{directory}/fuelsource.csv")
}

async fn fetch(http: &reqwest::Client, url: &str, log: &InvocationLog) -> Result<Option<Bytes>> {
    let request = || {
        http.get(url)
            // CAISO rejects requests without a User-Agent with HTTP 403.
            .header(
                reqwest::header::USER_AGENT,
                concat!("rustciso/", env!("CARGO_PKG_VERSION")),
            )
            .header("Cache-Control", "no-cache")
    };
    let response = request()
        .query(&[("_", Utc::now().timestamp_millis())])
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?;
    let response = if response.status() == StatusCode::NOT_FOUND {
        log.push(format!(
            "supply HTTP 404 for {url}; confirming canonical URL without cache-buster"
        ));
        request()
            .send()
            .await
            .with_context(|| format!("confirming {url}"))?
    } else {
        response
    };
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(response.error_for_status()?.bytes().await?))
}

fn collect_rows(
    data: &Bytes,
    date: NaiveDate,
    rows: &mut Rows,
    log: &InvocationLog,
) -> Result<usize> {
    let columns = crate::transform::csv_columns(data);
    let normalized = columns
        .iter()
        .map(|name| name.to_ascii_lowercase().replace(' ', "_"))
        .collect::<Vec<_>>();
    let expected = std::iter::once("time").chain(COLUMNS).collect::<Vec<_>>();
    ensure!(
        normalized == expected,
        "unexpected supply CSV header: {columns:?}"
    );
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let reader = ReaderBuilder::new(schema)
        .with_header(true)
        .build(Cursor::new(data.clone()))?;
    let before = rows.len();
    let mut previous_time = None;
    let mut wrapped_midnight = false;
    let mut duplicated = 0;
    for batch in reader {
        let batch = batch?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("expected CSV strings")
            })
            .collect::<Result<Vec<_>>>()?;
        for i in 0..batch.num_rows() {
            let value = |col: usize| {
                if columns[col].is_null(i) {
                    ""
                } else {
                    columns[col].value(i).trim()
                }
            };
            let time =
                NaiveTime::parse_from_str(value(0), "%H:%M").context("invalid supply time")?;
            ensure!(
                time.minute() % 5 == 0 && time.second() == 0,
                "time is not five-minute aligned"
            );
            ensure!(
                !wrapped_midnight,
                "unexpected supply row after trailing midnight"
            );
            if time == NaiveTime::MIN && previous_time == NaiveTime::from_hms_opt(23, 55, 0) {
                wrapped_midnight = true;
                log.push(format!("supply {date}: excluded trailing next-day midnight; next daily file is authoritative"));
                continue;
            }
            ensure!(
                previous_time.is_none_or(|previous| previous < time),
                "supply times are not strictly increasing"
            );
            previous_time = Some(time);
            let mut values = [None; 13];
            for col in 0..13 {
                if !value(col + 1).is_empty() {
                    values[col] =
                        Some(value(col + 1).parse::<i32>().with_context(|| {
                            format!("invalid {} at {date} {time}", COLUMNS[col])
                        })?);
                }
            }
            if values.iter().all(Option::is_none) {
                continue;
            }
            let local = date.and_time(time);
            let timestamps = match Los_Angeles.from_local_datetime(&local) {
                LocalResult::Single(time) => vec![time],
                LocalResult::Ambiguous(daylight, standard) => {
                    duplicated += 1;
                    vec![daylight, standard]
                }
                LocalResult::None => {
                    anyhow::bail!("nonexistent Pacific time {local} has nonempty supply data")
                }
            };
            for time in timestamps {
                ensure!(
                    rows.insert(time.timestamp_micros(), values).is_none(),
                    "duplicate supply timestamp {time}"
                );
            }
        }
    }
    if duplicated > 0 {
        log.push(format!("supply {date}: copied {duplicated} ambiguous clock times into both daylight and standard hours"));
    }
    Ok(rows.len() - before)
}

fn batch(rows: &Rows, downloaded_at: DateTime<Utc>) -> Result<RecordBatch> {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut fields = vec![Field::new("utcbegin", timestamp.clone(), true)];
    fields.extend(COLUMNS.map(|name| Field::new(name, DataType::Int32, true)));
    fields.push(Field::new("downloaded_at", timestamp, true));
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(
        TimestampMicrosecondArray::from_iter_values(rows.keys().copied()).with_timezone("UTC"),
    )];
    arrays.extend((0..13).map(|col| {
        Arc::new(Int32Array::from_iter(
            rows.values().map(|values| values[col]),
        )) as ArrayRef
    }));
    arrays.push(Arc::new(
        TimestampMicrosecondArray::from_value(downloaded_at.timestamp_micros(), rows.len())
            .with_timezone("UTC"),
    ));
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

async fn load(state: &AppState, payload: &Payload, log: &InvocationLog) -> Result<Continuation> {
    let today = Utc::now().with_timezone(&Los_Angeles).date_naive();
    let (dates, mut next) = payload.plan(today)?;
    let mut rows = Rows::new();
    for date in dates {
        let data = fetch(&state.http, &url(date, today), log).await?;
        let count = match data {
            Some(data) => collect_rows(&data, date, &mut rows, log)
                .with_context(|| format!("reading supply {date}"))?,
            None => 0,
        };
        if count == 0 && date < today {
            match payload {
                Payload::Backfill { .. } => {
                    log.push(format!(
                        "supply {date}: no data available; checking older days"
                    ));
                }
                Payload::Forward { .. } => {
                    anyhow::bail!("no historical supply data available for {date}")
                }
            }
        }
        log.push(format!("supply {date}: fetched {count} rows"));
    }
    if !rows.is_empty() {
        let loaded =
            lake::replace_window(&state.cfg, TABLE, vec![batch(&rows, Utc::now())?], log).await?;
        log.push(format!(
            "{TABLE}: snapshot {}, {} rows",
            loaded.snapshot_id, loaded.rows
        ));
    } else {
        log.push("no supply rows; leaving stored data unchanged");
        if matches!(payload, Payload::Backfill { .. }) {
            log.push("supply backfill stopped: entire requested date range has no data");
            next = None;
        }
    }
    if next.is_none() {
        log.push("supply backfill finished");
    }
    Ok(next)
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let log = InvocationLog::new(req.as_ref().ok().and_then(|Json(req)| request_id(req)));
    let parsed = (|| -> Result<Job> {
        let job: Job = serde_json::from_value(first_payload(req?.0)?)?;
        job.payload
            .plan(Utc::now().with_timezone(&Los_Angeles).date_naive())?;
        Ok(job)
    })();
    let job = match parsed {
        Ok(job) => job,
        Err(error) => {
            log.push(format!("bad supply request: {error:#}"));
            return respond(StatusCode::BAD_REQUEST, log);
        }
    };
    let result = async {
        log.push(InvocationLog::json("supply job", &job)?);
        match load(&state, &job.payload, &log).await {
            Ok(Some((payload, visibility))) => {
                let next = Job {
                    payload,
                    retry: job.retry.fresh(),
                };
                state
                    .queue_named(NAME)?
                    .send(&serde_json::to_string(&next)?, visibility)
                    .await?;
                log.push(InvocationLog::json("supply continuation", &next)?);
            }
            Ok(None) => {}
            Err(error) => {
                let reason = format!("supply load failed: {error:#}");
                log.push(&reason);
                state
                    .queue_named(NAME)?
                    .retry_or_poison(&job, job.retry, RETRY_DELAY, &reason, &log)
                    .await?;
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match result {
        Ok(()) => respond(StatusCode::OK, log),
        Err(error) => {
            log.push(format!("supply queue delivery failed: {error:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(value: &str) -> NaiveDate {
        value.parse().unwrap()
    }

    fn csv(body: &str) -> Bytes {
        Bytes::from(format!(
            "Time,Solar,Wind,Geothermal,Biomass,Biogas,Small hydro,Coal,Nuclear,Natural gas,Large hydro,Batteries,Imports,Other\n{body}"
        ))
    }

    #[test]
    fn backfill_is_bounded_and_forward_refreshes_yesterday_and_today() {
        let today = date("2026-11-19");
        let job: Job = serde_json::from_value(serde_json::json!({
            "mode":"backfill", "date":"2026-11-18", "until":"2021-01-01", "retries":2
        }))
        .unwrap();
        let (dates, next) = job.payload.plan(today).unwrap();
        assert_eq!(dates.len(), 31);
        assert_eq!(dates[30], date("2026-10-19"));
        assert_eq!(
            next,
            Some((
                Payload::Backfill {
                    date: date("2026-10-18"),
                    until: date("2021-01-01")
                },
                Duration::ZERO
            ))
        );
        let (dates, next) = (Payload::Backfill {
            date: date("2021-01-02"),
            until: date("2021-01-01"),
        })
        .plan(today)
        .unwrap();
        assert_eq!(dates, [date("2021-01-02"), date("2021-01-01")]);
        assert_eq!(next, None);
        let (dates, next) = (Payload::Forward {
            date: date("2026-11-18"),
        })
        .plan(today)
        .unwrap();
        assert_eq!(dates, [date("2026-11-18"), today]);
        assert_eq!(
            next,
            Some((
                Payload::Forward {
                    date: date("2026-11-18")
                },
                POLL_DELAY
            ))
        );
        let (dates, next) = (Payload::Forward {
            date: date("2026-10-01"),
        })
        .plan(today)
        .unwrap();
        assert_eq!(dates.len(), 31);
        assert_eq!(
            next,
            Some((
                Payload::Forward {
                    date: date("2026-11-01")
                },
                Duration::ZERO
            ))
        );
        assert!(
            (Payload::Backfill {
                date: today,
                until: today
            })
            .plan(today)
            .is_err()
        );
        assert!(
            (Payload::Backfill {
                date: date("2026-11-17"),
                until: date("2026-11-18")
            })
            .plan(today)
            .is_err()
        );
    }

    #[test]
    fn csv_preserves_ints_nulls_negatives_and_pacific_date() {
        let mut rows = Rows::new();
        collect_rows(&csv("00:00,-60,1115,676,233,162,205,0,2261,6827,2018,-520,9348,\r\n00:05,,,,,,,,,,,,,\r\n"),
            date("2025-12-08"), &mut rows, &InvocationLog::default()).unwrap();
        let downloaded = "2026-11-19T16:00:00Z".parse().unwrap();
        let batch = batch(&rows, downloaded).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            times.value(0),
            DateTime::parse_from_rfc3339("2025-12-08T08:00:00Z")
                .unwrap()
                .timestamp_micros()
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            -60
        );
        assert_eq!(
            batch
                .column(11)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            -520
        );
        assert!(batch.column(13).is_null(0));
        assert_eq!(
            batch
                .column(14)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            downloaded.timestamp_micros()
        );
    }

    #[test]
    fn dst_duplicates_fallback_and_skips_empty_spring_hour() {
        let log = InvocationLog::default();
        let mut rows = Rows::new();
        collect_rows(
            &csv("01:00,1,2,3,4,5,6,7,8,9,10,11,12,13\n"),
            date("2025-11-02"),
            &mut rows,
            &log,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        for time in ["2025-11-02T08:00:00Z", "2025-11-02T09:00:00Z"] {
            assert_eq!(
                rows[&DateTime::parse_from_rfc3339(time)
                    .unwrap()
                    .timestamp_micros()][0],
                Some(1)
            );
        }
        assert!(log.lines()[0].contains("both daylight and standard"));
        rows.clear();
        collect_rows(&csv("01:55,1,2,3,4,5,6,7,8,9,10,11,12,13\n02:00,,,,,,,,,,,,,\n03:00,1,2,3,4,5,6,7,8,9,10,11,12,13\n"),
            date("2025-03-09"), &mut rows, &log).unwrap();
        assert_eq!(
            rows.keys().last().unwrap() - rows.keys().next().unwrap(),
            300_000_000
        );
        assert!(
            collect_rows(
                &csv("02:00,1,2,3,4,5,6,7,8,9,10,11,12,13\n"),
                date("2025-03-09"),
                &mut rows,
                &log
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_source_is_not_mistaken_for_missing_history() {
        for data in [
            Bytes::from("<html>Unavailable</html>"),
            csv("00:00,bad,2,3,4,5,6,7,8,9,10,11,12,13\n"),
            csv("00:00,2147483648,2,3,4,5,6,7,8,9,10,11,12,13\n"),
            csv("00:01,1,2,3,4,5,6,7,8,9,10,11,12,13\n"),
            csv("00:05,1,2,3,4,5,6,7,8,9,10,11,12,13\n00:00,1,2,3,4,5,6,7,8,9,10,11,12,13\n"),
        ] {
            assert!(
                collect_rows(
                    &data,
                    date("2025-12-08"),
                    &mut Rows::new(),
                    &InvocationLog::default()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn trailing_next_day_midnight_does_not_overwrite_either_days_midnight() {
        let log = InvocationLog::default();
        let mut rows = Rows::new();
        collect_rows(&csv("00:00,1,2,3,4,5,6,7,8,9,10,11,12,13\n23:55,2,2,3,4,5,6,7,8,9,10,11,12,13\n00:00,3,2,3,4,5,6,7,8,9,10,11,12,13\n"),
                date("2021-08-16"), &mut rows, &log).unwrap();
        collect_rows(
            &csv("00:00,4,2,3,4,5,6,7,8,9,10,11,12,13\n"),
            date("2021-08-17"),
            &mut rows,
            &log,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.values().next().unwrap()[0], Some(1));
        assert_eq!(rows.values().last().unwrap()[0], Some(4));
        assert!(log.lines()[0].contains("excluded trailing next-day midnight"));
        assert!(collect_rows(&csv("23:55,1,2,3,4,5,6,7,8,9,10,11,12,13\n00:00,1,2,3,4,5,6,7,8,9,10,11,12,13\n00:05,1,2,3,4,5,6,7,8,9,10,11,12,13\n"),
                date("2021-08-18"), &mut rows, &log).is_err());
    }

    #[tokio::test]
    async fn only_404_is_missing_not_server_failures() {
        let app = axum::Router::new()
            .route(
                "/missing",
                axum::routing::get(|| async { StatusCode::NOT_FOUND }),
            )
            .route(
                "/failed",
                axum::routing::get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            )
            .route(
                "/cached",
                axum::routing::get(|uri: axum::http::Uri| async move {
                    if uri.query().is_some() {
                        (StatusCode::NOT_FOUND, "")
                    } else {
                        (StatusCode::OK, "Time,Solar\n00:00,-60\n")
                    }
                }),
            )
            .route(
                "/csv",
                axum::routing::get(|headers: axum::http::HeaderMap| async move {
                    assert_eq!(
                        headers["user-agent"],
                        concat!("rustciso/", env!("CARGO_PKG_VERSION"))
                    );
                    "Time,Solar\n00:00,-60\n"
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();
        let log = InvocationLog::default();
        assert!(
            fetch(&http, &format!("http://{address}/missing"), &log)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            fetch(&http, &format!("http://{address}/failed"), &log)
                .await
                .is_err()
        );
        assert!(
            fetch(&http, &format!("http://{address}/csv"), &log)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            fetch(&http, &format!("http://{address}/cached"), &log)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            log.lines()
                .iter()
                .any(|line| line.contains("confirming canonical URL"))
        );
        server.abort();
    }
}
