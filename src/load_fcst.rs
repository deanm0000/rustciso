use crate::azure::{FuncRequest, first_payload, request_id};
use crate::logging::InvocationLog;
use crate::oasis::{self, OasisResult, format_oasis_time, parse_oasis_time};
use crate::{AppState, HandlerResponse, lake, queue, respond};
use anyhow::{Context, Result, ensure};
use axum::Json;
use axum::extract::{State, rejection::JsonRejection};
use axum::http::StatusCode;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use datafusion::arrow::{
    array::{Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    io::Cursor,
    sync::Arc,
    time::Duration,
};

pub const NAME: &str = "load_fcst";
pub const QUEUE: &str = "load-fcst";
pub const HOURLY: &str = "load_fcst";
pub const RTPD: &str = "load_fcst_rtpd";
pub const RTM: &str = "load_fcst_rtm";
const CHUNK: TimeDelta = TimeDelta::days(15);
const RETRY_DELAY: Duration = Duration::from_secs(60);
const CONTINUATION_DELAY: Duration = Duration::from_secs(6);

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    startdatetime: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stopdatetime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    get_prev_until: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    get_next_until: Option<String>,
}

type Job = queue::Job<Payload>;

#[derive(Clone, Copy)]
struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Window {
    fn chunks(self, limit: TimeDelta) -> impl Iterator<Item = Window> {
        std::iter::successors(Some(self.start), move |start| {
            (*start + limit < self.end).then(|| *start + limit)
        })
        .map(move |start| Window {
            start,
            end: (start + limit).min(self.end),
        })
    }

    fn url(self) -> String {
        format!(
            "{}?resultformat=6&queryname=SLD_FCST&version=1&startdatetime={}&enddatetime={}",
            oasis::OASIS_URL,
            format_oasis_time(self.start),
            format_oasis_time(self.end),
        )
    }
}

impl Payload {
    fn windows(&self, now: DateTime<Utc>) -> Result<(Option<Window>, Option<(Payload, Duration)>)> {
        let start = parse_oasis_time(&self.startdatetime)?;
        let stop = self
            .stopdatetime
            .as_deref()
            .map(parse_oasis_time)
            .transpose()
            .context("invalid stopdatetime")?;
        let cutoff = self
            .get_prev_until
            .as_deref()
            .map(parse_oasis_time)
            .transpose()
            .context("invalid get_prev_until")?;
        if let Some(bound) = &self.get_next_until {
            ensure!(
                stop.is_none() && cutoff.is_none(),
                "get_next_until cannot be combined with stopdatetime or get_prev_until"
            );
            let plan = oasis::forward_window(start, parse_oasis_time(bound)?, now)?;
            let next = plan.next.map(|next| {
                (
                    Payload {
                        startdatetime: format_oasis_time(next),
                        stopdatetime: None,
                        get_prev_until: None,
                        get_next_until: self.get_next_until.clone(),
                    },
                    plan.delay,
                )
            });
            return Ok((plan.end.map(|end| Window { start, end }), next));
        }
        let (end, previous) = oasis::month_window(start, stop, cutoff)?;
        Ok((
            Some(Window { start, end }),
            previous.map(|prev_start| {
                (
                    Payload {
                        startdatetime: format_oasis_time(prev_start),
                        stopdatetime: Some(format_oasis_time(start)),
                        get_prev_until: self.get_prev_until.clone(),
                        get_next_until: None,
                    },
                    CONTINUATION_DELAY,
                )
            }),
        ))
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Series {
    Hourly(String),
    Rtpd,
    Rtm,
}

type Rows = BTreeMap<Series, BTreeMap<(i64, String), Option<f64>>>;

fn collect_rows(data: &Bytes, window: Window, rows: &mut Rows) -> Result<()> {
    let columns = crate::transform::csv_columns(data);
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let indexes = [
        "INTERVALSTARTTIME_GMT",
        "INTERVALENDTIME_GMT",
        "MARKET_RUN_ID",
        "EXECUTION_TYPE",
        "TAC_AREA_NAME",
        "MW",
    ]
    .map(|wanted| {
        let mut matches = columns
            .iter()
            .enumerate()
            .filter(|(_, name)| *name == wanted);
        let (index, _) = matches
            .next()
            .with_context(|| format!("missing {wanted}"))?;
        ensure!(matches.next().is_none(), "duplicate {wanted}");
        Ok(index)
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    let reader = ReaderBuilder::new(schema)
        .with_header(true)
        .build(Cursor::new(data.clone()))?;
    for batch in reader {
        let batch = batch?;
        let columns = indexes
            .iter()
            .map(|&i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("expected CSV string column")
            })
            .collect::<Result<Vec<_>>>()?;
        for i in 0..batch.num_rows() {
            let value = |col: usize| {
                let column = columns[col];
                if column.is_null(i) {
                    ""
                } else {
                    column.value(i).trim()
                }
            };
            let start = DateTime::parse_from_rfc3339(value(0)).context("invalid interval start")?;
            let end = DateTime::parse_from_rfc3339(value(1)).context("invalid interval end")?;
            ensure!(
                start >= window.start && start < end && end <= window.end,
                "interval [{start}, {end}) is outside requested window"
            );
            let duration = end - start;
            let market = value(2);
            let execution = value(3);
            let area = value(4);
            ensure!(!area.is_empty(), "missing TAC area at {start}");
            let series = match (market, execution, duration) {
                ("2DA" | "7DA" | "DAM" | "ACTUAL", exec, d)
                    if market == exec && d == TimeDelta::hours(1) =>
                {
                    Series::Hourly(market.to_owned())
                }
                ("RTM", "RTPD", d) if d == TimeDelta::minutes(15) => Series::Rtpd,
                ("RTM", "RTD", d) if d == TimeDelta::minutes(5) => Series::Rtm,
                _ => anyhow::bail!(
                    "unexpected market {market:?}, execution {execution:?}, duration {duration} at {start}"
                ),
            };
            ensure!(
                start.timestamp().rem_euclid(duration.num_seconds()) == 0
                    && start.timestamp_subsec_nanos() == 0,
                "misaligned {duration} interval at {start}"
            );
            let mw = if value(5).is_empty() {
                None
            } else {
                let mw: f64 = value(5).parse().context("invalid MW")?;
                ensure!(mw.is_finite(), "nonfinite MW");
                Some(mw)
            };
            let key = (start.timestamp_micros(), area.to_owned());
            if let Some(previous) = rows.entry(series).or_default().insert(key, mw) {
                ensure!(
                    previous == mw,
                    "conflicting MW for {market}/{execution}/{area} at {start}"
                );
            }
        }
    }
    Ok(())
}

fn series_batch(
    series: &Series,
    rows: &BTreeMap<(i64, String), Option<f64>>,
    downloaded_at: DateTime<Utc>,
) -> Result<RecordBatch> {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut fields = vec![Field::new("utcbegin", timestamp.clone(), true)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(
        TimestampMicrosecondArray::from_iter_values(rows.keys().map(|key| key.0))
            .with_timezone("UTC"),
    )];
    if let Series::Hourly(market) = series {
        for name in ["market", "exec_type"] {
            fields.push(Field::new(name, DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n(market.as_str(), rows.len()),
            )));
        }
    }
    fields.extend([
        Field::new("tac_area", DataType::Utf8, true),
        Field::new("mw", DataType::Float64, true),
        Field::new("downloaded_at", timestamp, true),
    ]);
    arrays.push(Arc::new(StringArray::from_iter_values(
        rows.keys().map(|key| key.1.as_str()),
    )));
    arrays.push(Arc::new(Float64Array::from_iter(rows.values().copied())));
    arrays.push(Arc::new(
        TimestampMicrosecondArray::from_value(downloaded_at.timestamp_micros(), rows.len())
            .with_timezone("UTC"),
    ));
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

async fn load(state: &AppState, window: Window, log: &InvocationLog) -> Result<bool> {
    let mut pending = window.chunks(CHUNK).collect::<VecDeque<_>>();
    let mut first = true;
    let mut loaded_rows = false;
    while let Some(chunk) = pending.pop_front() {
        if !first {
            tokio::time::sleep(oasis::RATE_LIMIT_WAIT).await;
        }
        first = false;
        let csvs = match oasis::fetch_url(&state.http, &chunk.url(), log).await? {
            OasisResult::NotReady(_) if chunk.end - chunk.start > TimeDelta::days(1) => {
                log.push(format!(
                    "load_fcst [{}, {}): OASIS reported no data; checking daily slices",
                    chunk.start, chunk.end
                ));
                for day in chunk
                    .chunks(TimeDelta::days(1))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                {
                    pending.push_front(day);
                }
                continue;
            }
            OasisResult::NotReady(message) => {
                log.push(format!(
                    "load_fcst [{}, {}): no data: {message}",
                    chunk.start, chunk.end
                ));
                continue;
            }
            OasisResult::Csvs(csvs) => csvs,
        };
        let mut rows = Rows::new();
        for (name, data) in csvs {
            collect_rows(&data, chunk, &mut rows).with_context(|| format!("reading {name}"))?;
        }
        if rows.is_empty() {
            log.push(format!(
                "load_fcst [{}, {}): no rows; leaving stored data unchanged",
                chunk.start, chunk.end
            ));
            continue;
        }
        loaded_rows = true;
        let downloaded_at = Utc::now();
        for (series, values) in rows {
            let name = match &series {
                Series::Hourly(_) => HOURLY,
                Series::Rtpd => RTPD,
                Series::Rtm => RTM,
            };
            let table_name = format!("ciso.{name}");
            let table = lake::resolve_table(&state.cfg.lake(log.clone()), &table_name).await?;
            let batch = series_batch(&series, &values, downloaded_at)?;
            let loaded = match &series {
                Series::Hourly(market) => {
                    table
                        .replace_partition_window(vec![batch], "market", market)
                        .await?
                }
                Series::Rtpd | Series::Rtm => table.replace_window(vec![batch]).await?,
            };
            log.push(format!(
                "{table_name} {series:?}: snapshot {}, {} rows",
                loaded.snapshot_id, loaded.rows
            ));
        }
    }
    Ok(loaded_rows)
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    req: Result<Json<FuncRequest>, JsonRejection>,
) -> HandlerResponse {
    let log = InvocationLog::new(req.as_ref().ok().and_then(|Json(req)| request_id(req)));
    let parsed = (|| -> Result<_> {
        let job: Job = serde_json::from_value(first_payload(req?.0)?)?;
        let (window, next) = job.payload.windows(Utc::now())?;
        Ok((job, window, next))
    })();
    let (job, window, next) = match parsed {
        Ok(parsed) => parsed,
        Err(error) => {
            log.push(format!("bad load_fcst request: {error:#}"));
            return respond(StatusCode::BAD_REQUEST, log);
        }
    };
    let result = async {
        if let Some(window) = window {
            log.push(format!(
                "load_fcst loading [{}, {})",
                window.start, window.end
            ));
            match load(&state, window, &log).await {
                Err(error) => {
                    let reason = format!(
                        "error loading load_fcst [{}, {}): {error:#}",
                        window.start, window.end
                    );
                    log.push(&reason);
                    return state
                        .queue_named(NAME)?
                        .retry_or_poison(&job, job.retry, RETRY_DELAY, &reason, &log)
                        .await;
                }
                Ok(false)
                    if job.payload.get_next_until.is_some()
                        && oasis::retry_empty_forward(window.start, Utc::now()) =>
                {
                    log.push("recent forward window has no rows; retrying in five minutes");
                    return state
                        .queue_named(NAME)?
                        .send(&serde_json::to_string(&job)?, Duration::from_secs(300))
                        .await;
                }
                Ok(_) => {}
            }
            log.push(format!(
                "load_fcst completed [{}, {})",
                window.start, window.end
            ));
        }
        if let Some((payload, delay)) = next {
            let next = Job {
                payload,
                retry: job.retry.fresh(),
            };
            state
                .queue_named(NAME)?
                .send(&serde_json::to_string(&next)?, delay)
                .await?;
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => respond(StatusCode::OK, log),
        Err(error) => {
            log.push(format!("load_fcst queue delivery failed: {error:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str =
        "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,MARKET_RUN_ID,EXECUTION_TYPE,TAC_AREA_NAME,MW\n";

    fn window() -> Window {
        Window {
            start: parse_oasis_time("20210101T08:00-0000").unwrap(),
            end: parse_oasis_time("20210102T08:00-0000").unwrap(),
        }
    }

    #[test]
    fn routes_grains_and_preserves_values_and_schema() {
        let data = Bytes::from(format!(
            "{HEADER}\
    2021-01-01T08:00:00Z,2021-01-01T09:00:00Z,DAM,DAM,CA ISO-TAC,12.5\n\
    2021-01-01T08:00:00Z,2021-01-01T09:00:00Z,ACTUAL,ACTUAL,CA ISO-TAC,13\n\
    2021-01-01T08:00:00Z,2021-01-01T08:15:00Z,RTM,RTPD,CA ISO-TAC,-2.5\n\
    2021-01-01T08:00:00Z,2021-01-01T08:05:00Z,RTM,RTD,CA ISO-TAC,\n"
        ));
        let mut rows = Rows::new();
        collect_rows(&data, window(), &mut rows).unwrap();
        collect_rows(&data, window(), &mut rows).unwrap();
        assert_eq!(rows.len(), 4);
        let hourly = series_batch(
            &Series::Hourly("DAM".into()),
            &rows[&Series::Hourly("DAM".into())],
            window().end,
        )
        .unwrap();
        assert_eq!(
            hourly
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            [
                "utcbegin",
                "market",
                "exec_type",
                "tac_area",
                "mw",
                "downloaded_at"
            ]
        );
        assert_eq!(
            hourly
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "DAM"
        );
        let rtpd = series_batch(&Series::Rtpd, &rows[&Series::Rtpd], window().end).unwrap();
        assert_eq!(
            rtpd.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["utcbegin", "tac_area", "mw", "downloaded_at"]
        );
        assert_eq!(
            rtpd.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            -2.5
        );
        let rtm = series_batch(&Series::Rtm, &rows[&Series::Rtm], window().end).unwrap();
        assert!(rtm.column(2).is_null(0));
        assert_eq!(
            rtm.column(3)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            window().end.timestamp_micros()
        );
    }

    #[test]
    fn rejects_bad_grains_conflicts_and_invalid_rows() {
        let row = "2021-01-01T08:00:00Z,2021-01-01T08:05:00Z,RTM,RTD,CA ISO-TAC,10\n";
        let mut rows = Rows::new();
        collect_rows(&Bytes::from(format!("{HEADER}{row}")), window(), &mut rows).unwrap();
        for invalid in [
            row.replace(",10\n", ",11\n"),
            row.replace(",RTD,", ",RTPD,"),
            row.replace(",RTM,RTD,", ",DAM,DAM,"),
            row.replace("08:00:00Z", "08:01:00Z")
                .replace("08:05:00Z", "08:06:00Z"),
            row.replace("2021-01-01T08:", "2020-12-31T08:"),
            row.replace("CA ISO-TAC", ""),
            row.replace(",10\n", ",NaN\n"),
        ] {
            assert!(
                collect_rows(
                    &Bytes::from(format!("{HEADER}{invalid}")),
                    window(),
                    &mut rows
                )
                .is_err(),
                "{invalid}"
            );
        }
        assert!(collect_rows(&Bytes::from("MW\n1\n"), window(), &mut Rows::new()).is_err());
    }

    #[test]
    fn backfill_continuation_respects_dst_and_cutoff() {
        let payload = Payload {
            startdatetime: "20261101T07:00-0000".into(),
            stopdatetime: None,
            get_prev_until: Some("20261015T07:00-0000".into()),
            get_next_until: None,
        };
        let (window, previous) = payload.windows(Utc::now()).unwrap();
        let window = window.unwrap();
        assert_eq!(format_oasis_time(window.end), "20261201T08:00-0000");
        let (previous, end) = previous.unwrap().0.windows(Utc::now()).unwrap();
        let previous = previous.unwrap();
        assert_eq!(format_oasis_time(previous.start), "20261015T07:00-0000");
        assert_eq!(previous.end, window.start);
        assert!(end.is_none());
    }

    #[test]
    fn forward_payload_advances_and_retains_bound() {
        let payload: Payload = serde_json::from_value(serde_json::json!({
            "startdatetime": "20261119T08:00-0000",
            "get_next_until": "20400101T00:00-0000"
        }))
        .unwrap();
        let (window, next) = payload
            .windows(parse_oasis_time("20261121T15:00-0000").unwrap())
            .unwrap();
        assert_eq!(
            format_oasis_time(window.unwrap().end),
            "20261120T08:00-0000"
        );
        let (next, delay) = next.unwrap();
        assert_eq!(next.startdatetime, "20261120T08:00-0000");
        assert_eq!(next.get_next_until, payload.get_next_until);
        assert_eq!(delay, Duration::from_secs(6));
        let invalid: Payload = serde_json::from_value(serde_json::json!({
            "startdatetime": "20261119T08:00-0000",
            "get_next_until": "20400101T00:00-0000",
            "get_prev_until": "20210101T08:00-0000"
        }))
        .unwrap();
        assert!(invalid.windows(Utc::now()).is_err());
    }

    #[test]
    #[ignore = "requires a downloaded OASIS CSV in LOAD_FCST_FIXTURE"]
    fn validates_downloaded_report() {
        let path = std::env::var("LOAD_FCST_FIXTURE").unwrap();
        let data = Bytes::from(std::fs::read(path).unwrap());
        let window = Window {
            start: parse_oasis_time("20261101T07:00-0000").unwrap(),
            end: parse_oasis_time("20261116T07:00-0000").unwrap(),
        };
        let mut rows = Rows::new();
        collect_rows(&data, window, &mut rows).unwrap();
        for series in [
            Series::Hourly("2DA".into()),
            Series::Hourly("7DA".into()),
            Series::Hourly("DAM".into()),
            Series::Hourly("ACTUAL".into()),
            Series::Rtpd,
            Series::Rtm,
        ] {
            assert!(!rows[&series].is_empty(), "{series:?}");
        }
    }
}
