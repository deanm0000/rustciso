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
    array::{Array, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Cursor, sync::Arc, time::Duration};

pub const NAME: &str = "wind_solar";
pub const QUEUE: &str = "wind-solar";
const TABLE: &str = "ciso.wind_solar";
const CHUNK: TimeDelta = TimeDelta::days(15);
const RETRY_DELAY: Duration = Duration::from_secs(60);

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
            "{}?resultformat=6&queryname=SLD_REN_FCST&version=1&startdatetime={}&enddatetime={}",
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
        let previous = previous.map(|prev_start| {
            (
                Payload {
                    startdatetime: format_oasis_time(prev_start),
                    stopdatetime: Some(format_oasis_time(start)),
                    get_prev_until: self.get_prev_until.clone(),
                    get_next_until: None,
                },
                Duration::ZERO,
            )
        });
        Ok((Some(Window { start, end }), previous))
    }
}

// Each market is written independently, with a stable sort and the source's row grain.
type Rows = BTreeMap<String, BTreeMap<(i64, String, String), Option<f64>>>;

fn collect_rows(data: &Bytes, window: Window, rows: &mut Rows) -> Result<()> {
    let columns = crate::transform::csv_columns(data);
    let fields = columns
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields.collect::<Vec<_>>()));
    let indexes = [
        "INTERVALSTARTTIME_GMT",
        "INTERVALENDTIME_GMT",
        "MARKET_RUN_ID",
        "TRADING_HUB",
        "RENEWABLE_TYPE",
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
            let market = value(2).to_ascii_lowercase();
            let (hub, renewable) = (value(3), value(4));
            ensure!(
                !market.is_empty() && !hub.is_empty() && !renewable.is_empty(),
                "missing market, hub, or renewable type"
            );
            let mw = if value(5).is_empty() {
                None
            } else {
                let mw: f64 = value(5).parse().context("invalid MW")?;
                ensure!(mw.is_finite(), "nonfinite MW");
                Some(mw)
            };
            let key = (
                start.timestamp_micros(),
                hub.to_owned(),
                renewable.to_owned(),
            );
            if let Some(previous) = rows.entry(market.clone()).or_default().insert(key, mw) {
                ensure!(
                    previous == mw,
                    "conflicting MW for {market}/{hub}/{renewable} at {start}"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str =
        "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,TRADING_HUB,RENEWABLE_TYPE,MW,MARKET_RUN_ID\n";

    #[test]
    fn daily_fallback_covers_partial_last_day_without_extending_window() {
        let start = parse_oasis_time("20260901T07:00-0000").unwrap();
        let end = parse_oasis_time("20260903T09:00-0000").unwrap();
        let slices = (Window { start, end })
            .chunks(TimeDelta::days(1))
            .collect::<Vec<_>>();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].start, start);
        assert_eq!(slices[0].end, slices[1].start);
        assert_eq!(slices[1].end, slices[2].start);
        assert_eq!(slices[2].end, end);
    }

    #[test]
    fn chunks_and_backward_cutoff_cover_pacific_dst_month_without_gaps() {
        let job: Job = serde_json::from_value(serde_json::json!({
            "startdatetime": "20261101T07:00-0000",
            "get_prev_until": "20261015T07:00-0000",
            "retries": 1
        }))
        .unwrap();
        let (window, previous) = job.payload.windows(Utc::now()).unwrap();
        let window = window.unwrap();
        assert_eq!(format_oasis_time(window.end), "20261201T08:00-0000");
        let chunks = window.chunks(CHUNK).collect::<Vec<_>>();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].start, window.start);
        assert_eq!(chunks[2].end, window.end);
        for pair in chunks.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
        let (backfill, done) = previous.unwrap().0.windows(Utc::now()).unwrap();
        let backfill = backfill.unwrap();
        assert_eq!(format_oasis_time(backfill.start), "20261015T07:00-0000");
        assert_eq!(backfill.end, window.start);
        assert!(done.is_none());
    }

    #[test]
    fn forward_payload_waits_for_closed_hour_and_preserves_bound() {
        let job: Job = serde_json::from_value(serde_json::json!({
            "startdatetime": "20261119T13:00-0000",
            "get_next_until": "20400101T00:00-0000"
        }))
        .unwrap();
        let (window, next) = job
            .payload
            .windows(parse_oasis_time("20261119T13:45-0000").unwrap())
            .unwrap();
        assert!(window.is_none());
        let (next, delay) = next.unwrap();
        assert_eq!(next.startdatetime, job.payload.startdatetime);
        assert_eq!(next.get_next_until, job.payload.get_next_until);
        assert_eq!(delay, Duration::from_secs(1200));
        let (window, next) = next
            .windows(parse_oasis_time("20261119T14:05-0000").unwrap())
            .unwrap();
        assert_eq!(
            format_oasis_time(window.unwrap().end),
            "20261119T14:00-0000"
        );
        assert_eq!(next.unwrap().0.startdatetime, "20261119T14:00-0000");
    }

    #[test]
    fn csv_preserves_market_grain_negative_mw_and_ingestion_timestamp() {
        let window = Window {
            start: parse_oasis_time("20260703T07:00-0000").unwrap(),
            end: parse_oasis_time("20260704T07:00-0000").unwrap(),
        };
        let data = Bytes::from(format!(
            "{HEADER}\
                        2026-07-04T06:00:00-00:00,2026-07-04T07:00:00-00:00,NP15,Solar,-7.57526,ACTUAL\n\
                        2026-07-04T06:00:00-00:00,2026-07-04T06:05:00-00:00,NP15,Solar,,RTD\n\
                        2026-07-04T06:00:00-00:00,2026-07-04T06:05:00-00:00,NP15,Wind,123,RTD\n"
        ));
        let mut rows = Rows::new();
        collect_rows(&data, window, &mut rows).unwrap();
        collect_rows(&data, window, &mut rows).unwrap(); // identical duplicate
        let actual = market_batch("actual", &rows["actual"], window.end).unwrap();
        assert_eq!(actual.num_rows(), 1);
        let mw = actual
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(mw.value(0), -7.57526);
        let downloaded = actual
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(downloaded.value(0), window.end.timestamp_micros());
        let rtd = market_batch("rtd", &rows["rtd"], window.end).unwrap();
        assert_eq!(rtd.num_rows(), 2);
        assert!(rtd.column(4).is_null(0));
        let conflict = Bytes::from(format!(
            "{HEADER}\
                        2026-07-04T06:00:00-00:00,2026-07-04T06:05:00-00:00,NP15,Wind,124,RTD\n"
        ));
        assert!(
            collect_rows(&conflict, window, &mut rows)
                .unwrap_err()
                .to_string()
                .contains("conflicting MW")
        );
    }
}

fn market_batch(
    market: &str,
    rows: &BTreeMap<(i64, String, String), Option<f64>>,
    downloaded_at: DateTime<Utc>,
) -> Result<RecordBatch> {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Schema::new(vec![
        Field::new("utcbegin", timestamp.clone(), true),
        Field::new("market_type", DataType::Utf8, true),
        Field::new("trading_hub", DataType::Utf8, true),
        Field::new("renewable_type", DataType::Utf8, true),
        Field::new("mw", DataType::Float64, true),
        Field::new("downloaded_at", timestamp, true),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.keys().map(|k| k.0))
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                market,
                rows.len(),
            ))),
            Arc::new(StringArray::from_iter_values(
                rows.keys().map(|k| k.1.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.keys().map(|k| k.2.as_str()),
            )),
            Arc::new(Float64Array::from_iter(rows.values().copied())),
            Arc::new(
                TimestampMicrosecondArray::from_value(downloaded_at.timestamp_micros(), rows.len())
                    .with_timezone("UTC"),
            ),
        ],
    )?)
}

async fn load(state: &AppState, window: Window, log: &InvocationLog) -> Result<bool> {
    let mut rows = Rows::new();
    let mut pending = window
        .chunks(CHUNK)
        .collect::<std::collections::VecDeque<_>>();
    let mut first = true;
    while let Some(chunk) = pending.pop_front() {
        if !first {
            tokio::time::sleep(oasis::RATE_LIMIT_WAIT).await;
        }
        first = false;
        match oasis::fetch_url(&state.http, &chunk.url(), log).await? {
            OasisResult::NotReady(_) if chunk.end - chunk.start > TimeDelta::days(1) => {
                log.push(format!(
                    "wind_solar [{}, {}): OASIS reported no data; checking daily slices",
                    chunk.start, chunk.end
                ));
                // Live OASIS returned no data for Sep 1-16, 2026 but did for Sep 3 alone.
                for day in chunk
                    .chunks(TimeDelta::days(1))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                {
                    pending.push_front(day);
                }
            }
            OasisResult::NotReady(message) => log.push(format!(
                "wind_solar [{}, {}): no data: {message}",
                chunk.start, chunk.end
            )),
            OasisResult::Csvs(csvs) => {
                for (name, data) in csvs {
                    collect_rows(&data, chunk, &mut rows)
                        .with_context(|| format!("reading {name}"))?;
                }
            }
        }
    }
    if rows.is_empty() {
        log.push("no wind_solar rows; leaving stored data unchanged");
        return Ok(false);
    }
    let table = lake::resolve_table(&state.cfg.lake(log.clone()), TABLE).await?;
    let downloaded_at = Utc::now();
    for (market, rows) in rows {
        let batch = market_batch(&market, &rows, downloaded_at)?;
        let loaded = table
            .replace_partition_window(vec![batch], "market_type", &market)
            .await?;
        log.push(format!(
            "{TABLE} {market}: snapshot {}, {} rows",
            loaded.snapshot_id, loaded.rows
        ));
    }
    Ok(true)
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
            log.push(format!("bad wind_solar request: {error:#}"));
            return respond(StatusCode::BAD_REQUEST, log);
        }
    };
    let result = async {
        if let Some(window) = window {
            log.push(format!(
                "wind_solar loading [{}, {})",
                window.start, window.end
            ));
            match load(&state, window, &log).await {
                Err(error) => {
                    let reason = format!(
                        "error loading wind_solar [{}, {}): {error:#}",
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
                "wind_solar completed [{}, {})",
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
            log.push(format!("wind_solar queue delivery failed: {error:#}"));
            respond(StatusCode::INTERNAL_SERVER_ERROR, log)
        }
    }
}
