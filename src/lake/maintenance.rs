//! Finds daily-file ranges eligible for repacking, materializing timestamp stats first.

use super::{SCHEMA, connect};
use crate::config::Config;
use crate::lake::repack::Job;
use crate::market::{MARKETS, Market};
use crate::queue::QueueClient;
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use std::time::Duration;
use tokio_postgres::GenericClient;

const MATERIALIZE: &str = "
    CREATE TEMP TABLE maintenance_s1 ON COMMIT DROP AS
    SELECT f.path,
           extract(epoch FROM s.max_value::timestamptz - s.min_value::timestamptz
                   + interval '5 minutes') / 3600 AS hours,
           s.min_value::timestamptz AT TIME ZONE 'America/Los_Angeles' AS min_value,
           s.max_value::timestamptz AT TIME ZONE 'America/Los_Angeles' AS max_value,
           f.record_count
      FROM ducklake.ducklake_data_file f
      INNER JOIN ducklake.ducklake_file_column_stats s USING (data_file_id, table_id)
      INNER JOIN ducklake.ducklake_table t USING (table_id)
      INNER JOIN ducklake.ducklake_schema sc USING (schema_id)
      INNER JOIN ducklake.ducklake_column c USING (table_id, column_id)
     WHERE f.end_snapshot IS NULL AND sc.schema_name = $1
       AND t.table_name = $2 AND c.column_name = 'utcbegin'";

const CANDIDATES: &str = "
    WITH s2 AS (
        SELECT date_trunc('month', min_value) AS mo, path, min_value, max_value
          FROM pg_temp.maintenance_s1
         WHERE extract(hour FROM max_value) = 23 AND extract(hour FROM min_value) = 0
    )
    SELECT min(min_value) AS begin_date, max(max_value) AS end_date, array_agg(path) AS files
      FROM s2
     GROUP BY mo
    HAVING min(min_value) = mo
       AND count(1) = extract(day FROM mo + interval '1 month' - interval '5 minutes')
     ORDER BY mo";

fn job(
    market: &Market,
    begin: NaiveDateTime,
    end: NaiveDateTime,
    files: Vec<String>,
) -> Result<Job> {
    let job = Job {
        table: market.table(),
        begin_date: begin.date(),
        // Catalog maxima are inclusive; repack dates are exclusive local midnights.
        end_date: end
            .date()
            .succ_opt()
            .context("maintenance end date is out of range")?,
        unique: Some(vec!["utcbegin".into(), "node".into()]),
        files: Some(files),
    };
    job.validate()?;
    Ok(job)
}

async fn table_jobs(client: &impl GenericClient, market: &Market) -> Result<Vec<Job>> {
    // Keep this as a separate statement: inlining the timestamp casts in a CTE
    // can cause Postgres to cast unrelated column stats before filtering them.
    client
        .execute(MATERIALIZE, &[&SCHEMA, &market.name])
        .await
        .with_context(|| format!("materializing {} timestamp stats", market.table()))?;
    client
        .query(CANDIDATES, &[])
        .await
        .with_context(|| format!("finding {} repack ranges", market.table()))?
        .into_iter()
        .map(|row| job(market, row.try_get(0)?, row.try_get(1)?, row.try_get(2)?))
        .collect()
}

async fn enqueue(queue: &QueueClient, jobs: &[Job], log: &mut Vec<String>) -> Result<()> {
    for job in jobs {
        queue
            .send(&serde_json::to_string(job)?, Duration::ZERO)
            .await
            .with_context(|| format!("enqueueing maintenance repack {job:?}"))?;
        let message = format!(
            "queued maintenance repack of {} [{}, {}) America/Los_Angeles",
            job.table, job.begin_date, job.end_date
        );
        eprintln!("{message}");
        log.push(message);
    }
    Ok(())
}

pub async fn run(cfg: &Config, queue: &QueueClient, log: &mut Vec<String>) -> Result<()> {
    let mut client = connect(cfg).await?;
    let mut jobs = Vec::new();
    for market in MARKETS {
        let tx = client.transaction().await?;
        let found = table_jobs(&tx, market).await?;
        log.push(format!(
            "{}: found {} maintenance ranges",
            market.table(),
            found.len()
        ));
        jobs.extend(found);
        tx.commit()
            .await
            .context("dropping maintenance temp table")?;
    }
    enqueue(queue, &jobs, log).await?;
    log.push(format!("maintenance queued {} repack jobs", jobs.len()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageAccount;
    use crate::market::{DAPRICES, FMPRICES, RTPRICES};
    use axum::{Router, routing::post};
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use serde_json::json;

    #[test]
    fn inclusive_local_maximum_becomes_exclusive_next_day() {
        for market in [&DAPRICES, &RTPRICES, &FMPRICES] {
            for (begin, end, exclusive) in [
                ("2026-01-01T00:00:00", "2026-01-31T23:55:00", "2026-02-01"),
                ("2026-03-01T00:00:00", "2026-03-31T23:55:00", "2026-04-01"),
                ("2026-11-01T00:00:00", "2026-11-30T23:45:00", "2026-12-01"),
                ("2026-12-01T00:00:00", "2026-12-31T23:00:00", "2027-01-01"),
                ("2028-02-01T00:00:00", "2028-02-29T23:55:00", "2028-03-01"),
                ("2026-10-01T00:00:00", "2026-10-18T23:55:00", "2026-10-19"),
            ] {
                let job = job(
                    market,
                    begin.parse().unwrap(),
                    end.parse().unwrap(),
                    vec!["daily.parquet".into()],
                )
                .unwrap();
                assert_eq!(
                    serde_json::to_value(job).unwrap(),
                    json!({
                        "table": market.table(),
                        "begin_date": &begin[..10],
                        "end_date": exclusive,
                        "unique": ["utcbegin", "node"],
                        "files": ["daily.parquet"]
                    })
                );
            }
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
    async fn enqueues_every_job_and_surfaces_queue_errors() {
        let jobs = [
            job(
                &DAPRICES,
                "2026-01-01T00:00:00".parse().unwrap(),
                "2026-01-31T23:00:00".parse().unwrap(),
                vec!["da-daily.parquet".into()],
            )
            .unwrap(),
            job(
                &RTPRICES,
                "2026-03-01T00:00:00".parse().unwrap(),
                "2026-03-31T23:55:00".parse().unwrap(),
                vec!["rt-daily.parquet".into()],
            )
            .unwrap(),
            job(
                &FMPRICES,
                "2026-11-01T00:00:00".parse().unwrap(),
                "2026-11-30T23:45:00".parse().unwrap(),
                vec!["fm-daily.parquet".into()],
            )
            .unwrap(),
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
        enqueue(&queue, &jobs, &mut log).await.unwrap();
        for expected in &jobs {
            let (uri, body) = receive.try_recv().unwrap();
            assert_eq!(uri.query(), Some("messagettl=-1&visibilitytimeout=0"));
            let encoded = body
                .strip_prefix("<QueueMessage><MessageText>")
                .unwrap()
                .strip_suffix("</MessageText></QueueMessage>")
                .unwrap();
            let decoded = B64.decode(encoded).unwrap();
            let job: Job = serde_json::from_slice(&decoded).unwrap();
            job.validate().unwrap();
            assert_eq!(
                serde_json::to_value(job).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
        assert_eq!(log.len(), jobs.len());
        assert!(receive.try_recv().is_err());
        let failed_queue = QueueClient::new(reqwest::Client::new(), account, "missing");
        let error = enqueue(&failed_queue, &jobs, &mut log).await.unwrap_err();
        assert!(format!("{error:#}").contains("HTTP 404"));
        assert_eq!(log.len(), jobs.len());
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires MAINTENANCE_TEST_DATABASE_URL pointing to a test Postgres without a ducklake schema"]
    async fn postgres_selection_materializes_stats_and_isolates_each_table() {
        let url = std::env::var("MAINTENANCE_TEST_DATABASE_URL").unwrap();
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        let mut tx = client.transaction().await.unwrap();
        tx.batch_execute("
            CREATE SCHEMA ducklake;
            CREATE TABLE ducklake.ducklake_schema (schema_id bigint, schema_name text);
            CREATE TABLE ducklake.ducklake_table (table_id bigint, schema_id bigint, table_name text);
            CREATE TABLE ducklake.ducklake_column (table_id bigint, column_id bigint, column_name text);
            CREATE TABLE ducklake.ducklake_data_file (
                data_file_id bigint, table_id bigint, path text, record_count bigint, end_snapshot bigint
            );
            CREATE TABLE ducklake.ducklake_file_column_stats (
                data_file_id bigint, table_id bigint, column_id bigint, min_value text, max_value text
            );
            INSERT INTO ducklake.ducklake_schema VALUES (1, 'ciso'), (2, 'other');
            INSERT INTO ducklake.ducklake_table VALUES
                (1, 1, 'rtprices'), (2, 1, 'fmprices'), (3, 1, 'daprices'), (4, 2, 'rtprices');
            INSERT INTO ducklake.ducklake_column
                SELECT table_id, 1, 'utcbegin' FROM ducklake.ducklake_table;
            INSERT INTO ducklake.ducklake_column
                SELECT table_id, 2, 'node' FROM ducklake.ducklake_table;
            CREATE TEMP TABLE fixture_days ON COMMIT DROP AS
                SELECT row_number() OVER () AS id, t, day
                  FROM generate_series(1, 4) t
                  CROSS JOIN generate_series(
                      timestamp '2026-01-01', timestamp '2026-07-31', interval '1 day'
                  ) day
                UNION ALL
                SELECT 1000 + row_number() OVER (), t, day
                  FROM generate_series(1, 4) t
                  CROSS JOIN generate_series(
                      timestamp '2028-02-01', timestamp '2028-02-29', interval '1 day'
                  ) day;
            INSERT INTO ducklake.ducklake_data_file
                SELECT id, t, 'file-' || t || '-' || day::date, 100,
                       CASE WHEN day = timestamp '2026-06-01' THEN 1 END
                  FROM fixture_days
                 WHERE day NOT IN (timestamp '2026-04-01', timestamp '2026-07-31');
            INSERT INTO ducklake.ducklake_file_column_stats
                SELECT id, t, 1,
                       (day AT TIME ZONE 'America/Los_Angeles')::text,
                       ((day + interval '23 hours 55 minutes') AT TIME ZONE 'America/Los_Angeles')::text
                  FROM fixture_days
                 WHERE day NOT IN (timestamp '2026-04-01', timestamp '2026-07-31');
            UPDATE ducklake.ducklake_file_column_stats
               SET max_value = ((min_value::timestamptz AT TIME ZONE 'America/Los_Angeles'
                                 + interval '55 minutes') AT TIME ZONE 'America/Los_Angeles')::text
             WHERE min_value::timestamptz AT TIME ZONE 'America/Los_Angeles'
                   >= timestamp '2026-05-01'
               AND min_value::timestamptz AT TIME ZONE 'America/Los_Angeles'
                   < timestamp '2026-06-01';
            INSERT INTO ducklake.ducklake_file_column_stats
                SELECT data_file_id, table_id, 2, 'not a timestamp', 'not a timestamp'
                FROM ducklake.ducklake_data_file;
        ").await.unwrap();
        for market in MARKETS {
            let savepoint = tx.transaction().await.unwrap();
            let jobs = table_jobs(&savepoint, market).await.unwrap();
            let expected = [
                ("2026-01-01", "2026-02-01", 31),
                ("2026-02-01", "2026-03-01", 28),
                ("2026-03-01", "2026-04-01", 31),
                ("2028-02-01", "2028-03-01", 29),
            ];
            assert_eq!(jobs.len(), expected.len());
            let table_id = match market.name {
                "rtprices" => 1,
                "fmprices" => 2,
                "daprices" => 3,
                _ => unreachable!(),
            };
            for (job, (begin, end, days)) in jobs.iter().zip(expected) {
                let mut expected_files: Vec<_> = (1..=days)
                    .map(|day| format!("file-{table_id}-{}-{day:02}", &begin[..7]))
                    .collect();
                let mut files = job.files.clone().unwrap();
                files.sort();
                expected_files.sort();
                assert_eq!(files, expected_files);
                assert_eq!(
                    serde_json::to_value(job).unwrap(),
                    json!({
                        "table": market.table(),
                        "begin_date": begin,
                        "end_date": end,
                        "unique": ["utcbegin", "node"],
                        "files": job.files
                    })
                );
            }
            savepoint.rollback().await.unwrap();
        }
        tx.rollback().await.unwrap();
        drop(client);
        connection.await.unwrap();
    }
}
