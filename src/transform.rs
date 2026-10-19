use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{ArrayRef, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::io::Cursor;
use std::sync::Arc;

const START_COLUMN: &str = "INTERVALSTARTTIME_GMT";
const NODE_COLUMN: &str = "NODE";
const TYPE_COLUMN: &str = "LMP_TYPE";
/// The price column is named differently per report (RTM, RTPD, DAM).
const VALUE_COLUMNS: [&str; 3] = ["VALUE", "PRC", "MW"];

/// OASIS LMP_TYPE -> column suffix; the table column is `{prefix}{suffix}`.
pub const LMP_TYPES: [(&str, &str); 5] = [
    ("LMP", "lmp"),
    ("MCC", "mcc"),
    ("MCE", "mce"),
    ("MCL", "mcl"),
    ("MGHG", "mghg"),
];

fn utc_ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

pub fn target_schema(prefix: &str) -> SchemaRef {
    let mut fields = vec![
        Field::new("utcbegin", utc_ts(), true),
        Field::new("node", DataType::Utf8, true),
    ];
    fields.extend(
        LMP_TYPES
            .iter()
            .map(|(_, c)| Field::new(format!("{prefix}{c}"), DataType::Float64, true)),
    );
    fields.push(Field::new("downloaded_at", utc_ts(), true));
    Arc::new(Schema::new(fields))
}

/// Normalized raw schema shared by every report: start, node, lmp_type, value.
fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("start", DataType::Utf8, true),
        Field::new("node", DataType::Utf8, true),
        Field::new("lmp_type", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// Builds a reader schema from the CSV header and returns the indexes of the raw columns.
fn csv_layout(name: &str, data: &[u8]) -> Result<(SchemaRef, [usize; 4])> {
    let header = data.split(|b| *b == b'\n').next().unwrap_or_default();
    let header = String::from_utf8_lossy(header);
    let columns: Vec<String> = header
        .trim_start_matches('\u{feff}')
        .trim_end()
        .split(',')
        .map(|c| c.trim().trim_matches('"').to_string())
        .collect();
    let find = |wanted: &[&str]| {
        columns
            .iter()
            .position(|c| wanted.contains(&c.as_str()))
            .with_context(|| format!("{name} has no {} column", wanted.join("/")))
    };
    let value = find(&VALUE_COLUMNS)?;
    let keep = [find(&[START_COLUMN])?, find(&[NODE_COLUMN])?, find(&[TYPE_COLUMN])?, value];
    let schema = Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let dt = if i == value { DataType::Float64 } else { DataType::Utf8 };
                Field::new(c, dt, true)
            })
            .collect::<Vec<_>>(),
    );
    Ok((Arc::new(schema), keep))
}

fn read_csvs(csvs: &[(String, Bytes)]) -> Result<Vec<RecordBatch>> {
    let raw = raw_schema();
    let mut batches = Vec::new();
    for (name, data) in csvs {
        let (schema, keep) = csv_layout(name, data)?;
        let reader = ReaderBuilder::new(schema)
            .with_header(true)
            .with_projection(keep.to_vec())
            .with_batch_size(65536)
            .build(Cursor::new(data.clone()))
            .with_context(|| format!("opening {name}"))?;
        for batch in reader {
            let batch = batch.with_context(|| format!("parsing {name}"))?;
            batches.push(RecordBatch::try_new(raw.clone(), batch.columns().to_vec())?);
        }
    }
    Ok(batches)
}

/// Union all OASIS CSVs, pivot LMP_TYPE into `{prefix}*` columns, sort by node, and stamp `downloaded_at`.
pub async fn pivot(
    csvs: &[(String, Bytes)],
    prefix: &str,
    downloaded_at: DateTime<Utc>,
) -> Result<Vec<RecordBatch>> {
    let raw = read_csvs(csvs)?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("raw", Arc::new(MemTable::try_new(raw_schema(), vec![raw])?))?;

    let pivots = LMP_TYPES
        .iter()
        .map(|(t, c)| format!("max(CASE WHEN lmp_type = '{t}' THEN value END) AS {prefix}{c}"))
        .collect::<Vec<_>>()
        .join(",\n  ");
    let sql = format!(
        "SELECT to_timestamp_micros(start) AS utcbegin,
  node,
  {pivots}
FROM raw
GROUP BY 1, 2
ORDER BY node, utcbegin"
    );
    let pivoted = ctx.sql(&sql).await?.collect().await?;

    let target = target_schema(prefix);
    let dl_us = downloaded_at.timestamp_micros();
    pivoted
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| {
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
            for (i, field) in target.fields().iter().take(b.num_columns()).enumerate() {
                cols.push(cast(b.column(i), field.data_type())?);
            }
            cols.push(Arc::new(
                TimestampMicrosecondArray::from_value(dl_us, b.num_rows()).with_timezone("UTC"),
            ));
            Ok(RecordBatch::try_new(target.clone(), cols)?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Float64Array, StringArray};

    const HEADER: &str = "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,OPR_DT,OPR_HR,NODE_ID_XML,NODE_ID,NODE,MARKET_RUN_ID,LMP_TYPE,XML_DATA_ITEM,PNODE_RESMRID,GRP_TYPE,POS,VALUE,OPR_INTERVAL,GROUP";

    fn csv(lmp_type: &str, rows: &[(&str, &str, f64)]) -> (String, Bytes) {
        let mut s = format!("{HEADER}\n");
        for (t, node, v) in rows {
            s.push_str(&format!(
                "{t},x,2026-10-16,01,{node},{node},{node},RTM,{lmp_type},X,{node},ALL,1,{v},1,1\n"
            ));
        }
        (format!("{lmp_type}.csv"), Bytes::from(s))
    }

    #[tokio::test]
    async fn pivots_and_sorts() {
        let t0 = "2026-10-16T07:00:00-00:00";
        let t1 = "2026-10-16T07:05:00-00:00";
        let mut files = Vec::new();
        for (i, (ty, _)) in LMP_TYPES.iter().enumerate() {
            let base = i as f64;
            files.push(csv(ty, &[(t1, "ZZZ", base + 0.5), (t0, "AAA", base), (t0, "ZZZ", base + 0.25)]));
        }
        let dl = Utc::now();
        let out = pivot(&files, "rt", dl).await.unwrap();
        let b = datafusion::arrow::compute::concat_batches(&target_schema("rt"), &out).unwrap();
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.schema(), target_schema("rt"));
        let nodes = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(nodes.iter().flatten().collect::<Vec<_>>(), ["AAA", "ZZZ", "ZZZ"]);
        let ts = b.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(ts.value(1), DateTime::parse_from_rfc3339("2026-10-16T07:00:00Z").unwrap().timestamp_micros());
        assert!(ts.value(1) < ts.value(2));
        let mcl = b.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(mcl.value(0), 3.0);
        assert_eq!(mcl.value(2), 3.5);
        let dla = b.column(7).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(dla.null_count(), 0);
        assert!(dla.iter().all(|v| v == Some(dl.timestamp_micros())));
    }

    const FM_HEADER: &str = "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,OPR_DT,OPR_HR,NODE_ID_XML,NODE_ID,NODE,MARKET_RUN_ID,LMP_TYPE,XML_DATA_ITEM,PNODE_RESMRID,GRP_TYPE,POS,PRC,OPR_INTERVAL,GROUP";
    const DA_HEADER: &str = "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,OPR_DT,OPR_HR,OPR_INTERVAL,NODE_ID_XML,NODE_ID,NODE,MARKET_RUN_ID,LMP_TYPE,XML_DATA_ITEM,PNODE_RESMRID,GRP_TYPE,POS,MW,GROUP";

    #[tokio::test]
    async fn pivots_combined_fm_and_da_layouts() {
        let t0 = "2026-10-16T07:00:00-00:00";
        let fm_row = |ty: &str, v: f64| {
            format!("{t0},x,2026-10-16,01,N1,N1,N1,RTPD,{ty},X,N1,ALL_APNODES,0,{v},1,1\r\n")
        };
        let da_row = |ty: &str, v: f64| {
            format!("{t0},x,2026-10-16,01,0,N1,N1,N1,DAM,{ty},X,N1,ALL_APNODES,0,{v},1\n")
        };
        for (prefix, header, row) in [
            ("fm", FM_HEADER, &fm_row as &dyn Fn(&str, f64) -> String),
            ("da", DA_HEADER, &da_row),
        ] {
            let mut body = format!("{header}\r\n");
            for (i, (ty, _)) in LMP_TYPES.iter().enumerate() {
                body.push_str(&row(ty, i as f64 + 0.5));
            }
            let files = [("all.csv".to_string(), Bytes::from(body))];
            let out = pivot(&files, prefix, Utc::now()).await.unwrap();
            let b = datafusion::arrow::compute::concat_batches(&target_schema(prefix), &out).unwrap();
            assert_eq!(b.num_rows(), 1, "{prefix}");
            assert_eq!(b.schema().field(2).name(), &format!("{prefix}lmp"));
            let nodes = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(nodes.value(0), "N1");
            for i in 0..LMP_TYPES.len() {
                let col = b.column(2 + i).as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(col.value(0), i as f64 + 0.5, "{prefix} {i}");
            }
        }
    }

    #[test]
    fn missing_value_column_is_an_error() {
        let err = csv_layout("x.csv", b"INTERVALSTARTTIME_GMT,NODE,LMP_TYPE,PRICE\n").unwrap_err();
        assert!(err.to_string().contains("VALUE/PRC/MW"));
    }
}
