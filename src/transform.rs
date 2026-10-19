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

const CSV_COLUMNS: [&str; 16] = [
    "INTERVALSTARTTIME_GMT",
    "INTERVALENDTIME_GMT",
    "OPR_DT",
    "OPR_HR",
    "NODE_ID_XML",
    "NODE_ID",
    "NODE",
    "MARKET_RUN_ID",
    "LMP_TYPE",
    "XML_DATA_ITEM",
    "PNODE_RESMRID",
    "GRP_TYPE",
    "POS",
    "VALUE",
    "OPR_INTERVAL",
    "GROUP",
];
/// INTERVALSTARTTIME_GMT, NODE, LMP_TYPE, VALUE
const KEEP: [usize; 4] = [0, 6, 8, 13];

/// OASIS LMP_TYPE -> ciso.rtprices column.
pub const LMP_COLUMNS: [(&str, &str); 5] = [
    ("LMP", "rtlmp"),
    ("MCC", "rtmcc"),
    ("MCE", "rtmce"),
    ("MCL", "rtmcl"),
    ("MGHG", "rtmghg"),
];

fn utc_ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

pub fn target_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("utcbegin", utc_ts(), true),
        Field::new("node", DataType::Utf8, true),
    ];
    fields.extend(LMP_COLUMNS.iter().map(|(_, c)| Field::new(*c, DataType::Float64, true)));
    fields.push(Field::new("downloaded_at", utc_ts(), true));
    Arc::new(Schema::new(fields))
}

fn csv_schema() -> SchemaRef {
    Arc::new(Schema::new(
        CSV_COLUMNS
            .iter()
            .map(|c| {
                let dt = if *c == "VALUE" { DataType::Float64 } else { DataType::Utf8 };
                Field::new(*c, dt, true)
            })
            .collect::<Vec<_>>(),
    ))
}

fn read_csvs(csvs: &[(String, Bytes)]) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let schema = csv_schema();
    let mut batches = Vec::new();
    let mut projected = None;
    for (name, data) in csvs {
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_projection(KEEP.to_vec())
            .with_batch_size(65536)
            .build(Cursor::new(data.clone()))
            .with_context(|| format!("opening {name}"))?;
        for batch in reader {
            let batch = batch.with_context(|| format!("parsing {name}"))?;
            projected.get_or_insert_with(|| batch.schema());
            batches.push(batch);
        }
    }
    let projected = projected.unwrap_or_else(|| Arc::new(schema.project(&KEEP).unwrap()));
    Ok((projected, batches))
}

/// Union all OASIS CSVs, pivot LMP_TYPE into columns, sort by node, and stamp `downloaded_at`.
pub async fn pivot(csvs: &[(String, Bytes)], downloaded_at: DateTime<Utc>) -> Result<Vec<RecordBatch>> {
    let (schema, raw) = read_csvs(csvs)?;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("raw", Arc::new(MemTable::try_new(schema, vec![raw])?))?;

    let pivots = LMP_COLUMNS
        .iter()
        .map(|(t, c)| format!("max(CASE WHEN \"LMP_TYPE\" = '{t}' THEN \"VALUE\" END) AS {c}"))
        .collect::<Vec<_>>()
        .join(",\n  ");
    let sql = format!(
        "SELECT to_timestamp_micros(\"INTERVALSTARTTIME_GMT\") AS utcbegin,
  \"NODE\" AS node,
  {pivots}
FROM raw
GROUP BY 1, 2
ORDER BY node, utcbegin"
    );
    let pivoted = ctx.sql(&sql).await?.collect().await?;

    let target = target_schema();
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
        for (i, (ty, _)) in LMP_COLUMNS.iter().enumerate() {
            let base = i as f64;
            files.push(csv(ty, &[(t1, "ZZZ", base + 0.5), (t0, "AAA", base), (t0, "ZZZ", base + 0.25)]));
        }
        let dl = Utc::now();
        let out = pivot(&files, dl).await.unwrap();
        let b = datafusion::arrow::compute::concat_batches(&target_schema(), &out).unwrap();
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.schema(), target_schema());
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
}
