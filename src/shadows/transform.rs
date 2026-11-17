use super::{Market, Report, Window};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Builder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::io::Cursor;
use std::sync::Arc;

pub(super) struct Normalized {
    pub batches: Vec<RecordBatch>,
    pub observed: Option<Window>,
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn dimensions(report: Report) -> &'static [(&'static str, &'static [&'static str])] {
    match report {
        Report::Nomogram => &[
            ("nomogram", &["NOMOGRAM_ID"]),
            ("constraint_cause", &["CONSTRAINT_CAUSE"]),
            ("product", &["PRODUCT"]),
        ],
        Report::Intertie => &[
            ("intertie", &["TRANSMISSION_INTERFACE_ID", "TI_ID"]),
            ("direction", &["DIRECTION", "TI_DIRECTION"]),
            ("limit_type", &["LIMIT_TYPE"]),
            ("constraint_cause", &["CONSTRAINT_CAUSE"]),
            ("product", &["PRODUCT"]),
        ],
    }
}

fn target_schema(report: Report) -> SchemaRef {
    let mut fields = vec![
        Field::new("utcbegin", timestamp_type(), true),
        Field::new("market_type", DataType::Utf8, true),
    ];
    fields.extend(
        dimensions(report)
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Utf8, true)),
    );
    fields.push(Field::new("shadow_price", DataType::Float64, true));
    fields.push(Field::new("downloaded_at", timestamp_type(), true));
    Arc::new(Schema::new(fields))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("{name} must be a string column"))
}

fn required<'a>(column: &'a StringArray, row: usize, name: &str) -> Result<&'a str> {
    ensure!(
        !column.is_null(row) && !column.value(row).is_empty(),
        "empty {name} at CSV data row {}",
        row + 1
    );
    Ok(column.value(row))
}

pub(super) async fn normalize(
    csvs: &[(String, Bytes)],
    report: Report,
    market: Market,
    window: Window,
    downloaded_at: DateTime<Utc>,
) -> Result<Normalized> {
    let target = target_schema(report);
    let dims = dimensions(report);
    let mut batches = Vec::new();
    let mut observed: Option<Window> = None;
    for (name, data) in csvs {
        let columns = crate::transform::csv_columns(data);
        ensure!(
            columns
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                == columns.len(),
            "{name}: duplicate CSV column names"
        );
        let has = |column: &&str| columns.iter().any(|name| name == column);
        let start_name = "INTERVALSTARTTIME_GMT";
        let end_name = "INTERVALENDTIME_GMT";
        let market_name = if market == Market::Interval {
            "MARKET_RUN_ID"
        } else {
            "MKT_TYPE"
        };
        let price_name = if market == Market::Interval {
            "PRC"
        } else {
            "PRICE"
        };
        for column in [start_name, end_name, market_name, price_name] {
            ensure!(has(&column), "{name}: missing {column}");
        }
        let selected = dims
            .iter()
            .map(|(target, sources)| {
                let source = sources.iter().copied().find(has);
                ensure!(
                    source.is_some() || (*target == "product" && market == Market::Interval),
                    "{name}: missing source column for {target}"
                );
                Ok(source)
            })
            .collect::<Result<Vec<_>>>()?;
        let raw_schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let reader = ReaderBuilder::new(raw_schema)
            .with_header(true)
            .with_batch_size(65536)
            .build(Cursor::new(data.clone()))
            .with_context(|| format!("opening {name}"))?;
        for raw in reader {
            let raw = raw.with_context(|| format!("parsing {name}"))?;
            if raw.num_rows() == 0 {
                continue;
            }
            let starts = string_column(&raw, start_name)?;
            let ends = string_column(&raw, end_name)?;
            let markets = string_column(&raw, market_name)?;
            let prices = string_column(&raw, price_name)?;
            let dimension_columns = selected
                .iter()
                .map(|name| name.map(|name| string_column(&raw, name)).transpose())
                .collect::<Result<Vec<_>>>()?;
            let mut time_builder = TimestampMicrosecondBuilder::new();
            let mut dim_builders: Vec<_> = dims.iter().map(|_| StringBuilder::new()).collect();
            let mut price_builder = Float64Builder::new();
            for row in 0..raw.num_rows() {
                let start = DateTime::parse_from_rfc3339(required(starts, row, start_name)?)
                    .with_context(|| format!("{name}: invalid {start_name}"))?
                    .with_timezone(&Utc);
                let end = DateTime::parse_from_rfc3339(required(ends, row, end_name)?)
                    .with_context(|| format!("{name}: invalid {end_name}"))?
                    .with_timezone(&Utc);
                ensure!(
                    start >= window.start && start < window.end && end <= window.end,
                    "{name}: interval [{start}, {end}) is outside requested window"
                );
                ensure!(
                    end - start == market.tick()
                        && start.timestamp_subsec_nanos() == 0
                        && start.timestamp().rem_euclid(market.tick().num_seconds()) == 0,
                    "{name}: unexpected interval for {}: [{start}, {end})",
                    market.name()
                );
                ensure!(
                    required(markets, row, market_name)? == market.upstream(),
                    "{name}: {market_name} does not match requested {}",
                    market.upstream()
                );
                let price: f64 = required(prices, row, price_name)?
                    .parse()
                    .with_context(|| format!("{name}: invalid {price_name}"))?;
                ensure!(price.is_finite(), "{name}: nonfinite {price_name}");
                time_builder.append_value(start.timestamp_micros());
                price_builder.append_value(price);
                for (i, column) in dimension_columns.iter().enumerate() {
                    if let Some(column) = column {
                        if matches!(dims[i].0, "nomogram" | "intertie" | "direction") {
                            required(column, row, dims[i].0)?;
                        }
                        if !column.is_null(row) && !column.value(row).is_empty() {
                            dim_builders[i].append_value(column.value(row));
                            continue;
                        }
                    }
                    dim_builders[i].append_null();
                }
                observed = Some(match observed {
                    Some(previous) => Window {
                        start: previous.start.min(start),
                        end: previous.end.max(end),
                    },
                    None => Window { start, end },
                });
            }
            let mut arrays: Vec<ArrayRef> = vec![
                Arc::new(time_builder.finish().with_timezone("UTC")),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    market.name(),
                    raw.num_rows(),
                ))),
            ];
            arrays.extend(
                dim_builders
                    .iter_mut()
                    .map(|builder| Arc::new(builder.finish()) as ArrayRef),
            );
            arrays.push(Arc::new(price_builder.finish()));
            arrays.push(Arc::new(
                TimestampMicrosecondArray::from_value(
                    downloaded_at.timestamp_micros(),
                    raw.num_rows(),
                )
                .with_timezone("UTC"),
            ));
            batches.push(RecordBatch::try_new(target.clone(), arrays)?);
        }
    }
    if batches.is_empty() {
        return Ok(Normalized { batches, observed });
    }
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table(
        "raw",
        Arc::new(MemTable::try_new(target.clone(), vec![batches])?),
    )?;
    let order = dims
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    let sorted = ctx
        .sql(&format!(
            "SELECT * FROM raw ORDER BY market_type, {order}, utcbegin"
        ))
        .await?
        .collect()
        .await?;
    let batches = sorted
        .into_iter()
        .map(|batch| {
            let arrays = batch
                .columns()
                .iter()
                .zip(target.fields())
                .map(|(array, field)| cast(array, field.data_type()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(RecordBatch::try_new(target.clone(), arrays)?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Normalized { batches, observed })
}
