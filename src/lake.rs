//! CISO table-policy resolution and adapters for the reusable dkplus library.
use crate::logging::InvocationLog;
use crate::{config::Config, report::REPORTS};
use anyhow::Result;
use datafusion::arrow::array::RecordBatch;

pub mod maintenance;
pub mod repack;

pub const SCHEMA: &str = "ciso";

pub fn table_builder(cfg: &dkplus::Config, table: &str) -> dkplus::TableBuilder {
    let (schema, name) = table.split_once('.').unwrap_or((SCHEMA, table));
    let builder = dkplus::Table::builder(&format!("{schema}.{name}"), cfg);
    if schema == SCHEMA && REPORTS.iter().any(|report| report.name == name) {
        builder
    } else {
        builder
            .with_time_column("utcbegin")
            .with_timezone(chrono_tz::America::Los_Angeles)
            .with_row_group_col("node")
            .with_sort_cols(vec!["node".into(), "utcbegin".into()])
            .with_dedup_timestamp("downloaded_at")
            .with_author("rustciso")
            .with_lock_namespace("rustciso.repack")
    }
}

pub async fn resolve_table(cfg: &dkplus::Config, table: &str) -> Result<dkplus::Table> {
    Ok(table_builder(cfg, table).resolve().await?)
}

pub async fn replace_window(
    cfg: &Config,
    table: &str,
    batches: Vec<RecordBatch>,
    log: &InvocationLog,
) -> Result<dkplus::LoadReport> {
    Ok(resolve_table(&cfg.lake(log.clone()), table)
        .await?
        .replace_window(batches)
        .await?)
}
