//! CISO policy adapters for the reusable dkplus storage library.
use crate::{
    config::Config,
    report::{REPORTS, RTPRICES, Report},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;

pub mod maintenance;
pub mod repack;

pub const SCHEMA: &str = "ciso";

pub fn layout(report: &Report) -> dkplus::Layout {
    dkplus::Layout {
        schema: SCHEMA.into(),
        time_column: "utcbegin".into(),
        time_type: dkplus::TimeType::TimestampTz,
        timezone: chrono_tz::America::Los_Angeles,
        lock_timezone: chrono_tz::America::Los_Angeles,
        row_group_col: report.row_group_col.map(str::to_owned),
        sort_cols: report.sort_cols.iter().map(|name| (*name).into()).collect(),
        dedup_timestamp: "downloaded_at".into(),
        author: "rustciso".into(),
        lock_namespace: "rustciso.repack".into(),
    }
}

pub fn repack_layout(table: &str) -> dkplus::Layout {
    let (schema, name) = table.split_once('.').unwrap_or((SCHEMA, table));
    let report = REPORTS
        .iter()
        .copied()
        .find(|report| schema == SCHEMA && name == report.name)
        .unwrap_or(&RTPRICES);
    let mut options = layout(report);
    options.schema = schema.into();
    options
}

pub async fn replace_window(
    cfg: &Config,
    table: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    batches: Vec<RecordBatch>,
) -> Result<dkplus::LoadReport> {
    dkplus::replace_window(
        &cfg.lake(),
        &repack_layout(table),
        table,
        &dkplus::Window::TimestampTz { start, end },
        batches,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ciso_policy_and_manual_table_fallback_remain_application_owned() {
        for report in REPORTS {
            let options = repack_layout(&report.table());
            assert_eq!(options.schema, "ciso");
            assert_eq!(options.time_column, "utcbegin");
            assert_eq!(options.time_type, dkplus::TimeType::TimestampTz);
            assert_eq!(options.timezone, chrono_tz::America::Los_Angeles);
            assert_eq!(options.lock_timezone, options.timezone);
            assert_eq!(options.lock_namespace, "rustciso.repack");
            assert_eq!(options.author, "rustciso");
            assert_eq!(options.dedup_timestamp, "downloaded_at");
            assert_eq!(options.row_group_col.as_deref(), report.row_group_col);
            assert_eq!(options.sort_cols, report.sort_cols);
            options.validate().unwrap();
        }
        let options = repack_layout("other.manual_table");
        assert_eq!(options.schema, "other");
        assert_eq!(options.sort_cols, ["node", "utcbegin"]);
        assert_eq!(repack_layout("manual_table").schema, "ciso");
    }
}
