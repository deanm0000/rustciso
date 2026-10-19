//! Per-feed settings for the OASIS LMP price tables.

use crate::oasis::OasisQuery;
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::Los_Angeles;

/// How much data one job loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    /// One UTC hour starting at `startdatetime`.
    Hour,
    /// One America/Los_Angeles operating day (23-25 hours); `startdatetime` must be local midnight.
    Day,
}

#[derive(Debug)]
pub struct Market {
    /// Function folder, queue name and table name (in `lake::SCHEMA`).
    pub name: &'static str,
    /// Prefix for the pivoted LMP columns (`{prefix}lmp`, `{prefix}mcc`, ...).
    pub prefix: &'static str,
    pub query: OasisQuery,
    pub window: Window,
    /// When OASIS is expected to have a window, relative to the window's start.
    pub ready_offset: Duration,
    pub not_ready_retry: std::time::Duration,
}

pub const RTPRICES: Market = Market {
    name: "rtprices",
    prefix: "rt",
    query: OasisQuery {
        queryname: "PRC_INTVL_LMP",
        version: "3",
        market_run_id: "RTM",
        grp_type: "ALL",
    },
    window: Window::Hour,
    // Hour close plus a 5 minute grace period.
    ready_offset: Duration::minutes(65),
    not_ready_retry: std::time::Duration::from_secs(60),
};

pub const FMPRICES: Market = Market {
    name: "fmprices",
    prefix: "fm",
    query: OasisQuery {
        queryname: "PRC_RTPD_LMP",
        version: "3",
        market_run_id: "RTPD",
        grp_type: "ALL_APNODES",
    },
    window: Window::Hour,
    ready_offset: Duration::minutes(65),
    not_ready_retry: std::time::Duration::from_secs(60),
};

pub const DAPRICES: Market = Market {
    name: "daprices",
    prefix: "da",
    query: OasisQuery {
        queryname: "PRC_LMP",
        version: "12",
        market_run_id: "DAM",
        grp_type: "ALL_APNODES",
    },
    window: Window::Day,
    // DAM results post around 13:00 PT the day before; local midnight - 10h is 14:00 PT.
    ready_offset: Duration::hours(-10),
    not_ready_retry: std::time::Duration::from_secs(600),
};

pub const MARKETS: [&Market; 3] = [&RTPRICES, &FMPRICES, &DAPRICES];

fn local_midnight(date: NaiveDate) -> Result<DateTime<Utc>> {
    Los_Angeles
        .from_local_datetime(&date.and_time(NaiveTime::MIN))
        .earliest()
        .map(|t| t.with_timezone(&Utc))
        .with_context(|| format!("no local midnight for {date}"))
}

pub fn local_date(t: DateTime<Utc>) -> NaiveDate {
    t.with_timezone(&Los_Angeles).date_naive()
}

impl Market {
    pub fn table(&self) -> String {
        format!("{}.{}", crate::lake::SCHEMA, self.name)
    }

    pub fn validate_start(&self, start: DateTime<Utc>) -> Result<()> {
        if self.window == Window::Day && local_midnight(local_date(start))? != start {
            bail!(
                "{} startdatetime must be midnight America/Los_Angeles (e.g. 20261016T07:00-0000)",
                self.name
            );
        }
        Ok(())
    }

    /// Exclusive end of the window starting at `start` (= start of the next window).
    pub fn window_end(&self, start: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match self.window {
            Window::Hour => Ok(start + Duration::hours(1)),
            Window::Day => local_midnight(
                local_date(start)
                    .succ_opt()
                    .context("date out of range")?,
            ),
        }
    }

    /// Start of the window before the one starting at `start`.
    pub fn prev_start(&self, start: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match self.window {
            Window::Hour => Ok(start - Duration::hours(1)),
            Window::Day => local_midnight(
                local_date(start)
                    .pred_opt()
                    .context("date out of range")?,
            ),
        }
    }

    pub fn ready_at(&self, start: DateTime<Utc>) -> DateTime<Utc> {
        start + self.ready_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oasis::parse_oasis_time;

    #[test]
    fn daily_windows_follow_local_days_including_dst() {
        for (start, end, prev) in [
            ("20261016T07:00-0000", "20261017T07:00-0000", "20261015T07:00-0000"),
            ("20260308T08:00-0000", "20260309T07:00-0000", "20260307T08:00-0000"),
            ("20261101T07:00-0000", "20261102T08:00-0000", "20261031T07:00-0000"),
            ("20260309T07:00-0000", "20260310T07:00-0000", "20260308T08:00-0000"),
        ] {
            let s = parse_oasis_time(start).unwrap();
            DAPRICES.validate_start(s).unwrap();
            assert_eq!(DAPRICES.window_end(s).unwrap(), parse_oasis_time(end).unwrap());
            assert_eq!(DAPRICES.prev_start(s).unwrap(), parse_oasis_time(prev).unwrap());
        }
        let not_midnight = parse_oasis_time("20261016T08:00-0000").unwrap();
        assert!(DAPRICES.validate_start(not_midnight).is_err());
        RTPRICES.validate_start(not_midnight).unwrap();
        FMPRICES.validate_start(not_midnight).unwrap();
    }

    #[test]
    fn hourly_windows_and_readiness() {
        let s = parse_oasis_time("20261016T07:00-0000").unwrap();
        for m in [&RTPRICES, &FMPRICES] {
            assert_eq!(m.window_end(s).unwrap(), s + Duration::hours(1));
            assert_eq!(m.prev_start(s).unwrap(), s - Duration::hours(1));
            assert_eq!(m.ready_at(s), s + Duration::minutes(65));
        }
        assert_eq!(
            DAPRICES.ready_at(s),
            parse_oasis_time("20261015T21:00-0000").unwrap()
        );
    }

    #[test]
    fn names_are_unique() {
        assert_eq!(RTPRICES.table(), "ciso.rtprices");
        let mut names: Vec<_> = MARKETS.iter().map(|m| m.name).collect();
        names.dedup();
        assert_eq!(names.len(), MARKETS.len());
    }
}
