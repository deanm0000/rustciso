-- Shadow-price schemas profiled from ten OASIS SingleZip downloads on 2026-10-27.
-- Each query requested the entire 30-day window in one request:
-- startdatetime=20260703T07:00-0000, enddatetime=20260802T07:00-0000.
-- All queries used resultformat=6.
--
-- Run in DuckDB with your DuckLake catalog attached and selected, for example:
-- LOAD ducklake;
-- ATTACH 'ducklake:/path/to/metadata.ducklake' AS lake;
-- USE lake;
-- DuckLake uses standard CREATE TABLE syntax, not a WITH ducklake suffix.
--
-- Both tables use lowercase market_type values dam, hasp, rtm, ruc, interval,
-- partitioned by market_type. rtm is the 15-minute summary report; interval is
-- the 5-minute report (which also identifies as RTM upstream).
-- dam, hasp and ruc intervals are 60 minutes.
-- Ingestion/replacement must be scoped by market_type and the time window.
-- Empty strings and dimensions absent from a source report become NULL.
-- GROUP is an export grouping ordinal, not a stable constraint identifier.
-- downloaded_at follows the existing price tables' ingestion-time convention.
-- Commented-out definitions document every omitted source column.
-- No uniqueness constraints are imposed; DuckLake does not enforce them.

CREATE SCHEMA IF NOT EXISTS ciso;

-- Sources (SingleZip):
-- PRC_NOMOGRAM v13; nomogram_id=ALL; market_run_id=DAM/HASP/RTM/RUC.
-- PRC_RTM_NOMOGRAM v1; nomogram_id=ALL; market_run_id=RTM.
-- Sample: DAM 11,281; HASP 6,029; RTM 15-minute 20,385; RUC 350;
-- RTM 5-minute 59,741 rows. Combined: 97,786 rows.
-- Grain: market_type, utcbegin, nomogram,
-- constraint_cause, product (unique in this sample).
-- PRODUCT contains IRU/IRD in DAM; empty in other summary markets and absent
-- from the interval report. Retain it to avoid merging distinct DAM prices.
CREATE TABLE ciso.nomogram_shadows (
    utcbegin TIMESTAMPTZ, -- INTERVALSTARTTIME_GMT
    market_type VARCHAR, -- MKT_TYPE (summary, lowercased) / MARKET_RUN_ID (interval RTM mapped to interval)
    -- utcend TIMESTAMPTZ, -- INTERVALENDTIME_GMT: utcbegin + market_type interval.
    -- opr_dt DATE, -- OPR_DT (interval only): Pacific operating date derived from utcbegin.
    -- opr_hr INTEGER, -- OPR_HR (interval only): Pacific hour + 1, derived from utcbegin.
    -- opr_interval INTEGER, -- OPR_INTERVAL (interval only): floor(minute / 5) + 1.
    nomogram VARCHAR, -- NOMOGRAM_ID
    constraint_cause VARCHAR, -- CONSTRAINT_CAUSE
    product VARCHAR, -- PRODUCT (summary only); NULL when empty or absent.
    shadow_price DOUBLE, -- PRICE (summary) / PRC (interval)
    -- "group" BIGINT, -- GROUP: export grouping ordinal.
    downloaded_at TIMESTAMPTZ -- Generated at ingestion; no OASIS source column.
);

ALTER TABLE ciso.nomogram_shadows SET PARTITIONED BY (market_type);

-- Sources (SingleZip):
-- PRC_CNSTR v13; ti_id=ALL; ti_direction=ALL; market_run_id=DAM/HASP/RTM/RUC.
-- PRC_RTM_FLOWGATE v12; ti_id=ALL; ti_direction=ALL; market_run_id=RTM.
-- Sample: DAM 988; HASP 2,011; RTM 15-minute 4,806; RUC 997;
-- RTM 5-minute 13,261 rows. Combined: 22,063 rows.
-- Grain: market_type, utcbegin, intertie, direction,
-- limit_type, constraint_cause, product (unique in this sample).
-- LIMIT_TYPE contains EA/AS/empty in DAM and RUC, EA/empty in RTM, and is
-- entirely empty in HASP. PRODUCT is empty in all summary rows and absent
-- from the interval report; retain it for source-schema consistency.
CREATE TABLE ciso.intertie_shadows (
    utcbegin TIMESTAMPTZ, -- INTERVALSTARTTIME_GMT
    market_type VARCHAR, -- MKT_TYPE (summary, lowercased) / MARKET_RUN_ID (interval RTM mapped to interval)
    -- utcend TIMESTAMPTZ, -- INTERVALENDTIME_GMT: utcbegin + market_type interval.
    -- opr_dt DATE, -- OPR_DT (interval only): Pacific operating date derived from utcbegin.
    -- opr_hr INTEGER, -- OPR_HR (interval only): Pacific hour + 1, derived from utcbegin.
    -- opr_interval INTEGER, -- OPR_INTERVAL (interval only): floor(minute / 5) + 1.
    intertie VARCHAR, -- TRANSMISSION_INTERFACE_ID (summary) / TI_ID (interval)
    direction VARCHAR, -- DIRECTION (summary) / TI_DIRECTION (interval)
    limit_type VARCHAR, -- LIMIT_TYPE; NULL when empty.
    constraint_cause VARCHAR, -- CONSTRAINT_CAUSE
    product VARCHAR, -- PRODUCT (summary only); NULL when empty or absent.
    shadow_price DOUBLE, -- PRICE (summary) / PRC (interval)
    -- "group" BIGINT, -- GROUP: export grouping ordinal.
    downloaded_at TIMESTAMPTZ -- Generated at ingestion; no OASIS source column.
);

ALTER TABLE ciso.intertie_shadows SET PARTITIONED BY (market_type);
