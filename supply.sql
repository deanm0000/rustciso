-- CAISO Today's Outlook fuelsource.csv; values are five-minute average MW.
-- Pacific calendar dates come from /outlook/history/YYYYMMDD/fuelsource.csv.
-- Fall-back's single source hour is copied to both UTC occurrences.
-- Empty rows (including spring-forward's nonexistent hour) are skipped.
-- A trailing next-day 00:00 row is excluded in favor of the next daily file.
--
-- Queue "supply" backfill example (inclusive dates, up to 31 days per job):
-- {"mode":"backfill","date":"2026-11-18","until":"2021-01-01"}
-- Continuations run immediately; missing days are logged and skipped. A 404
-- is confirmed without the cache-buster before treating a file as missing.
-- An entirely empty job window stops the backfill; one missing day does not
-- establish the end of history. Other HTTP, parsing, and write errors retry
-- and eventually go to supply-poison without advancing the date.
--
-- After verifying the backfill, seed one forward chain starting yesterday:
-- {"mode":"forward","date":"2026-11-18"}
-- This refreshes yesterday and today every five minutes, including late
-- updates to yesterday, and catches up automatically after downtime.
-- Messages use the existing retry envelope and Azure Functions base64 encoding.

CREATE TABLE ciso.supply (
    utcbegin TIMESTAMPTZ,
    solar INT,
    wind INT,
    geothermal INT,
    biomass INT,
    biogas INT,
    small_hydro INT,
    coal INT,
    nuclear INT,
    natural_gas INT,
    large_hydro INT,
    batteries INT,
    imports INT,
    other INT,
    downloaded_at TIMESTAMPTZ
) USING ducklake;
