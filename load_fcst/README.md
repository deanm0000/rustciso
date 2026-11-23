# CAISO demand forecasts and actual load

The `load_fcst` queue trigger reads OASIS `SLD_FCST` once per window and
writes hourly data to `ciso.load_fcst`, 15-minute RTPD data to
`ciso.load_fcst_rtpd`, and 5-minute RTD data to `ciso.load_fcst_rtm`
([table definitions](../load_fcst.sql)).
The hourly table includes `ACTUAL`. Market and execution values retain source
casing. `downloaded_at` is the ingestion time, not a forecast issue time.

Send a base64-encoded JSON message to the `load-fcst` queue, for example:

```json
{
  "startdatetime": "20261101T07:00-0000",
  "stopdatetime": "20261119T08:00-0000",
  "get_prev_until": "20210101T08:00-0000"
}
```

Only `startdatetime` is required. Times must be hour-aligned. The exclusive
`stopdatetime` defaults to the next Pacific month boundary and may not extend
beyond it. A successful job queues the previous window back to the inclusive
`get_prev_until`, if specified, with a six-second delay to avoid starting the
next request immediately after the previous window. Requests cover at most 15 days; multi-day
no-data responses are checked day by day. Empty windows leave stored data
unchanged. Each chunk replaces the observed time range in each hourly market
partition and each real-time table; reruns are safe. Unexpected market,
execution, or interval combinations fail rather than being silently discarded.
Conflicting duplicate rows fail; blank MW becomes NULL.

Failures use the [shared retry/poison policy](../rtprices/README.md#retry-policy-all-queue-triggers).
A poisoned job stops its backward chain; requeue the failing window to resume.
Partitioned hourly repacking is not supported.

For ongoing forward ingestion, use `startdatetime` and the inclusive,
hour-aligned `get_next_until` instead of `stopdatetime` or `get_prev_until`.
Forward jobs cover up to one day at a time without crossing a Pacific month
boundary. They wait until each hour has closed plus five minutes. An empty
recent window is retried after five minutes; empty historical windows advance.
For example:

```json
{
  "startdatetime": "20261119T08:00-0000",
  "get_next_until": "20400101T00:00-0000"
}
```
