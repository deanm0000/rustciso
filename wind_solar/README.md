# Wind and solar forecasts and actuals

The `wind_solar` function consumes the `wind-solar` Azure Storage Queue and
writes to [`ciso.wind_solar`](../wind_solar.sql). A job fetches all markets from
OASIS `SingleZip` (`SLD_REN_FCST`, version 1, result format 6).

```json
{
  "startdatetime": "20260701T07:00-0000",
  "stopdatetime": "20260801T07:00-0000",
  "get_prev_until": "20210101T08:00-0000"
}
```

Only `startdatetime` is required. Times must be hour-aligned. The exclusive
`stopdatetime` defaults to the next Pacific month boundary and may not extend
beyond it. The function splits a job into OASIS requests of at most 15 days.
If OASIS reports no data for a multi-day request, the function checks individual
days before treating that range as empty. Interrupted downloads retry the same
request before retrying the entire job.
After a job succeeds, `get_prev_until` (if supplied) queues preceding windows
back to that inclusive start, including when OASIS reports no data. Use an
explicit stop for the current month to limit the job to the desired hours.

The row key is `(utcbegin, market_type, trading_hub, renewable_type)`.
Market names are lowercased; hub and renewable labels retain source casing.
Blank MW values become NULL; negative MW is preserved. Writes replace the
observed range independently for each market; no-data windows leave existing
rows untouched. Partitioned repacking is not supported.

Failures use the [shared retry/poison policy](../rtprices/README.md#retry-policy-all-queue-triggers).
A poisoned job stops its backward chain; requeue the failing window to resume.

For ongoing forward ingestion, supply `startdatetime` and an inclusive,
hour-aligned `get_next_until`; do not combine this with `stopdatetime` or
`get_prev_until`. Forward jobs cover at most one day and never cross a Pacific
month boundary. They wait until each hour has closed plus five minutes. Empty
recent windows retry after five minutes; empty historical windows advance.

```json
{
  "startdatetime": "20261119T13:00-0000",
  "get_next_until": "20400101T00:00-0000"
}
```
