# Hourly RT prices queue trigger

Enqueue a base64-encoded JSON message on the `rtprices` Azure Storage queue
(`AzureWebJobsStorage`):

```json
{
  "startdatetime": "20261016T07:00-0000",
  "get_next_until": "20261017T06:00-0000",
  "get_prev_until": "20261015T07:00-0000",
  "repack_if_last": true,
  "repack_if_first": true
}
```

Only `startdatetime` is required. All datetime strings use the same OASIS
format, `YYYYMMDDTHH:MM+HHMM` or `YYYYMMDDTHH:MM-HHMM`, including the UTC
offset. Bounds are compared as instants, not as strings.

After successfully loading the hour:

- `get_next_until`, when non-null, queues the hour one hour later unless its
  start is after the bound.
- `get_prev_until`, when non-null, queues the hour one hour earlier unless its
  start is before the bound.
- Both bounds are inclusive. Setting both can emit two continuation messages.
  Each child retains only its own direction's bound, so the chains do not
  bounce back and forth. The former boolean `get_next` field has been replaced
  by `get_next_until`; update existing producers and queued jobs to use the
  datetime bound.
- Both repack flags are copied unchanged to every continuation. Missing, null,
  or false flags disable that repack condition.

Continuations wait until their hour has closed plus five minutes, with queue
visibility capped at seven days. Historical hours are immediately visible.
Not-ready responses and load failures retry the original job with all its
options intact; they do not emit continuations or repack jobs.

If `repack_if_first` is true and the loaded hour is the first hour of its
`America/Los_Angeles` date, or `repack_if_last` is true and it is the last hour,
one message is sent immediately to the [repack queue](../repack/README.md).
It contains `table: "ciso.rtprices"` and the half-open local-date interval
`[that date, the next date)`. Daylight-saving days with 23 or 25 hours are
handled using local date boundaries. A first-hour repack does not wait for
the rest of that day's hours to load; it repacks the data currently available.

Queue-send failures fail the invocation so the Functions host retries it.
Sends to multiple queues are not atomic, so host retries can duplicate
continuations or repack requests that were already sent.

## Local verification

```sh
cargo test --bin rustciso tests:: -- --skip lake::
```
