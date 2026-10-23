# Hourly RT prices queue trigger

See also [fmprices](../fmprices/README.md) and [daprices](../daprices/README.md),
which share this implementation ([src/prices.rs](../src/prices.rs), configured
in [src/report.rs](../src/report.rs)).

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
Retries do not emit continuations or repack jobs.

## Retry policy (all queue triggers)

All price and repack queue jobs accept optional nonnegative integer fields:

- `retries`: retries already scheduled for this job; omitted or null means `0`.
- `max_retries`: retries allowed after the initial attempt; omitted or null
  means `3` (four total attempts). Set `0` to disable failure retries.

Each failure requeue increments `retries` and preserves the other options.
Price failures retain their existing delay: 60 seconds, or one hour when the
queue has at least five messages. Repack failures retry after 60 seconds.
Once the limit is reached, the failed job is sent to `<queue>-poison`
(created if necessary), with its payload and retry count intact, and the
reason is logged. No further retry or continuation is scheduled.
Successful continuation and price-generated repack jobs inherit `max_retries`
but start with a fresh retry count. Maintenance-generated repack jobs use
the defaults.

OASIS "no data" responses for dates before today in `America/Los_Angeles`
stop immediately, logging the reason without requeuing, poisoning, or emitting
continuations. For today's date or future dates, they keep polling without
incrementing `retries` or consuming the retry allowance (every minute for RT/FMM,
every ten minutes for DAM).

OASIS ZIP responses containing error `1015` ("GroupZip DownLoad is in
Processing") are retried every five seconds for up to one minute after the
first processing response. This limit includes subsequent requests and
response reads. If the limit expires, the load fails and the original job
is retried through the usual failure path. Other OASIS errors are unchanged.

If `repack_if_first` is true and the loaded hour is the first hour of its
`America/Los_Angeles` date, or `repack_if_last` is true and it is the last hour,
one message is sent immediately to the [repack queue](../repack/README.md).
It contains `table: "ciso.rtprices"`, the half-open local-date interval
`[that date, the next date)`. Repack deduplication and output layout come from
the table's DuckLake dkplus tags.
Daylight-saving days with 23 or 25 hours are handled using local date
boundaries. A first-hour repack does not wait for the rest of that day's hours
to load; it repacks the data currently available.

Queue-send failures fail the invocation so the Functions host retries it.
Sends to multiple queues are not atomic, so host retries can duplicate
continuations or repack requests that were already sent.

## Local verification

```sh
cargo test --bin rustciso -- --skip lake::
```
