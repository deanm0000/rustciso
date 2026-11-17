# Nomogram shadow-price queue trigger

The `nomogram_shadows` function consumes the `nomogram-shadows` Azure Queue
and writes `ciso.nomogram_shadows`. The matching intertie function is documented
[here](../intertie_shadows/README.md). Both use the same job format:

```json
{
  "startdatetime": "20261001T07:00-0000",
  "stopdatetime": "20261016T07:00-0000",
  "market_run_id": "dam",
  "get_next_until": "20261017T06:00-0000",
  "get_prev_until": "20260901T07:00-0000"
}
```

Only `startdatetime` and `market_run_id` are required. Times use the OASIS
format shown above; offsets are accepted and continuations serialize as UTC.
`stopdatetime` is exclusive. When omitted, it is midnight at the beginning
of the next `America/Los_Angeles` month, accounting for DST. An explicit
window must be positive and no longer than 31 days. Start/stop times must
align with the selected market's interval.

| `market_run_id` / stored `market_type` | OASIS query | Version | Upstream market | Interval |
|---|---|---|---|---|
| `dam` | `PRC_NOMOGRAM` | 13 | DAM | 60 minutes |
| `hasp` | `PRC_NOMOGRAM` | 13 | HASP | 60 minutes |
| `rtm` | `PRC_NOMOGRAM` | 13 | RTM | 15 minutes |
| `ruc` | `PRC_NOMOGRAM` | 13 | RUC | 60 minutes |
| `interval` | `PRC_RTM_NOMOGRAM` | 1 | RTM | 5 minutes |

Market input is case-insensitive; stored values are lowercase. Requests use
`SingleZip`, `resultformat=6`, and `nomogram_id=ALL`.

## Continuations

- Forward: the next start is the latest returned interval start plus one market
  interval, **not** the requested stop. Its stop defaults to the next Pacific
  month boundary.
- Backward: if the current start is Pacific midnight on the first of a month,
  request the previous full Pacific month, ending at the current start.
  Otherwise request from the first of the current Pacific month up to the
  earliest returned interval start (exclusive).
- `get_next_until` and `get_prev_until` are inclusive bounds on continuation
  **start times**, matching the existing price functions. A backward bound
  should be a Pacific month boundary to include that entire month.
- Children retain only their own direction's bound and reset the retry count.
- With no rows, historical jobs continue as if the full requested range was
  covered: forward starts at the requested stop; backward uses the requested
  start as the minimum.
- A no-data job whose start is later than
  `date_trunc('hour', now() - interval '5 minutes')` (UTC) retries the same
  job after 60 seconds without consuming retries. Continuations for future
  starts are delayed until start + 65 minutes, capped at Azure's seven-day
  visibility limit.
- Other fetch/transform/storage errors use the shared
  [retry/poison policy](../rtprices/README.md#retry-policy-all-queue-triggers),
  with a 60-second retry delay. Queue delivery failures return an error so
  the Functions host can redeliver the original job.

## Storage

Tables must already exist, partitioned by `market_type`, using the definitions
in [shadow_prices.sql](../shadow_prices.sql). Blank dimensions and dimensions
absent from an endpoint become NULL. There is no pivot or aggregation: preserve
each constraint cause and product, including DAM's `IRU` and `IRD`.

Writes replace only the requested market's observed time range, preserving
other market partitions. Empty results do not delete stored rows. dkplus
supports a single identity VARCHAR partition for this operation. It explicitly
rejects overlapping legacy/mixed files or inlined rows that cannot safely be
attributed to that partition, rather than risking deletion of another market.

Table policies are resolved from `ducklake_tag` through dkplus; there are no
column tags or legacy `node` defaults for these tables. Configure at least
`dkplus.time_column=utcbegin`, `dkplus.timezone=America/Los_Angeles`,
`dkplus.row_group_col=nomogram`, and sort columns including the constraint
dimensions and `utcbegin`. For deduplication, use the grain documented in the
SQL file and `dkplus.dedup_timestamp=downloaded_at`. Since a table holds multiple
resolutions, use `unpredictable` rather than a fixed market interval if setting
the optional granularity tag. Partitioned repacking is not yet supported:
the maintenance scheduler explicitly logs and skips these two shadow tables,
and manual repack requests fail rather than publishing incorrectly partitioned
files. Market-scoped ingestion/replacement is supported independently.
