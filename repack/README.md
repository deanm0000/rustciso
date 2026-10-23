# Parquet repack queue trigger

## Shared storage library

The DuckLake catalog, Parquet writer, external sorting, and repack/fastpack
implementation live in [`dkplus`](../../../modules/dkplus/README.md).
This app retains the Azure trigger/queue contract. Table policy for
`ciso.rtprices`, `ciso.fmprices`, and `ciso.daprices` is resolved from their
DuckLake `dkplus.*` tags; see the [dkplus tag settings](../../../modules/dkplus/README.md#catalog-configuration-and-operations).
[`src/lake.rs`](../src/lake.rs) and
[`src/lake/repack.rs`](../src/lake/repack.rs) are application adapters only.

The Cargo path dependency is `../../modules/dkplus`, relative to this app's
manifest. Local and CI release builds must make the sibling crate available at
that relative location (currently `~/pyjs/modules/dkplus`). The deployed Functions
binary does not need the crate's source. The existing
[`compile_deploy.sh`](../compile_deploy.sh) build/publish commands are unchanged.
Keep both directories when moving/checking out this app; cloning rustciso alone
is not sufficient for a source build.

Enqueue a base64-encoded JSON message on the `repack` Azure Storage queue
(`AzureWebJobsStorage`), just like the existing `rtprices` queue:

```json
{
  "table": "rtprices",
  "begin": {"tz": "2026-01-01T08:00:00Z"},
  "end": {"tz": "2026-10-01T07:00:00Z"}
}
```

`begin` and `end` are `dkplus::repack::Bound` values and define the half-open
interval `[begin, end)`. `table` defaults to the `ciso` schema;
`ciso.rtprices` is also accepted. The older `begin_date`/`end_date` format is
still accepted for queued messages during rollout; those dates are converted
to `America/Los_Angeles` midnight with daylight-saving rules applied to each
boundary. Stored `utcbegin` timestamps remain UTC. For example, the legacy
range `2026-01-01` through `2026-10-01` becomes
`[2026-01-01T08:00:00Z, 2026-10-01T07:00:00Z)`. A local day spanning a
daylight-saving transition can contain 23 or 25 hours.

The maintenance timer sends serialized `dkplus::maintenance::PotentialRepack`
plans on this same queue. These include the rule level, granularity, bounds,
and source paths. The worker rechecks each plan against current catalog
statistics before repacking; stale plans are logged and skipped.

Optional `retries` and `max_retries` use the shared
[queue retry policy](../rtprices/README.md#retry-policy-all-queue-triggers).
Failures retry after 60 seconds, incrementing `retries`; by default three retries
are allowed after the initial attempt. Exhausted jobs move to `repack-poison`
with their payload intact and the failure reason logged.

`unique` is an optional list of column names that overrides the table's
`dkplus.unique_by_cols` tag. When omitted or `null`, the table-tag policy is
used. For the three CISO price tables this is configured in DuckLake, including
the `downloaded_at` deduplication timestamp. When deduplication is enabled, only
rows within the requested interval are deduplicated; the entire row with the
latest timestamp is retained. Equal timestamps are broken by the highest
DuckLake row ID, null timestamps sort behind non-null timestamps, and null key
values compare equal. An explicit override must be nonempty, contain distinct
existing columns, and the table must have the configured microsecond timestamp.

`files` is an optional list of **catalog paths**, exactly as stored in
`ducklake.ducklake_data_file.path` (not resolved blob URLs). For example:

```json
"files": ["daily-01.parquet", "daily-02.parquet"]
```

For manual jobs, after acquiring the range's advisory locks the worker checks
the current snapshot for these paths in the requested table. If **any** are
still active (`end_snapshot IS NULL`), the repack proceeds normally. If none
remain, it logs a successful skip. An empty list also skips. Omit `files` or
set it to `null` to bypass this guard, preserving existing manually queued and
price-triggered jobs. The list is a freshness guard, not a filter: all current
files overlapping the window are still selected for repacking. Maintenance
plans additionally revalidate their full planning rules before execution.

## Output layout

The three price tables' `dkplus.sort_cols` and `dkplus.row_group_col` tags
configure the ascending output order and node row grouping. A row-group column
must be `varchar` and the first sort column. Manually named tables outside the
report registry retain the existing price-table layout and schema requirements
through explicit application defaults.

- One nonempty Parquet file contains all live rows in the requested interval,
  sorted ascending by `(node, utcbegin)`, with ZSTD level 3 compression.
- For intervals shorter than one calendar month, whole nodes are packed together.
  Row groups target the Parquet default of 1,048,576 rows or approximately
  128 MiB of encoded, uncompressed data. Flushing waits until a node ends, so a
  node never spans row groups, even if that makes a group exceed the target.
- For one calendar month or longer, each row group contains only one node.
  Large nodes can span multiple groups at the same size targets. The `node`
  statistics are untruncated, with `min = max` for each group.
  The calendar-month cutoff uses local dates and the same day next month, clamped to that
  month's last day (January 31 through February 28 counts as a month).
- Files crossing a date boundary are split logically before compaction. Their
  outside rows are preserved in up to two additional sorted files: one wholly
  before the interval, one wholly after it. Outside files also use node-only
  groups. An empty region produces no file.

All replacements are published in one atomic DuckLake compaction snapshot.
Sources remain available for time travel; this trigger never physically deletes
source data or delete files. Embedded DuckLake row IDs and original row snapshot
IDs of surviving rows are preserved. Logical record counts decrease by the
number of duplicates removed. `next_row_id` does not change, and table-wide
value statistics remain conservative bounds.

The [hourly RT prices trigger](../rtprices/README.md) can also enqueue a
single-day repack after successfully processing the first or last Los Angeles
hour of that day, using its `repack_if_first` and `repack_if_last` flags.

The [table maintenance timer](../maintenance/README.md) checks all three price
tables every 30 minutes and enqueues eligible month-grouped ranges with
the discovered `files` list. Deduplication and output layout come from the
table's dkplus tags.

## Azure runtime and failure behavior

The repacker first attempts an aligned row-group path when the report defines a
`row_group_col` (currently `node`). It opens each source Parquet reader,
inspects row-group min/max statistics, and requires exact non-null bounds and
ordered row groups within each source. Different sources may have different
boundaries and overlapping node ranges: the worker splits each row group by
actual node values, retains unconsumed tails, and only emits a node after it has
collected that node from every source. Actual values are checked against the
statistics and ordering before use. This path writes replacement Parquet
directly, without creating temporary sort-run blobs. A `unique` job can use it
only when `unique` includes the `row_group_col`; deduplication then happens
while that node is buffered.

Metadata opens and row-group loads use up to eight concurrent source reads.
Parquet column byte ranges are fetched through the object store's batched range
API. Once a node is assembled, the worker also reads the next row groups for
exhausted source cursors while writing the current node. Read-ahead is
opportunistic: reservations include buffered source groups, the current output
batches, and in-flight row-group size estimates within `REPACK_NODE_MB`.
If there is no room for speculative reads, they wait until the output batches
are released. There are no detached read tasks left running after fallback.

Fastpack logs include the table and half-open local date range to distinguish
daily and yearly jobs. They report metadata progress every 16 sources or 15 seconds, the
first completed node, and then every 5000 nodes or 15 seconds of processing.
Node progress includes live rows, duplicates removed, row groups read/total,
buffered bytes, and elapsed time. While awaiting metadata, row-group reads,
writes, or finalization, a wait message is emitted every 15 seconds.
Success is logged after output finalization and row-count validation; catalog
publication and cleanup follow separately. Progress is streamed to stderr
(Live Log Stream) and buffered for the custom-handler response, so response
logs still reach Application Insights only when the invocation returns.

If statistics are missing/inexact, values do not match their declared order,
or a node's working set exceeds the configured limit, the worker discards any
unpublished aligned-path output and restarts through the normal external sort.
The external sorter flushes approximately `REPACK_RUN_MB` of Arrow arrays
per sorted run (default 128 MiB) and uses eight-way streaming merge passes.
Intermediate Parquet runs are stored beneath the table's
`_tmp/repack-<invocation-id>/` blob prefix, not on local Functions temp disk.
`REPACK_NODE_MB` sets the aligned path's per-node working-set limit (default
256 MiB).

Set `REPACK_RUN_MB` and `REPACK_NODE_MB` as positive whole MiB counts
in the Function App configuration (1 MiB = 1,048,576 bytes), for example
`REPACK_RUN_MB=128` and `REPACK_NODE_MB=256`. These replace the former
`REPACK_RUN_BYTES` and `REPACK_NODE_BYTES` settings, which are no longer read.
Missing, malformed, zero, or overflowing values use the defaults.
These are memory targets, not total process memory caps: decoding,
sorting, merge readers, Parquet metadata, open row groups, output encoding, and
temporary copies also use memory. The aligned path retains one current row
group per source while reconciling boundaries, so it may exceed its node target
briefly; exceeding the target triggers the external-sort fallback. Short-range
groups must retain a whole node regardless of size.
External-sort deduplication uses a streaming pass sorted by the unique columns
and descending download timestamp, followed by the normal `(node, utcbegin)`
output sort.
Parquet footer reads use a blob-size lookup and explicit bounded byte ranges;
Azure Blob Storage does not support suffix-range requests.

The app's `functionTimeout` is `-1`, which **requires an Azure Functions hosting
plan supporting unbounded execution**. Azure platform restarts and scale-in can
still interrupt an invocation. This setting applies to the whole Function App.
No durable continuation/checkpoint is implemented; a retry starts sorting again.
Jobs carrying `files` skip retries when their original files are no longer active.

Every repack acquires one blocking, session-scoped Postgres advisory lock per
UTC calendar date covered by the requested time range before reading the
catalog. Locks are keyed by the table's lock namespace, schema/table name, and
UTC date; the namespace defaults to the table name. Overlapping ranges on a
table wait for one another; disjoint ranges can run concurrently. Different
tables can also run concurrently. Locks are acquired in chronological order.
The same connection holds all locks through sorting, commit retries and cleanup,
without keeping a transaction open during sorting. Normal returns, errors and
disconnects release the locks when that connection closes, including partially
acquired ranges. Waiting jobs check `files` only after acquiring all locks.
These locks coordinate repack workers, not ordinary ingestion writers;
existing commit-time source/schema validation remains in place.
Disjoint ranges can still select the same boundary-straddling Parquet file; a
conflicting publication fails the existing source recheck and retries. Each
UTC day consumes an advisory lock, so unusually long ranges may require
Postgres lock-capacity tuning or smaller jobs.

Los Angeles date ranges can cross UTC midnight, so a local-day job can acquire
locks for two UTC dates. Adjacent local days still serialize if their UTC date
sets overlap.

Progress is written immediately to the handler console, rather than waiting for
the invocation response: catalog checks, each source file, sorted runs, merge
passes, output sizes, commit attempts and cleanup. Logs include the unique
temporary prefix and elapsed times. Handled failures include the error chain in
the custom-handler response. Inlined timestamps are cast from DuckLake's text
representation before checking the requested UTC interval.

On normal success or handled failure, temporary runs are deleted. If an
invocation is terminated, its temporary prefix can remain; configure an
appropriate storage lifecycle policy, allowing more time than the longest job.
Do not expire active jobs' runs.

Files and schema are rechecked before publishing. Changed sources cause an
explicit failure and bounded queue retry, rather than overwriting concurrent writes.
Snapshot allocation conflicts are retried without re-sorting. Candidate final
files are retained on commit errors, since a lost COMMIT response might mean
they were published. Only remove such files after confirming they are
unreferenced by **every retained snapshot**. The queue retry policy above sends
exhausted jobs to `repack-poison`. If sending a retry or poison message fails,
the invocation fails so the host can redeliver or poison the original message
according to `maxDequeueCount` in [`host.json`](../host.json).

## Current catalog support

The trigger uses this app's existing Postgres DuckLake catalog and `Synblob`
storage configuration. Supported tables are unpartitioned, Azure-backed tables
with `varchar node` and microsecond `timestamptz utcbegin`. The shared library also
supports boolean, signed/unsigned 8/16/32/64-bit integers, float32/float64,
varchar, Date32 dates, and naive/aware microsecond timestamps as data columns.
File columns must carry DuckLake Parquet field IDs. Null window/group keys
are rejected. The three price-table settings are resolved from their DuckLake
dkplus tags; other scrapers can configure their own columns, timezone and
grouping policy in dkplus.

Unencrypted, non-partial Parquet positional delete files are applied while
streaming; their `pos` entries must be unique and ascending. In-range inlined
data must first be flushed to Parquet. Partitioned tables, column-mapped files,
encrypted files, partial files and Puffin deletion vectors fail explicitly;
they are never read as if they had no deletes.

## Local verification

```sh
cargo test --manifest-path ../../modules/dkplus/Cargo.toml repack
```

With an isolated, empty test Postgres database, all storage/catalog regressions
can run without live catalog access. This includes inline text timestamp casts,
typed windows, snapshot publication, advisory locks and file guards:

```sh
DKPLUS_TEST_DATABASE_URL='postgresql://postgres@localhost:5432/testdb' \
MAINTENANCE_TEST_DATABASE_URL='postgresql://postgres@localhost:5432/testdb' \
  cargo test --manifest-path ../../modules/dkplus/Cargo.toml \
    -- --include-ignored --test-threads=1
```

Tests use an in-memory object store with the same range-read and multipart
writer interfaces as Azure. They verify multi-pass merging, physical sort
order, boundary splitting, stable row identity, positional deletes, and actual
Parquet row-group sizes/statistics, including a node larger than the default
row-group limit. A live Functions/Azure/Postgres invocation is a separate
deployment integration check.
