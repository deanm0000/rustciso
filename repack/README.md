# Parquet repack queue trigger

Enqueue a base64-encoded JSON message on the `repack` Azure Storage queue
(`AzureWebJobsStorage`), just like the existing `rtprices` queue:

```json
{
  "table": "rtprices",
  "begin_date": "2026-01-01",
  "end_date": "2026-10-01",
  "unique": ["utcbegin", "node"]
}
```

`table` defaults to the `ciso` schema; `ciso.rtprices` is also accepted. Dates
define the half-open interval `[begin_date, end_date)` at midnight in
`America/Los_Angeles`, with daylight-saving rules applied independently to each
boundary. Stored `utcbegin` timestamps remain UTC. For example, the range above
is `[2026-01-01T08:00:00Z, 2026-10-01T07:00:00Z)`. A local day spanning a
daylight-saving transition can contain 23 or 25 hours.

`unique` is an optional list of column names. When supplied, only rows within
the requested interval are deduplicated by that combination of columns, keeping
the entire row with the latest `downloaded_at`. Equal timestamps are broken by
the highest DuckLake row ID; null timestamps sort behind non-null timestamps.
Null key values compare equal. The list must be nonempty, contain distinct
existing columns, and the table must have microsecond `timestamptz downloaded_at`.
Omit `unique` or set it to `null` to preserve all rows without deduplication.

## Output layout

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

## Azure runtime and failure behavior

Sorting uses approximately 64 MiB input runs and eight-way streaming merge
passes. Intermediate Parquet runs are stored beneath the table's
`_tmp/repack-<invocation-id>/` blob prefix, not on local Functions temp disk.
These are sort-buffer targets, not a total process memory cap: decoding,
sorting, merge readers, Parquet metadata and an open row group also use memory.
Short-range groups must retain a whole node regardless of size.
Deduplication adds a streaming pass sorted by the unique columns and descending
download timestamp, followed by the normal `(node, utcbegin)` output sort.

The app's `functionTimeout` is `-1`, which **requires an Azure Functions hosting
plan supporting unbounded execution**. Azure platform restarts and scale-in can
still interrupt an invocation. This setting applies to the whole Function App.
No durable continuation/checkpoint is implemented; a retry starts sorting again.

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
explicit failure and host retry, rather than overwriting concurrent writes.
Snapshot allocation conflicts are retried without re-sorting. Candidate final
files are retained on commit errors, since a lost COMMIT response might mean
they were published. Only remove such files after confirming they are
unreferenced by **every retained snapshot**. The host's existing five-attempt
policy eventually sends unsuccessful jobs to `repack-poison`.

## Current catalog support

The trigger uses this app's existing Postgres DuckLake catalog and `Synblob`
storage configuration. Supported tables are unpartitioned, Azure-backed tables
with `varchar node`, microsecond `timestamptz utcbegin`, and columns of types
`varchar`, `float64`, or microsecond `timestamptz`. File columns must carry
DuckLake Parquet field IDs. Null sort keys are rejected.

Unencrypted, non-partial Parquet positional delete files are applied while
streaming; their `pos` entries must be unique and ascending. In-range inlined
data must first be flushed to Parquet. Partitioned tables, column-mapped files,
encrypted files, partial files and Puffin deletion vectors fail explicitly;
they are never read as if they had no deletes.

## Local verification

```sh
cargo test lake::repack
```

With the app's environment variables set, the read-only catalog regression test
also verifies timestamp parameters against real inlined tables:

```sh
cargo test inlined_text_timestamps_accept_datetime_parameters -- --ignored
```

Tests use an in-memory object store with the same range-read and multipart
writer interfaces as Azure. They verify multi-pass merging, physical sort
order, boundary splitting, stable row identity, positional deletes, and actual
Parquet row-group sizes/statistics, including a node larger than the default
row-group limit. A live Functions/Azure/Postgres invocation is a separate
deployment integration check.
