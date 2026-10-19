# Parquet repack queue trigger

Enqueue a base64-encoded JSON message on the `repack` Azure Storage queue
(`AzureWebJobsStorage`), just like the existing `rtprices` queue:

```json
{
  "table": "rtprices",
  "begin_date": "2026-01-01",
  "end_date": "2026-10-01"
}
```

`table` defaults to the `ciso` schema; `ciso.rtprices` is also accepted. Dates
define the half-open interval `[begin_date, end_date)` at midnight in
`America/Los_Angeles`, with daylight-saving rules applied independently to each
boundary. Stored `utcbegin` timestamps remain UTC. For example, the range above
is `[2026-01-01T08:00:00Z, 2026-10-01T07:00:00Z)`. A local day spanning a
daylight-saving transition can contain 23 or 25 hours. No rows are deduplicated
or aggregated.

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
IDs are preserved. Logical record counts, `next_row_id` and table-wide value
statistics do not change.

## Azure runtime and failure behavior

Sorting uses approximately 64 MiB input runs and eight-way streaming merge
passes. Intermediate Parquet runs are stored beneath the table's
`_tmp/repack-<invocation-id>/` blob prefix, not on local Functions temp disk.
These are sort-buffer targets, not a total process memory cap: decoding,
sorting, merge readers, Parquet metadata and an open row group also use memory.
Short-range groups must retain a whole node regardless of size.

The app's `functionTimeout` is `-1`, which **requires an Azure Functions hosting
plan supporting unbounded execution**. Azure platform restarts and scale-in can
still interrupt an invocation. This setting applies to the whole Function App.
No durable continuation/checkpoint is implemented; a retry starts sorting again.

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

Tests use an in-memory object store with the same range-read and multipart
writer interfaces as Azure. They verify multi-pass merging, physical sort
order, boundary splitting, stable row identity, positional deletes, and actual
Parquet row-group sizes/statistics, including a node larger than the default
row-group limit. A live Functions/Azure/Postgres invocation is a separate
deployment integration check.
