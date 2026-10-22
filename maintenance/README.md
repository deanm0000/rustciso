# Table maintenance timer

Catalog candidate discovery is implemented by
[`dkplus::maintenance`](../../../modules/dkplus/src/maintenance.rs).
The [application module](../src/lake/maintenance.rs) keeps CISO report selection,
job construction, timer behavior, and Azure queue delivery. See the
[shared-library build requirements](../repack/README.md#shared-storage-library)
for the sibling Cargo path dependency.

Runs at minutes `00` and `30` of every hour (UTC by default) while backfilling.
The timer is monitored by Azure Functions using `AzureWebJobsStorage` and does
not run just because the app starts. To switch to the start of each month later,
change `schedule` in [function.json](./function.json) to `0 0 0 1 * *`.

For each of `ciso.daprices`, `ciso.rtprices`, and `ciso.fmprices`, maintenance
materializes active `utcbegin` file statistics in a **Postgres temporary table**.
The timezone casts remain in a separate statement, not an inlined CTE.
Each table has its own transaction; the temporary table is dropped on commit
or rolled back on failure.

The `hours` statistic includes the final data interval, using each
[`Report`](../src/report.rs)'s `granularity`: 5 minutes for `rtprices`,
15 minutes for `fmprices`, and 60 minutes for `daprices`. This is independent
of the hour/day download window. For example, a normal daily file covers
24 hours and a spring DST-transition day covers 23 hours for all three reports.
The duration is passed as a SQL parameter; the temp-table materialization
and Los Angeles timezone conversions are unchanged.

The supplied candidate query is preserved: select files whose Los Angeles
minimum hour is `0` and maximum hour is `23`, group by the minimum's calendar
month, and require the earliest minimum to equal the first of that month.
The number of files must equal the number of days in the month, including
February in leap years. This is still a file-statistics heuristic, not a check
for exactly one file per distinct day or contiguous data.

Each result produces one job on the existing
[`repack` queue](../repack/README.md), with `"unique": ["utcbegin", "node"]`
and `"files"` containing the catalog paths from `array_agg(path)`.
The minimum's local date becomes `begin_date`; the day **after** the maximum's
local date becomes the exclusive `end_date`, including the last day's rows.
Los Angeles daylight-saving boundaries are handled by the repack worker.
Discovery finishes for all three tables before enqueueing starts.

Database and queue failures fail the invocation and are included in its logs.
A later timer invocation can still enqueue the same range while a previous job
is pending, and a partial enqueue failure can leave some jobs already queued.
The repack worker acquires per-table, per-local-date Postgres advisory locks
for the requested range, then checks whether any supplied path is still active.
Overlapping ranges wait; disjoint ranges on the same table may run concurrently.
If none of the supplied paths remain active, it
logs a successful skip without sorting or rewriting the data. Source rechecks
at commit also protect against concurrent catalog changes from other writers.

## Verification

```sh
cargo test lake::maintenance
```

The SQL regression test needs an isolated Postgres database without an existing
`ducklake` schema. It creates fixture tables and rolls them back:

```sh
MAINTENANCE_TEST_DATABASE_URL='postgresql://postgres@localhost:5432/testdb' \
  cargo test lake::maintenance -- --include-ignored
```

Live Azure timer delivery, Postgres discovery, and queue processing require
deployment integration verification. Running unit tests does not enqueue
jobs in Azure or modify the live catalog.
