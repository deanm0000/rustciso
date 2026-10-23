# Table maintenance timer

Candidate planning and rule evaluation are provided by
[`dkplus::maintenance`](../../../modules/dkplus/src/maintenance.rs). The
[application module](../src/lake/maintenance.rs) selects the CISO price tables,
opens a planning transaction for each table, and sends discovered plans to the
existing [`repack` queue](../repack/README.md). See the
[shared-library build requirements](../repack/README.md#shared-storage-library)
for the sibling Cargo path dependency.

Runs at minutes `00` and `30` of every hour (UTC by default) while backfilling.
The timer is monitored by Azure Functions using `AzureWebJobsStorage` and does
not run just because the app starts. To switch to the start of each month later,
change `schedule` in [function.json](./function.json) to `0 0 0 1 * *`.

For each of `ciso.daprices`, `ciso.rtprices`, and `ciso.fmprices`, dkplus reads
active file statistics and plans non-overlapping repacks from the table's
`dkplus.granularity` and `dkplus.repack_level<N>_*` tags. It does not read
Parquet files while planning. Levels, interval rules, calendar boundaries, and
the required granularity tag are documented in the
[dkplus maintenance guide](../../../modules/dkplus/README.md#maintenance-repack-levels).
The timer sends one serialized `PotentialRepack` plan per eligible group. The
plan includes its bounds, applicable rules, granularity, and source catalog
paths.

On the repack queue, the worker resolves the table policy and calls
`dkplus::maintenance::do_repack` with `Strictness::Raise`. The library rechecks
that the source files are still current and the rules still produce the
original window. A stale plan is logged and skipped. For a valid plan, the
source paths are used as a freshness guard and the data is compacted only when
the selected sources can produce exactly one in-window output file.

Database or queue failures fail the timer invocation and are included in its
logs. Discovery finishes for all three tables before enqueueing starts, though
a partial enqueue failure can leave some plans queued. A later timer invocation
may discover a plan again while an earlier copy is pending; execution-time
revalidation makes redundant plans safe.

## Verification

```sh
cargo test lake::maintenance
```

Live Azure timer delivery, Postgres planning, and queue processing require
deployment integration verification. Unit tests do not enqueue jobs in Azure or
modify the live catalog.
