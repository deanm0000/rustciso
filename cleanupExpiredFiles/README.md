# Expired-file cleanup timer

Runs daily at **08:30 UTC** (`0 30 8 * * *`). The timer is monitored and does not
run on startup.

The [application adapter](../src/lake/maintenance.rs) calls
`dkplus::maintenance::schedule_unreferenced_table_files` for `ciso.rtprices`.
It removes retired file metadata only when no retained snapshot references it,
and queues the files for deletion without expiring snapshots. The same timer calls
`dkplus::maintenance::cleanup_expired_files` with catalog-wide scope,
`dry_run: false`, and a strict cutoff of **12 hours before invocation**.
Only files scheduled for deletion before that cutoff are eligible.
This is the scheduling age, not the file's modification time.

Cleanup physically deletes queued files, then acknowledges their deletion entries.
It does not expire snapshots or scan storage for orphaned files. dkplus rejects
files still referenced by catalog metadata. Readers are not tracked: a reader
that holds a stale file reference past the 12-hour delay may fail and must refresh.
This job must not overlap other direct object-store cleanup tools.

The timer also scans `_tmp/` under each catalog table directory in the configured
Azure storage account and deletes temporary files last modified more than 24 hours
ago. The cutoff is checked again immediately before deletion. Active repacks are
not tracked; a job that uses a temporary file for more than 24 hours can fail.
Directories outside the configured Azure storage are logged and skipped.
The timer also removes Azure `_tmp` directory markers last modified more than
24 hours ago, processing children first. Deletion is nonrecursive and conditional
on the directory ETag: nonempty or changed directories are retained. The cutoff
does not track active jobs, and a job using an old empty directory may need to
recreate it. Directory candidate, deleted, nonempty, changed, and missing counts
are logged separately.

Catalog-wide deletion commits in batches of 100. Deletion errors fail the
invocation; completed batches remain acknowledged, while the failing batch is
retryable. A failed batch may already have deleted some objects; retrying safely
acknowledges missing objects. Logs include scheduled, candidate, deleted, and
already-missing counts.

`cargo test lake::maintenance` verifies the daily schedule and exact cutoff
without modifying the live catalog or deleting Azure blobs.
