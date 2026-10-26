# Expired-file cleanup timer

Runs daily at **08:30 UTC** (`0 30 8 * * *`). The timer is monitored and does not
run on startup.

The [application adapter](../src/lake/maintenance.rs) calls
`dkplus::maintenance::cleanup_expired_files` with catalog-wide scope,
`dry_run: false`, and a strict cutoff of **12 hours before invocation**.
Only files scheduled for deletion before that cutoff are eligible.
This is the scheduling age, not the file's modification time.

Cleanup physically deletes files already queued by DuckLake snapshot expiry,
then acknowledges their deletion entries. It does not expire snapshots or scan
for orphaned files. dkplus rejects files still referenced by catalog metadata.
The 12-hour delay must exceed the longest active reader, and this job must not
overlap other direct object-store cleanup tools.

Deletion errors fail the invocation and leave queue entries retryable. A failed
invocation may already have deleted some objects; retrying safely acknowledges
missing objects. Logs include candidate, deleted, and already-missing counts.

`cargo test lake::maintenance` verifies the daily schedule and exact cutoff
without modifying the live catalog or deleting Azure blobs.
