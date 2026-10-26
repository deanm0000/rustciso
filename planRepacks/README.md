# Catalog repack planner

Runs every hour at minute 10 (UTC). The timer is monitored and does not run on
startup. It replaces the old `maintenance` timer.

The [application adapter](../src/lake/maintenance.rs) lists every active table in
every active DuckLake catalog schema, resolves each table from its `dkplus.*`
tags, and calls `Table::find_potential_repacks()`. No CISO table list or
application-specific maintenance policy is applied. Tables without repack levels
produce no plans; invalid table policy or missing required statistics fails the
invocation with an explicit error.

Each discovered `PotentialRepack` is serialized and sent immediately to the
`maintenance-repack` queue for the
[maintenance worker](../maintenanceRepack/README.md). Discovery and enqueueing
errors fail the timer invocation. Earlier tables or plans may already have been
queued when a failure occurs. A later invocation can queue the same plan again;
dkplus revalidates plans at execution time and skips stale plans.

The existing [repack queue](../repack/README.md) remains available for ingestion
and manual jobs. Source builds require the sibling dkplus crate described there.

## Verification

`cargo test lake::maintenance` checks schedules, cleanup policy, and delivery to a
local queue mock. Live catalog discovery and Azure timer/queue delivery require
deployment integration verification.
