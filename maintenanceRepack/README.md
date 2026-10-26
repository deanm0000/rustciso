# Maintenance repack worker

Consumes serialized `dkplus::maintenance::PotentialRepack` plans from the
`maintenance-repack` Azure Storage queue (`AzureWebJobsStorage`).
The [hourly planner](../planRepacks/README.md) produces these messages.

The worker resolves the plan's table directly from catalog tags and calls
`dkplus::maintenance::do_repack` with `Strictness::Raise`. dkplus rechecks source
files and rules; stale plans are logged and skipped. Successful maintenance
repacks expire snapshots referencing the replaced files, removing time travel
to those versions across the catalog. Physical deletion is deferred to the
[daily cleanup timer](../cleanupExpiredFiles/README.md).

Retries follow the shared [queue policy](../rtprices/README.md#retry-policy-all-queue-triggers):
60-second delay, three retries by default, then `maintenance-repack-poison`.
Optional `retries` and `max_retries` fields remain in the message across retries.
Malformed requests fail with a structured Azure response.

The separate [repack queue](../repack/README.md) retains its existing ingestion,
manual-job, and legacy maintenance-plan support for rollout compatibility.
