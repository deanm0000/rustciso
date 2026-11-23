# rustciso

## Project overview

This repository is a Rust Azure Functions custom handler for CISO market-data
ingestion and DuckLake maintenance. The handler is in `src/main.rs`; report
parsing, loading, and storage logic is split across the other modules in `src/`.
Each trigger has a folder-level `function.json`, and queue payloads and
trigger-specific behavior are documented in that folder's `README.md`.

The Cargo manifest depends on the sibling crate `../../modules/dkplus`.
That checkout must be present for builds, and changes to it can affect this
app's release binary. Check both worktrees when changing shared-library
behavior.

## Development and validation

- Format Rust changes with `cargo fmt --all`; verify with
  `cargo fmt --all -- --check`.
- Run focused tests for the changed module. The repository's common app test
  command is `cargo test --bin rustciso -- --skip lake::`; lake/catalog tests
  may require isolated test resources and must not be pointed at production.
- Build the Azure Linux binary with
  `cargo build --release --target x86_64-unknown-linux-musl`.
- For local Functions integration, use `func start` with local settings and
  non-production storage/database resources. The binary path is configured in
  `host.json`; custom-handler routes must match the function folder names.
- Consult the affected trigger's README before constructing or replaying a
  queue payload. Queue triggers can start backfills, retries, or data
  replacement, so do not send messages to shared or production queues as a
  routine test.

## Configuration and data safety

`src/config.rs` reads PostgreSQL credentials from `POSTGRES_USER` and
`POSTGRES_PW`, and Azure storage connection strings from `Synblob` and
`AzureWebJobsStorage`. Keep credentials and connection strings in approved
secret storage or ignored local settings. Never commit them, print them, or
include them in logs, test fixtures, or this file.

For production database diagnostics, use only an approved credential source
and a least-privilege account. Prefer read-only queries and an explicit
read-only transaction. Do not modify production data, grants, or database
sessions without explicit authorization. Never assume passwordless production
database access.

To troubleshoot and/or verify data ingestion, check the prod db with `psql -h projectsdb.usspapps.com -p 55669 -U cisoscraper -d everything`. 

The Azure CLI executable is available in WSL at:

```text
/mnt/c/Program Files/Microsoft SDKs/Azure/CLI2/wbin/az
```

Before any Azure operation, confirm the active account and subscription with
`az account show | jq -r '.name'` which should show 'pd-segment-projects-amer'. Do not expose subscription credentials or application settings.

This azure function app is 'app-ciso', and its storage queues are in the storage account 'stciso'. The application insights resource is also named 'app-ciso'. All 3 are in the resource group rg-ciso.

## Production release and verification

When implementing fixes and new features, first we should test locally. After local tests are successful, unless instructed otherwise, run the compile_deploy.sh script to push the new version to prod immediately and create a queue message (or other trigger) for the new function or fix to trigger its execution in azure. After that, sleep for 5 minutes, and then check the database and/or az logs to see if it worked in the cloud. If it did, then you're done. If it didn't, then fix whatever new issues arose, and loop back to deploying the fix. 