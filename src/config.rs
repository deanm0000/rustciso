use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::env;

/// Postgres host/db that holds the DuckLake catalog (tables live in schema `ducklake`).
const PG_HOST: &str = "projectsdb.usspapps.com";
const PG_PORT: u16 = 55669;
const PG_DB: &str = "everything";
pub(crate) const DEFAULT_REPACK_RUN_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const DEFAULT_REPACK_NODE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct StorageAccount {
    pub name: String,
    pub key: String,
    pub queue_endpoint: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub postgres: tokio_postgres::Config,
    /// Account holding the DuckLake parquet files (`Synblob`).
    pub lake_account: StorageAccount,
    /// Account holding the price (`rtprices`, `fmprices`, `daprices`) and `repack` queues (`AzureWebJobsStorage`).
    pub queue_account: StorageAccount,
    pub repack_run_bytes: usize,
    pub repack_node_bytes: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let user = env::var("POSTGRES_USER").context("missing POSTGRES_USER")?;
        let pw = env::var("POSTGRES_PW").context("missing POSTGRES_PW")?;
        let mut postgres = tokio_postgres::Config::new();
        postgres
            .host(PG_HOST)
            .port(PG_PORT)
            .dbname(PG_DB)
            .user(&user)
            .password(&pw)
            .ssl_mode(tokio_postgres::config::SslMode::Require)
            .application_name("rustciso");
        let lake_account = parse_conn_str(&env::var("Synblob").context("missing Synblob")?)?;
        let queue_account = parse_conn_str(
            &env::var("AzureWebJobsStorage").context("missing AzureWebJobsStorage")?,
        )?;
        let repack_run_bytes = env_mb("REPACK_RUN_MB", DEFAULT_REPACK_RUN_BYTES);
        let repack_node_bytes = env_mb("REPACK_NODE_MB", DEFAULT_REPACK_NODE_BYTES);
        Ok(Self {
            postgres,
            lake_account,
            queue_account,
            repack_run_bytes,
            repack_node_bytes,
        })
    }
}

fn env_mb(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(value) => match parse_mb(&value) {
            Some(bytes) => bytes,
            None => {
                eprintln!(
                    "{name} must be a positive whole MiB count that fits in bytes; using default {} MiB",
                    default / (1024 * 1024)
                );
                default
            }
        },
        Err(env::VarError::NotPresent) => default,
        Err(error) => {
            eprintln!(
                "could not read {name} ({error}); using default {} MiB",
                default / (1024 * 1024)
            );
            default
        }
    }
}

fn parse_mb(value: &str) -> Option<usize> {
    value
        .parse::<usize>()
        .ok()
        .filter(|mb| *mb > 0)?
        .checked_mul(1024 * 1024)
}

pub fn parse_conn_str(conn: &str) -> Result<StorageAccount> {
    let parts: HashMap<String, String> = conn
        .trim()
        .trim_matches('"')
        .split(';')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let name = parts
        .get("accountname")
        .ok_or_else(|| anyhow!("connection string missing AccountName"))?
        .clone();
    let key = parts
        .get("accountkey")
        .ok_or_else(|| anyhow!("connection string missing AccountKey"))?
        .clone();
    let queue_endpoint = match parts.get("queueendpoint") {
        Some(ep) => ep.trim_end_matches('/').to_string(),
        None => {
            let proto = parts
                .get("defaultendpointsprotocol")
                .map(String::as_str)
                .unwrap_or("https");
            let suffix = parts
                .get("endpointsuffix")
                .map(String::as_str)
                .unwrap_or("core.windows.net");
            format!("{proto}://{name}.queue.{suffix}")
        }
    };
    Ok(StorageAccount {
        name,
        key,
        queue_endpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connection_string() {
        let acct = parse_conn_str(
            "DefaultEndpointsProtocol=https;AccountName=foo;AccountKey=a2V5==;EndpointSuffix=core.windows.net",
        )
        .unwrap();
        assert_eq!(acct.name, "foo");
        assert_eq!(acct.key, "a2V5==");
        assert_eq!(acct.queue_endpoint, "https://foo.queue.core.windows.net");
    }

    #[test]
    fn parses_positive_mb_budgets_and_uses_defaults_for_invalid_values() {
        assert_eq!(parse_mb("1"), Some(1024 * 1024));
        assert_eq!(parse_mb("128"), Some(DEFAULT_REPACK_RUN_BYTES));
        assert_eq!(parse_mb("256"), Some(DEFAULT_REPACK_NODE_BYTES));
        assert_eq!(parse_mb("0"), None);
        assert_eq!(parse_mb("-1"), None);
        assert_eq!(parse_mb("1.5"), None);
        assert_eq!(parse_mb("128MiB"), None);
        assert_eq!(parse_mb("18446744073709551616"), None);
        assert_eq!(parse_mb(&usize::MAX.to_string()), None);
        assert_eq!(env_mb_from(Some("1024"), 9), 1024 * 1024 * 1024);
        assert_eq!(env_mb_from(Some("not-mb"), 9), 9);
        assert_eq!(env_mb_from(Some("0"), 9), 9);
        assert_eq!(env_mb_from(None, 9), 9);
    }

    fn env_mb_from(value: Option<&str>, default: usize) -> usize {
        value.and_then(parse_mb).unwrap_or(default)
    }
}
