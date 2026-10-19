use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::env;

/// Postgres host/db that holds the DuckLake catalog (tables live in schema `ducklake`).
const PG_HOST: &str = "projectsdb.usspapps.com";
const PG_PORT: u16 = 55669;
const PG_DB: &str = "everything";

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
    /// Account holding the `rtprices` and `repack` queues (`AzureWebJobsStorage`).
    pub queue_account: StorageAccount,
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
        Ok(Self {
            postgres,
            lake_account,
            queue_account,
        })
    }
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
}
