use crate::config::StorageAccount;
use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime};

const API_VERSION: &str = "2021-08-06";
/// Azure Queue max visibility timeout (7 days).
pub const MAX_VISIBILITY: Duration = Duration::from_secs(7 * 24 * 3600);

#[derive(Clone)]
pub struct QueueClient {
    http: reqwest::Client,
    account: StorageAccount,
    queue: String,
}

impl QueueClient {
    pub fn new(http: reqwest::Client, account: StorageAccount, queue: &str) -> Self {
        Self {
            http,
            account,
            queue: queue.to_string(),
        }
    }

    /// Enqueue `message` (base64-encoded, as the Functions queue trigger expects) that
    /// becomes visible after `visibility`. Messages never expire so long delays are safe.
    pub async fn send(&self, message: &str, visibility: Duration) -> Result<()> {
        let vis = visibility.min(MAX_VISIBILITY).as_secs();
        let body = format!(
            "<QueueMessage><MessageText>{}</MessageText></QueueMessage>",
            B64.encode(message)
        );
        let path = format!("/{}/messages", self.queue);
        let query = [("messagettl", "-1".to_string()), ("visibilitytimeout", vis.to_string())];
        let resp = self
            .request(reqwest::Method::POST, &path, &query, Some(body))
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            bail!("queue put failed: HTTP {status}: {}", resp.text().await.unwrap_or_default());
        }
        Ok(())
    }

    pub async fn approximate_count(&self) -> Result<u64> {
        let path = format!("/{}", self.queue);
        let resp = self
            .request(reqwest::Method::GET, &path, &[("comp", "metadata".to_string())], None)
            .await?;
        if !resp.status().is_success() {
            bail!("queue metadata failed: HTTP {}", resp.status());
        }
        resp.headers()
            .get("x-ms-approximate-messages-count")
            .context("missing x-ms-approximate-messages-count")?
            .to_str()?
            .parse()
            .context("bad message count")
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<String>,
    ) -> Result<reqwest::Response> {
        let date = httpdate::fmt_http_date(SystemTime::now());
        let body = body.unwrap_or_default();
        let content_type = if body.is_empty() { "" } else { "application/xml" };
        let auth = self.sign(method.as_str(), path, query, body.len(), content_type, &date)?;

        let qs = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let url = format!("{}{path}?{qs}", self.account.queue_endpoint);
        let mut req = self
            .http
            .request(method, url)
            .header("x-ms-date", &date)
            .header("x-ms-version", API_VERSION)
            .header("Authorization", auth);
        if !body.is_empty() {
            req = req.header("Content-Type", content_type).body(body);
        }
        Ok(req.send().await?)
    }

    fn sign(
        &self,
        verb: &str,
        path: &str,
        query: &[(&str, String)],
        content_len: usize,
        content_type: &str,
        date: &str,
    ) -> Result<String> {
        let len = if content_len == 0 { String::new() } else { content_len.to_string() };
        let mut params: Vec<_> = query.iter().map(|(k, v)| (k.to_ascii_lowercase(), v.as_str())).collect();
        params.sort();
        let mut resource = format!("/{}{path}", self.account.name);
        for (k, v) in params {
            resource.push_str(&format!("\n{k}:{v}"));
        }
        let string_to_sign = format!(
            "{verb}\n\n\n{len}\n\n{content_type}\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:{API_VERSION}\n{resource}"
        );
        let key = B64.decode(&self.account.key).context("AccountKey is not base64")?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(string_to_sign.as_bytes());
        Ok(format!(
            "SharedKey {}:{}",
            self.account.name,
            B64.encode(mac.finalize().into_bytes())
        ))
    }
}
