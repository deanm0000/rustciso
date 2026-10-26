use crate::config::StorageAccount;
use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{Duration, SystemTime};

const API_VERSION: &str = "2021-08-06";
/// Azure Queue max visibility timeout (7 days).
pub const MAX_VISIBILITY: Duration = Duration::from_secs(7 * 24 * 3600);

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RetryState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

impl RetryState {
    pub fn fresh(self) -> Self {
        Self {
            retries: None,
            ..self
        }
    }

    fn next(self) -> Option<Self> {
        let retries = self.retries.unwrap_or(0);
        (retries < self.max_retries.unwrap_or(3)).then(|| Self {
            retries: Some(retries + 1),
            ..self
        })
    }
}

#[derive(Debug, Serialize)]
pub struct Job<T> {
    #[serde(flatten)]
    pub payload: T,
    #[serde(flatten)]
    pub retry: RetryState,
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for Job<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let retry = serde_json::from_value(value.clone()).map_err(D::Error::custom)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| D::Error::custom("queue job must be an object"))?;
        // The repack payload rejects unknown fields, so strip the envelope first.
        object.remove("retries");
        object.remove("max_retries");
        let payload = serde_json::from_value(value).map_err(D::Error::custom)?;
        Ok(Self { payload, retry })
    }
}

#[derive(Clone)]
pub struct QueueClient {
    http: reqwest::Client,
    account: StorageAccount,
    queue: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn retry_limits_and_defaults() {
        let mut retry = RetryState::default();
        for count in 1..=3 {
            retry = retry.next().unwrap();
            assert_eq!(retry.retries, Some(count));
        }
        assert_eq!(retry.next(), None);
        for limit in [0, 1, 5, u32::MAX] {
            let retry = RetryState {
                retries: Some(limit),
                max_retries: Some(limit),
            };
            assert_eq!(retry.next(), None);
            assert_eq!(
                retry.fresh(),
                RetryState {
                    retries: None,
                    max_retries: Some(limit)
                }
            );
        }
        assert_eq!(
            RetryState {
                retries: Some(5),
                max_retries: Some(2)
            }
            .next(),
            None
        );
    }

    #[test]
    fn envelope_preserves_strict_payload_validation() {
        #[derive(Debug, Deserialize, Serialize, PartialEq)]
        #[serde(deny_unknown_fields)]
        struct Payload {
            table: String,
        }
        let job: Job<Payload> = serde_json::from_value(json!({
            "table": "ciso.daprices", "retries": 2, "max_retries": 4
        }))
        .unwrap();
        assert_eq!(
            job.retry,
            RetryState {
                retries: Some(2),
                max_retries: Some(4)
            }
        );
        assert_eq!(
            serde_json::to_value(job).unwrap(),
            json!({
                "table": "ciso.daprices", "retries": 2, "max_retries": 4
            })
        );
        let job: Job<Payload> = serde_json::from_value(json!({"table": "x"})).unwrap();
        assert_eq!(job.retry, RetryState::default());
        assert_eq!(serde_json::to_value(job).unwrap(), json!({"table": "x"}));
        let job: Job<Payload> = serde_json::from_value(json!({
            "table": "x", "retries": null, "max_retries": null
        }))
        .unwrap();
        assert_eq!(job.retry, RetryState::default());
        for invalid in [
            json!({"table": "x", "retries": -1}),
            json!({"table": "x", "max_retries": "3"}),
            json!({"table": "x", "unexpected": true}),
        ] {
            assert!(serde_json::from_value::<Job<Payload>>(invalid).is_err());
        }
    }

    #[test]
    fn repack_retry_envelope_roundtrips() {
        let value = json!({
            "table": "ciso.daprices",
            "begin_date": "2023-07-09",
            "end_date": "2023-07-10",
            "unique": ["utcbegin", "node"],
            "files": ["daily.parquet"],
            "retries": 1,
            "max_retries": 5
        });
        let job: Job<crate::lake::repack::RepackPayload> = serde_json::from_value(value).unwrap();
        assert_eq!(job.retry.retries, Some(1));
        let crate::lake::repack::RepackPayload::Job(payload) = &job.payload else {
            panic!("expected a regular repack job");
        };
        assert_eq!(
            payload.begin.utc().to_rfc3339(),
            "2023-07-09T07:00:00+00:00"
        );
        assert_eq!(payload.end.utc().to_rfc3339(), "2023-07-10T07:00:00+00:00");
        assert_eq!(
            serde_json::to_value(job).unwrap(),
            json!({
                "table": "ciso.daprices",
                "begin": {"tz": "2023-07-09T07:00:00Z"},
                "end": {"tz": "2023-07-10T07:00:00Z"},
                "unique": ["utcbegin", "node"],
                "files": ["daily.parquet"],
                "retries": 1,
                "max_retries": 5
            })
        );
    }

    #[test]
    fn maintenance_plan_retry_envelope_roundtrips() {
        let value = json!({
            "table": "ciso.rtprices",
            "level": {
                "level": 1,
                "targetwindow_min": null,
                "targetwindow_max": null,
                "sourcewindow_min": null,
                "sourcewindow_max": null,
                "age_min": null,
                "age_max": null,
                "relage_min": null,
                "relage_max": null,
                "calendar_begin": "month"
            },
            "granularity": {"fixed": {"months": 0, "days": 0, "micros": 300000000}},
            "begin": {"tz": "2026-01-01T08:00:00Z"},
            "end": {"tz": "2026-02-01T08:00:00Z"},
            "files": ["daily.parquet"],
            "retries": 1,
            "max_retries": 4
        });
        let job: Job<crate::lake::repack::RepackPayload> =
            serde_json::from_value(value.clone()).unwrap();
        assert_eq!(job.retry.retries, Some(1));
        assert!(matches!(
            job.payload,
            crate::lake::repack::RepackPayload::Maintenance(_)
        ));
        assert_eq!(serde_json::to_value(job).unwrap(), value);
    }

    #[tokio::test]
    async fn retries_and_poison_use_correct_queue_payloads_and_surface_errors() {
        use axum::{
            Router,
            body::Bytes,
            http::{Method, StatusCode, Uri},
        };
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicU8, Ordering},
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(AtomicU8::new(0));
        let captured = requests.clone();
        let fail = failures.clone();
        let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
            let is_poison = uri.path().contains("-poison");
            let mode = fail.load(Ordering::SeqCst);
            let rejected = (mode == 1 && method == Method::POST)
                || (mode == 2 && method == Method::PUT)
                || (mode == 3 && method == Method::POST && is_poison);
            captured.lock().unwrap().push((method, uri, body));
            async move {
                if rejected {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::CREATED
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let queue = QueueClient::new(
            reqwest::Client::new(),
            StorageAccount {
                name: "test".into(),
                key: B64.encode("key"),
                queue_endpoint: endpoint,
            },
            "daprices",
        );
        let mut job = json!({"startdatetime": "20230709T07:00-0000", "get_prev_until": "20200101T08:00-0000"});
        let mut retry = RetryState::default();
        let mut log = Vec::new();
        for count in 1..=3 {
            queue
                .retry_or_poison(
                    &job,
                    retry,
                    Duration::from_secs(60),
                    "test failure",
                    &mut log,
                )
                .await
                .unwrap();
            let sent = requests.lock().unwrap();
            let (method, uri, body) = sent.last().unwrap();
            assert_eq!(method, Method::POST);
            assert_eq!(uri.path(), "/daprices/messages");
            assert!(uri.query().unwrap().contains("visibilitytimeout=60"));
            let body = std::str::from_utf8(body).unwrap();
            let encoded = body
                .strip_prefix("<QueueMessage><MessageText>")
                .unwrap()
                .strip_suffix("</MessageText></QueueMessage>")
                .unwrap();
            let decoded: serde_json::Value =
                serde_json::from_slice(&B64.decode(encoded).unwrap()).unwrap();
            job["retries"] = json!(count);
            assert_eq!(decoded, job);
            retry = serde_json::from_value(job.clone()).unwrap();
        }
        queue
            .retry_or_poison(
                &job,
                retry,
                Duration::from_secs(60),
                "test failure",
                &mut log,
            )
            .await
            .unwrap();
        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent.len(), 5);
            assert_eq!(sent[3].0, Method::PUT);
            assert_eq!(sent[3].1.path(), "/daprices-poison");
            assert_eq!(sent[4].1.path(), "/daprices-poison/messages");
            assert!(sent[4].1.query().unwrap().contains("visibilitytimeout=0"));
            let body = std::str::from_utf8(&sent[4].2).unwrap();
            let encoded = body
                .strip_prefix("<QueueMessage><MessageText>")
                .unwrap()
                .strip_suffix("</MessageText></QueueMessage>")
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&B64.decode(encoded).unwrap()).unwrap(),
                job
            );
        }
        assert!(log.last().unwrap().contains("daprices-poison"));
        let no_retry = json!({"table": "x", "max_retries": 0});
        queue
            .retry_or_poison(
                &no_retry,
                RetryState {
                    retries: None,
                    max_retries: Some(0),
                },
                Duration::ZERO,
                "no retries allowed",
                &mut log,
            )
            .await
            .unwrap();
        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent.len(), 7);
            assert_eq!(sent[5].0, Method::PUT);
            assert_eq!(sent[6].1.path(), "/daprices-poison/messages");
        }
        for mode in 1..=3 {
            failures.store(mode, Ordering::SeqCst);
            let retry = if mode == 1 {
                RetryState::default()
            } else {
                retry
            };
            assert!(
                queue
                    .retry_or_poison(&job, retry, Duration::ZERO, "failure", &mut log)
                    .await
                    .is_err()
            );
        }
        server.abort();
    }
}

impl QueueClient {
    pub fn new(http: reqwest::Client, account: StorageAccount, queue: &str) -> Self {
        Self {
            http,
            account,
            queue: queue.to_string(),
        }
    }

    pub async fn retry_or_poison(
        &self,
        job: &impl Serialize,
        retry: RetryState,
        visibility: Duration,
        reason: &str,
        log: &mut Vec<String>,
    ) -> Result<()> {
        if let Some(next) = retry.next() {
            let mut payload = serde_json::to_value(job)?;
            payload
                .as_object_mut()
                .context("queue job must be an object")?
                .insert("retries".into(), serde_json::json!(next.retries));
            self.send(&serde_json::to_string(&payload)?, visibility)
                .await?;
            log.push(format!(
                "requeued {} retry {}/{} visible in {}s: {reason}",
                self.queue,
                next.retries.unwrap_or(0),
                next.max_retries.unwrap_or(3),
                visibility.as_secs()
            ));
        } else {
            let poison = Self::new(
                self.http.clone(),
                self.account.clone(),
                &format!("{}-poison", self.queue),
            );
            let response = poison
                .request(
                    reqwest::Method::PUT,
                    &format!("/{}", poison.queue),
                    &[],
                    None,
                )
                .await?;
            if !response.status().is_success() {
                bail!(
                    "creating {} failed: HTTP {}: {}",
                    poison.queue,
                    response.status(),
                    response.text().await?
                );
            }
            poison
                .send(&serde_json::to_string(job)?, Duration::ZERO)
                .await?;
            log.push(format!(
                "moved to {} after {} retries (max_retries={}): {reason}",
                poison.queue,
                retry.retries.unwrap_or(0),
                retry.max_retries.unwrap_or(3)
            ));
        }
        Ok(())
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
        let query = [
            ("messagettl", "-1".to_string()),
            ("visibilitytimeout", vis.to_string()),
        ];
        let resp = self
            .request(reqwest::Method::POST, &path, &query, Some(body))
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            bail!(
                "queue put failed: HTTP {status}: {}",
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    pub async fn approximate_count(&self) -> Result<u64> {
        let path = format!("/{}", self.queue);
        let resp = self
            .request(
                reqwest::Method::GET,
                &path,
                &[("comp", "metadata".to_string())],
                None,
            )
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
        let content_type = if body.is_empty() {
            ""
        } else {
            "application/xml"
        };
        let auth = self.sign(
            method.as_str(),
            path,
            query,
            body.len(),
            content_type,
            &date,
        )?;

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
        let len = if content_len == 0 {
            String::new()
        } else {
            content_len.to_string()
        };
        let mut params: Vec<_> = query
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.as_str()))
            .collect();
        params.sort();
        let mut resource = format!("/{}{path}", self.account.name);
        for (k, v) in params {
            resource.push_str(&format!("\n{k}:{v}"));
        }
        let string_to_sign = format!(
            "{verb}\n\n\n{len}\n\n{content_type}\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:{API_VERSION}\n{resource}"
        );
        let key = B64
            .decode(&self.account.key)
            .context("AccountKey is not base64")?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(string_to_sign.as_bytes());
        Ok(format!(
            "SharedKey {}:{}",
            self.account.name,
            B64.encode(mac.finalize().into_bytes())
        ))
    }
}
