use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use std::io::{Cursor, Read};

const OASIS_URL: &str = "https://oasis.caiso.com/oasisapi/SingleZip";
const OASIS_FMT: &str = "%Y%m%dT%H:%M%z";
const RATE_LIMIT_WAIT: std::time::Duration = std::time::Duration::from_millis(5100);
const RATE_LIMIT_RETRIES: usize = 2;
/// OASIS error code for "No data returned for the specified selection".
const NO_DATA_ERR_CODE: &str = "1000";
const PROCESSING_ERR_CODE: &str = "1015";
const PROCESSING_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
const PROCESSING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug)]
struct ProcessingError(String);

impl std::fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OASIS error {PROCESSING_ERR_CODE}: {}", self.0)
    }
}

impl std::error::Error for ProcessingError {}

pub fn parse_oasis_time(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_str(s.trim(), OASIS_FMT)
        .with_context(|| format!("invalid OASIS datetime '{s}', expected e.g. 20261016T07:00-0000"))?
        .with_timezone(&Utc))
}

pub fn format_oasis_time(t: DateTime<Utc>) -> String {
    t.format("%Y%m%dT%H:%M-0000").to_string()
}

/// The OASIS SingleZip parameters that vary between price reports.
#[derive(Debug, Clone, Copy)]
pub struct OasisQuery {
    pub queryname: &'static str,
    pub version: &'static str,
    pub market_run_id: &'static str,
    pub grp_type: &'static str,
}

pub fn build_url(query: &OasisQuery, start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    format!(
        "{OASIS_URL}?resultformat=6&queryname={}&version={}&startdatetime={}&enddatetime={}&market_run_id={}&grp_type={}",
        query.queryname,
        query.version,
        format_oasis_time(start),
        format_oasis_time(end),
        query.market_run_id,
        query.grp_type,
    )
}

#[derive(Debug)]
pub enum OasisResult {
    /// Raw CSV file contents (one per LMP type for some reports, one combined file for others).
    Csvs(Vec<(String, Bytes)>),
    /// OASIS has no data for the window yet.
    NotReady(String),
}

pub async fn fetch(
    client: &reqwest::Client,
    query: &OasisQuery,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    log: &mut Vec<String>,
) -> Result<OasisResult> {
    let url = build_url(query, start, end);
    fetch_url(client, &url, log).await
}

async fn fetch_url(client: &reqwest::Client, url: &str, log: &mut Vec<String>) -> Result<OasisResult> {
    let mut attempt = 0;
    let mut body = fetch_body(client, url, log, &mut attempt).await?;
    let mut deadline = None;
    loop {
        let processing = match parse_zip(&body) {
            Err(err) if err.is::<ProcessingError>() => err,
            result => return result,
        };
        let deadline = *deadline.get_or_insert_with(|| tokio::time::Instant::now() + PROCESSING_TIMEOUT);
        log.push(format!("{processing}; waiting {PROCESSING_WAIT:?} before retry (60s limit)"));
        body = tokio::time::timeout_at(deadline, async {
            tokio::time::sleep(PROCESSING_WAIT).await;
            fetch_body(client, url, log, &mut attempt).await
        })
        .await
        .with_context(|| format!("OASIS still processing after 60s: {processing}"))??;
    }
}

async fn fetch_body(
    client: &reqwest::Client,
    url: &str,
    log: &mut Vec<String>,
    attempt: &mut usize,
) -> Result<Bytes> {
    let body = loop {
        log.push(format!("GET {url}"));
        let resp = client.get(url).send().await.context("OASIS request failed")?;
        let status = resp.status();
        if status == StatusCode::TOO_MANY_REQUESTS && *attempt < RATE_LIMIT_RETRIES {
            *attempt += 1;
            log.push(format!("OASIS 429, waiting {RATE_LIMIT_WAIT:?} (retry {attempt}/{RATE_LIMIT_RETRIES})"));
            tokio::time::sleep(RATE_LIMIT_WAIT).await;
            continue;
        }
        if !status.is_success() {
            bail!("OASIS returned HTTP {status}");
        }
        break resp.bytes().await.context("reading OASIS body")?;
    };
    Ok(body)
}

pub fn parse_zip(body: &[u8]) -> Result<OasisResult> {
    let mut archive = zip::ZipArchive::new(Cursor::new(body)).context("OASIS response is not a zip")?;
    let mut csvs = Vec::new();
    let mut xml_errors = Vec::new();
    for i in 0..archive.len() {
        let mut f = archive.by_index(i)?;
        let name = f.name()?.to_string();
        let mut buf = Vec::with_capacity(f.size() as usize);
        f.read_to_end(&mut buf)?;
        let lower = name.to_ascii_lowercase();
        if lower.ends_with(".csv") {
            csvs.push((name, Bytes::from(buf)));
        } else if lower.ends_with(".xml") {
            xml_errors.push(parse_xml_error(&String::from_utf8_lossy(&buf)));
        }
    }
    if !csvs.is_empty() {
        return Ok(OasisResult::Csvs(csvs));
    }
    match xml_errors.into_iter().next() {
        Some((Some(code), desc)) if code == NO_DATA_ERR_CODE => Ok(OasisResult::NotReady(desc)),
        Some((Some(code), desc)) if code == PROCESSING_ERR_CODE => Err(ProcessingError(desc).into()),
        Some((code, desc)) => Err(anyhow!("OASIS error {}: {desc}", code.unwrap_or_default())),
        None => Err(anyhow!("OASIS zip contained no csv or xml files")),
    }
}

fn tag_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<m:{tag}>");
    let close = format!("</m:{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim())
}

fn parse_xml_error(xml: &str) -> (Option<String>, String) {
    (
        tag_text(xml, "ERR_CODE").map(str::to_string),
        tag_text(xml, "ERR_DESC").unwrap_or("unknown OASIS error").to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    const NO_DATA_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<m:OASISReport xmlns:m="http://www.caiso.com/soa/OASISReport_v1.xsd">
<m:MessagePayload><m:RTO><m:name>CAISO</m:name>
<m:ERROR>
<m:ERR_CODE>1000</m:ERR_CODE>
<m:ERR_DESC>No data returned for the specified selection</m:ERR_DESC>
</m:ERROR></m:RTO></m:MessagePayload></m:OASISReport>"#;

    fn processing_zip() -> Vec<u8> {
        zip_of(&[("error.xml", &NO_DATA_XML
            .replace(">1000<", ">1015<")
            .replace(
                "No data returned for the specified selection",
                "GroupZip DownLoad is in Processing, Please Submit request after Sometime",
            ))])
    }

    struct TestServer {
        url: String,
        requests: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn serve(responses: Vec<Vec<u8>>) -> TestServer {
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let app = axum::Router::new().route("/", axum::routing::get(move || {
            let index = count.fetch_add(1, Ordering::SeqCst).min(responses.len() - 1);
            let body = responses[index].clone();
            async move { body }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        TestServer { url, requests, task }
    }

    fn zip_of(files: &[(&str, &str)]) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, body) in files {
            w.start_file(*name, zip::write::SimpleFileOptions::default()).unwrap();
            w.write_all(body.as_bytes()).unwrap();
        }
        w.finish().unwrap().into_inner()
    }

    #[test]
    fn time_roundtrip_and_url() {
        let t = parse_oasis_time("20261016T07:00-0000").unwrap();
        assert_eq!(format_oasis_time(t), "20261016T07:00-0000");
        let end = parse_oasis_time("20261017T07:00-0000").unwrap();
        let url = build_url(&crate::market::DAPRICES.query, t, end);
        assert_eq!(
            url,
            "https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime=20261016T07:00-0000&enddatetime=20261017T07:00-0000&market_run_id=DAM&grp_type=ALL_APNODES"
        );
    }

    #[test]
    fn detects_not_ready() {
        let z = zip_of(&[("x.xml", NO_DATA_XML)]);
        assert!(matches!(parse_zip(&z).unwrap(), OasisResult::NotReady(_)));
    }

    #[test]
    fn other_xml_error_is_error() {
        let z = zip_of(&[("x.xml", &NO_DATA_XML.replace(">1000<", ">9999<"))]);
        assert!(parse_zip(&z).is_err());
    }

    #[test]
    fn detects_processing_inside_zip() {
        let err = parse_zip(&processing_zip()).unwrap_err();
        assert!(err.is::<ProcessingError>());
        assert!(err.to_string().contains("GroupZip DownLoad is in Processing"));
    }

    #[tokio::test]
    async fn retries_processing_until_csvs_are_ready() {
        let server = serve(vec![processing_zip(), processing_zip(), zip_of(&[("a.csv", "h\n1\n")])]).await;
        let mut log = Vec::new();
        let started = tokio::time::Instant::now();
        let result = fetch_url(&reqwest::Client::new(), &server.url, &mut log).await.unwrap();
        assert!(matches!(result, OasisResult::Csvs(csvs) if csvs.len() == 1));
        assert_eq!(server.requests.load(Ordering::SeqCst), 3);
        assert!(started.elapsed() >= PROCESSING_WAIT * 2);
        assert_eq!(log.iter().filter(|line| line.contains("waiting 5s")).count(), 2);
    }

    #[tokio::test]
    async fn stops_processing_retries_after_one_minute() {
        let server = serve(vec![processing_zip()]).await;
        let mut log = Vec::new();
        let started = tokio::time::Instant::now();
        let err = fetch_url(&reqwest::Client::new(), &server.url, &mut log).await.unwrap_err();
        assert!(err.to_string().contains("still processing after 60s"));
        assert!(err.to_string().contains("1015"));
        assert!(started.elapsed() >= PROCESSING_TIMEOUT);
        assert!(started.elapsed() < PROCESSING_TIMEOUT + PROCESSING_WAIT);
        let requests = server.requests.load(Ordering::SeqCst);
        assert!((2..=13).contains(&requests), "unexpected request count: {requests}");
    }

    #[tokio::test]
    async fn does_not_retry_other_oasis_responses() {
        for (xml, not_ready) in [
            (NO_DATA_XML.to_string(), true),
            (NO_DATA_XML.replace(">1000<", ">9999<"), false),
        ] {
            let server = serve(vec![zip_of(&[("error.xml", &xml)])]).await;
            let result = fetch_url(&reqwest::Client::new(), &server.url, &mut Vec::new()).await;
            if not_ready {
                assert!(matches!(result.unwrap(), OasisResult::NotReady(_)));
            } else {
                assert!(result.unwrap_err().to_string().contains("9999"));
            }
            assert_eq!(server.requests.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn returns_csvs() {
        let z = zip_of(&[("a.csv", "h\n1\n"), ("b.csv", "h\n2\n")]);
        match parse_zip(&z).unwrap() {
            OasisResult::Csvs(c) => assert_eq!(c.len(), 2),
            _ => panic!(),
        }
    }
}
