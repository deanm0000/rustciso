use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use std::io::{Cursor, Read};

const OASIS_URL: &str = "https://oasis.caiso.com/oasisapi/SingleZip";
const OASIS_FMT: &str = "%Y%m%dT%H:%M%z";
/// Each OASIS RTM interval query returns at most one hour.
pub const WINDOW: Duration = Duration::hours(1);
const RATE_LIMIT_WAIT: std::time::Duration = std::time::Duration::from_millis(5100);
const RATE_LIMIT_RETRIES: usize = 2;
/// OASIS error code for "No data returned for the specified selection".
const NO_DATA_ERR_CODE: &str = "1000";

pub fn parse_oasis_time(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_str(s.trim(), OASIS_FMT)
        .with_context(|| format!("invalid OASIS datetime '{s}', expected e.g. 20261016T07:00-0000"))?
        .with_timezone(&Utc))
}

pub fn format_oasis_time(t: DateTime<Utc>) -> String {
    t.format("%Y%m%dT%H:%M-0000").to_string()
}

pub fn build_url(start: DateTime<Utc>) -> String {
    format!(
        "{OASIS_URL}?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={}&enddatetime={}&market_run_id=RTM&grp_type=ALL",
        format_oasis_time(start),
        format_oasis_time(start + WINDOW),
    )
}

#[derive(Debug)]
pub enum OasisResult {
    /// Raw CSV file contents, one per LMP type.
    Csvs(Vec<(String, Bytes)>),
    /// OASIS has no data for the window yet.
    NotReady(String),
}

pub async fn fetch(client: &reqwest::Client, start: DateTime<Utc>, log: &mut Vec<String>) -> Result<OasisResult> {
    let url = build_url(start);
    let mut attempt = 0;
    let body = loop {
        log.push(format!("GET {url}"));
        let resp = client.get(&url).send().await.context("OASIS request failed")?;
        let status = resp.status();
        if status == StatusCode::TOO_MANY_REQUESTS && attempt < RATE_LIMIT_RETRIES {
            attempt += 1;
            log.push(format!("OASIS 429, waiting {RATE_LIMIT_WAIT:?} (retry {attempt}/{RATE_LIMIT_RETRIES})"));
            tokio::time::sleep(RATE_LIMIT_WAIT).await;
            continue;
        }
        if !status.is_success() {
            bail!("OASIS returned HTTP {status}");
        }
        break resp.bytes().await.context("reading OASIS body")?;
    };
    parse_zip(&body)
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

    const NO_DATA_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<m:OASISReport xmlns:m="http://www.caiso.com/soa/OASISReport_v1.xsd">
<m:MessagePayload><m:RTO><m:name>CAISO</m:name>
<m:ERROR>
<m:ERR_CODE>1000</m:ERR_CODE>
<m:ERR_DESC>No data returned for the specified selection</m:ERR_DESC>
</m:ERROR></m:RTO></m:MessagePayload></m:OASISReport>"#;

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
        let url = build_url(t);
        assert!(url.contains("startdatetime=20261016T07:00-0000&enddatetime=20261016T08:00-0000"));
    }

    #[test]
    fn detects_not_ready() {
        let z = zip_of(&[("x.xml", NO_DATA_XML)]);
        assert!(matches!(parse_zip(&z).unwrap(), OasisResult::NotReady(_)));
    }

    #[test]
    fn other_xml_error_is_error() {
        let z = zip_of(&[("x.xml", &NO_DATA_XML.replace(">1000<", ">1015<"))]);
        assert!(parse_zip(&z).is_err());
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
