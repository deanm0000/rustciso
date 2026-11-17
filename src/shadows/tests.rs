use super::*;
use crate::azure::FuncRequest;
use bytes::Bytes;
use datafusion::arrow::array::{Array, Float64Array, StringArray};
use serde_json::{Value, json};
use std::collections::HashMap;

fn time(value: &str) -> DateTime<Utc> {
    oasis::parse_oasis_time(value).unwrap()
}

fn payload(start: &str, market: Market) -> Payload {
    Payload {
        startdatetime: start.into(),
        stopdatetime: None,
        market_run_id: market,
        get_next_until: None,
        get_prev_until: None,
    }
}

fn window(start: &str, end: &str) -> Window {
    Window {
        start: time(start),
        end: time(end),
    }
}

#[test]
fn pacific_month_defaults_and_dst_boundaries() {
    for (start, end) in [
        ("20261016T07:00-0000", "20261101T07:00-0000"),
        ("20261101T07:00-0000", "20261201T08:00-0000"),
        ("20260301T08:00-0000", "20260401T07:00-0000"),
        ("20240201T08:00-0000", "20240301T08:00-0000"),
        ("20261201T08:00-0000", "20270101T08:00-0000"),
    ] {
        let p = payload(start, Market::Dam);
        assert_eq!(p.window().unwrap(), window(start, end));
    }
    let mut p = payload("20261001T07:00-0000", Market::Dam);
    p.stopdatetime = Some("20261016T07:00-0000".into());
    assert_eq!(p.window().unwrap().end, time("20261016T07:00-0000"));
    for end in [
        "20261001T07:00-0000",
        "20260901T07:00-0000",
        "20261102T07:00-0000",
        "bad",
        "20261002T07:05-0000",
    ] {
        p.stopdatetime = Some(end.into());
        assert!(p.window().is_err(), "{end}");
    }
    p.stopdatetime = Some("20261101T07:00-0000".into());
    assert_eq!(
        p.window().unwrap().end - p.window().unwrap().start,
        Duration::days(31)
    );
}

#[test]
fn forward_uses_latest_observed_tick_not_requested_end() {
    for market in [
        Market::Dam,
        Market::Hasp,
        Market::Ruc,
        Market::Rtm,
        Market::Interval,
    ] {
        let mut p = payload("20261001T07:00-0000", market);
        p.get_next_until = Some("20261101T07:00-0000".into());
        let latest = time("20261016T09:00-0000");
        let observed = Window {
            start: time("20261002T08:00-0000"),
            end: latest + market.tick(),
        };
        let children = p
            .continuations(p.window().unwrap(), Some(observed))
            .unwrap();
        assert_eq!(children.len(), 1);
        let child = &children[0];
        assert_eq!(time(&child.startdatetime), latest + market.tick());
        assert_eq!(child.market_run_id, market);
        assert!(child.stopdatetime.is_none() && child.get_prev_until.is_none());
        p.get_next_until = Some(oasis::format_oasis_time(latest));
        assert!(
            p.continuations(p.window().unwrap(), Some(observed))
                .unwrap()
                .is_empty()
        );
        p.get_next_until = Some(oasis::format_oasis_time(observed.end));
        assert_eq!(
            p.continuations(p.window().unwrap(), Some(observed))
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn backward_windows_do_not_overlap_observed_data_and_keep_direction() {
    for (start, prev_start, prev_end) in [
        (
            "20261016T07:00-0000",
            "20261001T07:00-0000",
            "20261016T09:00-0000",
        ),
        (
            "20261101T07:00-0000",
            "20261001T07:00-0000",
            "20261101T07:00-0000",
        ),
        (
            "20261201T08:00-0000",
            "20261101T07:00-0000",
            "20261201T08:00-0000",
        ),
        (
            "20260401T07:00-0000",
            "20260301T08:00-0000",
            "20260401T07:00-0000",
        ),
        (
            "20260101T08:00-0000",
            "20251201T08:00-0000",
            "20260101T08:00-0000",
        ),
    ] {
        let mut p = payload(start, Market::Rtm);
        p.get_prev_until = Some("20240101T08:00-0000".into());
        let win = p.window().unwrap();
        let observed = Window {
            start: win.start + Duration::hours(2),
            end: win.end,
        };
        let children = p.continuations(win, Some(observed)).unwrap();
        assert_eq!(children.len(), 1);
        let child = &children[0];
        assert_eq!(child.startdatetime, prev_start);
        assert_eq!(child.stopdatetime.as_deref(), Some(prev_end));
        assert!(child.get_next_until.is_none());
        child.window().unwrap();
    }
    let mut p = payload("20261016T07:00-0000", Market::Dam);
    p.get_prev_until = Some("20261001T07:00-0000".into());
    assert_eq!(p.continuations(p.window().unwrap(), None).unwrap().len(), 1);
    p.get_prev_until = Some("20261001T08:00-0000".into());
    assert!(
        p.continuations(p.window().unwrap(), None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn empty_historical_windows_advance_both_directions() {
    let mut p = payload("20261016T07:00-0000", Market::Dam);
    p.stopdatetime = Some("20261020T07:00-0000".into());
    p.get_next_until = Some("20261101T07:00-0000".into());
    p.get_prev_until = Some("20260901T07:00-0000".into());
    let children = p.continuations(p.window().unwrap(), None).unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].startdatetime, "20261020T07:00-0000");
    assert_eq!(children[1].startdatetime, "20261001T07:00-0000");
    assert_eq!(
        children[1].stopdatetime.as_deref(),
        Some("20261016T07:00-0000")
    );
}

#[test]
fn current_threshold_and_visibility_are_exact() {
    assert_eq!(
        current_threshold(time("20261027T19:04-0000")),
        time("20261027T18:00-0000")
    );
    assert_eq!(
        current_threshold(time("20261027T19:05-0000")),
        time("20261027T19:00-0000")
    );
    assert_eq!(
        visibility(time("20261027T19:00-0000"), time("20261027T20:04-0000")).as_secs(),
        60
    );
    assert_eq!(
        visibility(time("20261027T19:00-0000"), time("20261027T21:00-0000")).as_secs(),
        0
    );
    assert_eq!(
        visibility(time("20271027T19:00-0000"), time("20261027T20:00-0000")),
        queue::MAX_VISIBILITY
    );
}

#[test]
fn queue_payload_validation_and_endpoint_mapping() {
    assert!(is_table("ciso", "nomogram_shadows"));
    assert!(is_table("ciso", "intertie_shadows"));
    assert!(!is_table("other", "nomogram_shadows"));
    assert!(!is_table("ciso", "rtprices"));
    let mut value = json!({
        "startdatetime": "20261001T07:00-0000", "market_run_id": "DaM",
        "retries": 2, "max_retries": 5
    });
    let req = FuncRequest {
        Data: HashMap::from([(
            "arbitrary".into(),
            Value::String(value.to_string().to_string()),
        )]),
        Metadata: HashMap::new(),
    };
    let job = parse_job(Ok(Json(req))).unwrap();
    assert_eq!(job.payload.market_run_id, Market::Dam);
    assert_eq!(job.retry.retries, Some(2));
    assert_eq!(serde_json::to_value(&job).unwrap()["market_run_id"], "dam");
    for (key, invalid) in [
        ("market_run_id", json!("rtpd")),
        ("stopdatetime", json!("bad")),
        ("get_next_until", json!("bad")),
        ("get_prev_until", json!("bad")),
        ("retries", json!(-1)),
        ("enddatetime", json!("20261016T07:00-0000")),
    ] {
        let mut invalid_job = value.clone();
        invalid_job[key] = invalid;
        let req = FuncRequest {
            Data: HashMap::from([("x".into(), invalid_job)]),
            Metadata: HashMap::new(),
        };
        assert!(parse_job(Ok(Json(req))).is_err(), "{key}");
    }
    value["market_run_id"] = json!("INTERVAL");
    let job: Job = serde_json::from_value(value).unwrap();
    let win = job.payload.window().unwrap();
    for report in REPORTS {
        for market in [
            Market::Dam,
            Market::Hasp,
            Market::Rtm,
            Market::Ruc,
            Market::Interval,
        ] {
            let url = url::Url::parse(&report.url(market, win)).unwrap();
            let params: HashMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(params["market_run_id"], market.upstream());
            assert_eq!(params["resultformat"], "6");
            let (query, version) = match (report, market) {
                (Report::Nomogram, Market::Interval) => ("PRC_RTM_NOMOGRAM", "1"),
                (Report::Nomogram, _) => ("PRC_NOMOGRAM", "13"),
                (Report::Intertie, Market::Interval) => ("PRC_RTM_FLOWGATE", "12"),
                (Report::Intertie, _) => ("PRC_CNSTR", "13"),
            };
            assert_eq!(params["queryname"], query);
            assert_eq!(params["version"], version);
            assert_eq!(
                params[if report == Report::Nomogram {
                    "nomogram_id"
                } else {
                    "ti_id"
                }],
                "ALL"
            );
        }
    }
}

fn csv(report: Report, market: Market) -> (String, Bytes) {
    let interval = market == Market::Interval;
    let (header, row) = if interval {
        let (header, row) = match report {
            Report::Nomogram => ("NOMOGRAM_ID", "N1"),
            Report::Intertie => ("TI_ID,TI_DIRECTION,LIMIT_TYPE", "T1,I,EA"),
        };
        (
            format!(
                "INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,MARKET_RUN_ID,{header},CONSTRAINT_CAUSE,PRC"
            ),
            format!(
                "{},2026-07-03T07:05:00Z,RTM,{row},Base Case,-1.25",
                "2026-07-03T07:00:00Z"
            ),
        )
    } else {
        let (header, row) = match report {
            Report::Nomogram => ("NOMOGRAM_ID", "N1"),
            Report::Intertie => ("TRANSMISSION_INTERFACE_ID,DIRECTION,LIMIT_TYPE", "T1,E,"),
        };
        let end = if market == Market::Rtm {
            "07:15"
        } else {
            "08:00"
        };
        (
            format!(
                "MKT_TYPE,INTERVALSTARTTIME_GMT,INTERVALENDTIME_GMT,{header},CONSTRAINT_CAUSE,PRODUCT,PRICE,GROUP"
            ),
            format!(
                "{},2026-07-03T07:00:00Z,2026-07-03T{end}:00Z,{row},Base Case,{},-1.25,1",
                market.upstream(),
                if market == Market::Dam { "IRU" } else { "" }
            ),
        )
    };
    ("test.csv".into(), Bytes::from(format!("{header}\n{row}\n")))
}

#[tokio::test]
async fn normalizes_every_report_and_market_without_losing_dimensions() {
    let win = window("20260703T07:00-0000", "20260802T07:00-0000");
    for report in REPORTS {
        for market in [
            Market::Dam,
            Market::Hasp,
            Market::Rtm,
            Market::Ruc,
            Market::Interval,
        ] {
            let dl = time("20261027T19:00-0000");
            let data = transform::normalize(&[csv(report, market)], report, market, win, dl)
                .await
                .unwrap();
            assert_eq!(
                data.observed,
                Some(Window {
                    start: win.start,
                    end: win.start + market.tick()
                })
            );
            let batch = &data.batches[0];
            assert_eq!(batch.num_rows(), 1);
            assert!(
                batch
                    .schema()
                    .column_with_name("interval_minutes")
                    .is_none()
            );
            let markets = batch
                .column_by_name("market_type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(markets.value(0), market.name());
            let product = batch
                .column_by_name("product")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            if market == Market::Dam {
                assert_eq!(product.value(0), "IRU");
            } else {
                assert!(product.is_null(0));
            }
            let prices = batch
                .column_by_name("shadow_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(prices.value(0), -1.25);
        }
    }
}

#[tokio::test]
async fn rejects_invalid_intervals_markets_prices_and_required_columns() {
    let win = window("20260703T07:00-0000", "20260802T07:00-0000");
    let (name, body) = csv(Report::Nomogram, Market::Dam);
    let original = String::from_utf8(body.to_vec()).unwrap();
    for (from, to) in [
        ("2026-07-03T08:00:00Z", "2026-07-03T08:15:00Z"),
        ("2026-07-03T07:00:00Z", "2026-07-02T07:00:00Z"),
        ("DAM,", "RTM,"),
        ("-1.25", "not-a-price"),
        ("-1.25", "NaN"),
        ("-1.25", ""),
        ("N1,", ","),
        ("PRICE", "MISSING_PRICE"),
        ("PRODUCT", "MISSING_PRODUCT"),
    ] {
        let broken = (name.clone(), Bytes::from(original.replace(from, to)));
        assert!(
            transform::normalize(&[broken], Report::Nomogram, Market::Dam, win, Utc::now())
                .await
                .is_err(),
            "{from} -> {to}"
        );
    }
    let header = original.lines().next().unwrap();
    let empty = (name, Bytes::from(format!("{header}\n")));
    let result = transform::normalize(&[empty], Report::Nomogram, Market::Dam, win, Utc::now())
        .await
        .unwrap();
    assert!(result.observed.is_none() && result.batches.is_empty());
}

#[tokio::test]
async fn queue_delivery_preserves_retry_state_direction_and_trigger_contracts() {
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    let captured = Arc::new(Mutex::new(Vec::new()));
    let reject = Arc::new(AtomicBool::new(false));
    let requests = captured.clone();
    let failing = reject.clone();
    let app = axum::Router::new().fallback(move |uri: axum::http::Uri, body: Bytes| {
        requests.lock().unwrap().push((uri, body));
        let failure = failing.load(Ordering::SeqCst);
        async move {
            if failure {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::CREATED
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let account = crate::config::StorageAccount {
        name: "test".into(),
        key: B64.encode("key"),
        queue_endpoint: format!("http://{addr}"),
    };
    let http = reqwest::Client::new();
    let cfg = dkplus::Table::default().cfg;
    let state = Arc::new(AppState {
        cfg: crate::config::Config {
            postgres: tokio_postgres::Config::new(),
            lake_account: account.clone(),
            queue_account: account.clone(),
            repack_run_bytes: cfg.repack_run_bytes,
            repack_node_bytes: cfg.repack_node_bytes,
        },
        http: http.clone(),
        queues: REPORTS
            .into_iter()
            .map(|report| {
                (
                    report.name(),
                    queue::QueueClient::new(http.clone(), account.clone(), report.queue_name()),
                )
            })
            .collect(),
        repack_queue: queue::QueueClient::new(http.clone(), account.clone(), "repack"),
        maintenance_repack_queue: queue::QueueClient::new(
            http.clone(),
            account,
            "maintenance-repack",
        ),
    });
    let mut p = payload("20261016T07:00-0000", Market::Interval);
    p.get_next_until = Some("20261201T08:00-0000".into());
    p.get_prev_until = Some("20260901T07:00-0000".into());
    let win = p.window().unwrap();
    let original = Job {
        payload: p,
        retry: queue::RetryState {
            retries: Some(2),
            max_retries: Some(5),
        },
    };
    let log = InvocationLog::new(None);
    complete_window(
        &state,
        Report::Nomogram,
        original,
        win,
        None,
        time("20261016T07:04-0000"),
        &log,
    )
    .await
    .unwrap();
    let request = captured.lock().unwrap()[0].clone();
    assert_eq!(request.0.path(), "/nomogram-shadows/messages");
    assert!(request.0.query().unwrap().contains("visibilitytimeout=60"));
    let body = String::from_utf8(request.1.to_vec()).unwrap();
    let encoded = body
        .split("<MessageText>")
        .nth(1)
        .unwrap()
        .split("</MessageText>")
        .next()
        .unwrap();
    let retry: Job = serde_json::from_slice(&B64.decode(encoded).unwrap()).unwrap();
    assert_eq!(retry.retry.retries, Some(2));
    assert_eq!(retry.payload.startdatetime, "20261016T07:00-0000");
    assert!(retry.payload.get_next_until.is_some() && retry.payload.get_prev_until.is_some());
    captured.lock().unwrap().clear();
    let win = retry.payload.window().unwrap();
    complete_window(
        &state,
        Report::Intertie,
        retry,
        win,
        None,
        time("20261027T19:00-0000"),
        &log,
    )
    .await
    .unwrap();
    let requests = captured.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    for (i, (uri, body)) in requests.iter().enumerate() {
        assert_eq!(uri.path(), "/intertie-shadows/messages");
        let body = String::from_utf8(body.to_vec()).unwrap();
        let encoded = body
            .split("<MessageText>")
            .nth(1)
            .unwrap()
            .split("</MessageText>")
            .next()
            .unwrap();
        let job: Job = serde_json::from_slice(&B64.decode(encoded).unwrap()).unwrap();
        assert_eq!(job.retry.retries, None);
        assert_eq!(job.retry.max_retries, Some(5));
        assert_eq!(job.payload.market_run_id, Market::Interval);
        assert_eq!(job.payload.get_next_until.is_some(), i == 0);
        assert_eq!(job.payload.get_prev_until.is_some(), i == 1);
    }
    captured.lock().unwrap().clear();
    let mut p = payload("20261016T07:00-0000", Market::Interval);
    p.get_next_until = Some("20261020T07:00-0000".into());
    p.get_prev_until = Some("20261001T07:00-0000".into());
    let win = p.window().unwrap();
    complete_window(
        &state,
        Report::Nomogram,
        Job {
            payload: p,
            retry: Default::default(),
        },
        win,
        Some(window("20261016T08:00-0000", "20261016T09:05-0000")),
        time("20261027T19:00-0000"),
        &log,
    )
    .await
    .unwrap();
    let requests = captured.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    for (i, (_, body)) in requests.iter().enumerate() {
        let body = String::from_utf8(body.to_vec()).unwrap();
        let encoded = body
            .split("<MessageText>")
            .nth(1)
            .unwrap()
            .split("</MessageText>")
            .next()
            .unwrap();
        let job: Job = serde_json::from_slice(&B64.decode(encoded).unwrap()).unwrap();
        if i == 0 {
            assert_eq!(job.payload.startdatetime, "20261016T09:05-0000");
            assert!(job.payload.stopdatetime.is_none());
        } else {
            assert_eq!(job.payload.startdatetime, "20261001T07:00-0000");
            assert_eq!(
                job.payload.stopdatetime.as_deref(),
                Some("20261016T08:00-0000")
            );
        }
    }
    reject.store(true, Ordering::SeqCst);
    let job = Job {
        payload: payload("20261016T07:00-0000", Market::Dam),
        retry: Default::default(),
    };
    assert!(
        enqueue(
            &state,
            Report::Nomogram,
            &job,
            std::time::Duration::ZERO,
            &log
        )
        .await
        .is_err()
    );

    let app = crate::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handler = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for report in REPORTS {
        let response = http
            .post(format!("http://{address}/{}", report.name()))
            .header("Content-Type", "application/json")
            .body(
                json!({"Data": {"any": {"startdatetime": "bad", "market_run_id": "dam"}}})
                    .to_string(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert!(
            response["Logs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v.as_str().unwrap().contains("bad"))
        );
        assert!(response["Outputs"].is_null());
        let binding: Value = serde_json::from_str(
            &std::fs::read_to_string(format!("{}/function.json", report.name())).unwrap(),
        )
        .unwrap();
        assert_eq!(binding["bindings"][0]["queueName"], report.queue_name());
        assert_eq!(binding["bindings"][0]["type"], "queueTrigger");
    }
    handler.abort();
    server.abort();
}

#[tokio::test]
#[ignore = "requires downloaded 30-day CSV ZIPs in SHADOW_FIXTURE_DIR"]
async fn validates_all_downloaded_rows() {
    use std::io::Read;
    let root = std::path::PathBuf::from(std::env::var("SHADOW_FIXTURE_DIR").unwrap());
    let win = window("20260703T07:00-0000", "20260802T07:00-0000");
    let mut total = 0;
    for report in REPORTS {
        for market in [
            Market::Dam,
            Market::Hasp,
            Market::Rtm,
            Market::Ruc,
            Market::Interval,
        ] {
            let stem = if market == Market::Interval {
                format!("rtm_interval_{}", report.name())
            } else {
                format!("{}_{}", market.name(), report.name())
            };
            let mut archive = zip::ZipArchive::new(
                std::fs::File::open(root.join(format!("{stem}.zip"))).unwrap(),
            )
            .unwrap();
            let mut csvs = Vec::new();
            for i in 0..archive.len() {
                let mut file = archive.by_index(i).unwrap();
                let name = file.name().unwrap().to_string();
                if name.ends_with(".csv") {
                    let mut body = Vec::new();
                    file.read_to_end(&mut body).unwrap();
                    csvs.push((name, Bytes::from(body)));
                }
            }
            let result = transform::normalize(&csvs, report, market, win, Utc::now())
                .await
                .unwrap();
            let rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
            assert!(result.observed.is_some() && rows > 0);
            println!("{stem}: {rows} rows");
            total += rows;
        }
    }
    assert_eq!(total, 119849);
}
