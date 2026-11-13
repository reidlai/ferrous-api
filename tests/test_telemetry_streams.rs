//! Integration tests for Story 8.5: Benchmark & Pipeline Telemetry Streaming Endpoints
//! (`/api/v1/benchmarks/{run_id}/stream` and `/api/v1/telemetry/pipeline/stream`).

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    create_router_with_app_state,
    generated::models::{BenchmarkTelemetryItem, PipelineTelemetryItem, ProblemDetails},
    state::AppState,
    telemetry::{
        PipelineCollectorConfig, PipelineTelemetryHub, SOURCE_STATUS_AVAILABLE,
        SOURCE_STATUS_UNAVAILABLE,
    },
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower::ServiceExt;

/// Parses SSE `data: <json>` lines into `BenchmarkTelemetryItem` records.
fn parse_benchmark_sse_events(raw_sse: &str) -> Vec<BenchmarkTelemetryItem> {
    raw_sse
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|json_str| serde_json::from_str::<BenchmarkTelemetryItem>(json_str).ok())
        .collect()
}

/// Parses SSE `data: <json>` lines into `PipelineTelemetryItem` records.
fn parse_pipeline_sse_events(raw_sse: &str) -> Vec<PipelineTelemetryItem> {
    raw_sse
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|json_str| serde_json::from_str::<PipelineTelemetryItem>(json_str).ok())
        .collect()
}

/// Reads the next chunk of SSE text from a response body within a timeout.
async fn next_sse_chunk(body: &mut Body) -> Option<String> {
    loop {
        let frame_res = tokio::time::timeout(Duration::from_millis(500), body.frame())
            .await
            .expect("Timed out waiting for next SSE frame")?;
        let frame = frame_res.expect("SSE body frame error");
        if let Ok(data) = frame.into_data() {
            let text = String::from_utf8_lossy(&data).to_string();
            if !text.trim().is_empty() {
                return Some(text);
            }
        }
    }
}

/// Spawns a lightweight loopback HTTP server returning `response_json` on every GET request.
async fn spawn_mock_json_http_server(response_json: Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body_str = serde_json::to_string(&response_json).unwrap();

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let body = body_str.clone();
            tokio::spawn(async move {
                let mut req_buf = [0u8; 1024];
                let _ = socket.read(&mut req_buf).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    format!("http://{addr}/stats")
}

#[tokio::test]
async fn test_real_time_benchmark_telemetry_stream_and_run_id_filtering() {
    let (state, _publisher, _validator, _event_broadcaster) = AppState::with_in_memory_live();
    let bench_broadcaster = Arc::clone(&state.benchmark_broadcaster);
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/bench-run-777/stream")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.starts_with("text/event-stream"));

    // Publish events for a different run (must be filtered out) and for bench-run-777
    bench_broadcaster.publish_step("bench-other-999", 1, 9000, 2100, 30, 8);
    bench_broadcaster.publish(
        BenchmarkTelemetryItem::new("bench-run-777", 2, 11840, 2950, 6)
            .with_jitter(42)
            .with_model("qwen2.5:7b", "ollama"),
    );
    bench_broadcaster.publish(
        BenchmarkTelemetryItem::new("bench-run-777", 3, 11840, 3010, 7)
            .with_jitter(60)
            .with_model("qwen2.5:7b", "ollama"),
    );

    let mut body = response.into_body();
    let mut collected = Vec::new();
    while collected.len() < 3 {
        let chunk = next_sse_chunk(&mut body)
            .await
            .expect("Expected benchmark SSE chunk");
        assert!(
            chunk.contains("event: benchmark_telemetry"),
            "Expected event: benchmark_telemetry header: {chunk}"
        );
        collected.extend(parse_benchmark_sse_events(&chunk));
    }

    assert!(
        collected.iter().all(|item| item.run_id == "bench-run-777"),
        "Benchmark SSE stream must only emit items for bench-run-777: {collected:?}"
    );
    assert_eq!(collected[1].step, 2);
    assert_eq!(collected[1].ttft_micros, 11840);
    assert_eq!(collected[1].itl_micros, 2950);
    assert_eq!(collected[1].itl_jitter_micros, Some(42));
    assert_eq!(collected[1].token_delta, 6);
    assert_eq!(collected[1].model_id.as_deref(), Some("qwen2.5:7b"));
    assert_eq!(collected[1].provider.as_deref(), Some("ollama"));
    assert!(collected[1].tps.unwrap() > 0.0);

    assert_eq!(collected[2].step, 3);
    assert_eq!(collected[2].itl_jitter_micros, Some(60));
    assert_eq!(collected[2].token_delta, 7);
}

#[tokio::test]
async fn test_benchmark_stream_blank_run_id_400_and_unregistered_run_404() {
    let (state, _publisher, _validator) = AppState::with_in_memory();
    state
        .benchmark_broadcaster
        .set_require_registered_runs(true);
    state.benchmark_broadcaster.register_run("known-run-1");

    let app = create_router_with_app_state(state);

    // 1. Blank run_id returns 400 Bad Request ProblemDetails
    let bad_req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/%20%20/stream")
        .body(Body::empty())
        .unwrap();
    let bad_res = app.clone().oneshot(bad_req).await.unwrap();
    assert_eq!(bad_res.status(), StatusCode::BAD_REQUEST);
    let bad_bytes = bad_res.into_body().collect().await.unwrap().to_bytes();
    let bad_problem: ProblemDetails = serde_json::from_slice(&bad_bytes).unwrap();
    assert_eq!(bad_problem.status, 400);
    assert_eq!(
        bad_problem.instance.as_deref(),
        Some("/api/v1/benchmarks/{run_id}/stream")
    );

    // 2. Unregistered run_id when require_registered_runs is enabled returns 404 Not Found
    let missing_req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/unknown-run-404/stream")
        .body(Body::empty())
        .unwrap();
    let missing_res = app.clone().oneshot(missing_req).await.unwrap();
    assert_eq!(missing_res.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        missing_res.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    let missing_bytes = missing_res.into_body().collect().await.unwrap().to_bytes();
    let missing_problem: ProblemDetails = serde_json::from_slice(&missing_bytes).unwrap();
    assert_eq!(missing_problem.status, 404);
    assert_eq!(
        missing_problem.instance.as_deref(),
        Some("/api/v1/benchmarks/unknown-run-404/stream")
    );

    // 3. Registered run_id returns 200 OK text/event-stream
    let ok_req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/known-run-1/stream")
        .body(Body::empty())
        .unwrap();
    let ok_res = app.oneshot(ok_req).await.unwrap();
    assert_eq!(ok_res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_multi_source_pipeline_telemetry_collection_from_iggy_arroyo_and_delta_logs() {
    // 1. Start mock Iggy (:3000 equivalent) and Arroyo (:5115 equivalent) HTTP endpoints
    let iggy_url = spawn_mock_json_http_server(json!({
        "iggy_commit_lag": 3,
        "iggy_fsync_latency_us": 290,
        "topics": {
            "olist/orders_stream": {
                "messages_per_sec": 250.5,
                "high_watermark": 98450,
                "consumer_lag": 14
            }
        }
    }))
    .await;

    let arroyo_url = spawn_mock_json_http_server(json!({
        "data": [
            {
                "id": "job-olist-orders",
                "watermark_lag_ms": 38
            }
        ]
    }))
    .await;

    // 2. Create temporary Delta Lake directory with 3 _delta_log JSON commit files
    let temp_dir = std::env::temp_dir().join(format!(
        "ferrous_api_delta_telemetry_{}",
        uuid::Uuid::now_v7()
    ));
    let delta_log_dir = temp_dir.join("orders_current_state").join("_delta_log");
    fs::create_dir_all(&delta_log_dir).unwrap();
    for version in 0..3 {
        let commit_path = delta_log_dir.join(format!("{version:020}.json"));
        fs::write(
            &commit_path,
            "{\"commitInfo\":{\"timestamp\":1700000000}}\n",
        )
        .unwrap();
    }

    let (state, _publisher, _validator) = AppState::with_in_memory();
    state
        .pipeline_telemetry
        .set_collector_config(PipelineCollectorConfig {
            iggy_stats_url: Some(iggy_url),
            arroyo_api_url: Some(arroyo_url),
            delta_data_dir: Some(temp_dir.clone()),
            http_timeout: Duration::from_millis(250),
        });

    // Poll all 3 sources once before connecting SSE client
    let polled = state.pipeline_telemetry.poll_sources_once().await;
    assert_eq!(polled.iggy_commit_lag, 3);
    assert_eq!(polled.iggy_fsync_latency_us, Some(290));
    assert_eq!(polled.arroyo_watermark_lag_ms, 38);
    assert_eq!(polled.delta_commit_count, 3);
    assert_eq!(polled.olist_replay_rate_eps, Some(250.5));
    assert_eq!(polled.olist_high_watermark, Some(98450));
    assert_eq!(polled.olist_consumer_lag, Some(14));
    assert_eq!(polled.iggy_status.as_deref(), Some(SOURCE_STATUS_AVAILABLE));
    assert_eq!(
        polled.arroyo_status.as_deref(),
        Some(SOURCE_STATUS_AVAILABLE)
    );
    assert_eq!(
        polled.delta_status.as_deref(),
        Some(SOURCE_STATUS_AVAILABLE)
    );

    let app = create_router_with_app_state(state);
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/telemetry/pipeline/stream")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(body_str.contains("event: pipeline_telemetry"));

    let events = parse_pipeline_sse_events(&body_str);
    assert!(!events.is_empty());
    assert_eq!(events[0].iggy_fsync_latency_us, Some(290));
    assert_eq!(events[0].arroyo_watermark_lag_ms, 38);
    assert_eq!(events[0].delta_commit_count, 3);
    assert_eq!(events[0].olist_replay_rate_eps, Some(250.5));
    assert_eq!(events[0].olist_high_watermark, Some(98450));
    assert_eq!(events[0].olist_consumer_lag, Some(14));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_partial_upstream_source_unavailability_marks_unreachable_source_unavailable() {
    // Reachable Iggy server, but unreachable Arroyo port and non-existent Delta path
    let iggy_url = spawn_mock_json_http_server(json!({
        "iggy_commit_lag": 1,
        "iggy_fsync_latency_us": 310,
        "olist_replay_rate_eps": 25.0,
        "olist_high_watermark": 5000,
        "olist_consumer_lag": 2
    }))
    .await;

    let (state, _publisher, _validator) = AppState::with_in_memory();
    state
        .pipeline_telemetry
        .set_collector_config(PipelineCollectorConfig {
            iggy_stats_url: Some(iggy_url),
            arroyo_api_url: Some("http://127.0.0.1:9/api/v1/pipelines".to_string()),
            delta_data_dir: Some(PathBuf::from("/nonexistent/delta/path/8-5")),
            http_timeout: Duration::from_millis(100),
        });

    let polled = state.pipeline_telemetry.poll_sources_once().await;
    assert_eq!(polled.iggy_status.as_deref(), Some(SOURCE_STATUS_AVAILABLE));
    assert_eq!(
        polled.arroyo_status.as_deref(),
        Some(SOURCE_STATUS_UNAVAILABLE)
    );
    assert_eq!(
        polled.delta_status.as_deref(),
        Some(SOURCE_STATUS_UNAVAILABLE)
    );
    assert_eq!(polled.iggy_fsync_latency_us, Some(310));
    assert_eq!(polled.olist_high_watermark, Some(5000));

    let app = create_router_with_app_state(state);
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/telemetry/pipeline/stream")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let events = parse_pipeline_sse_events(&String::from_utf8_lossy(&body_bytes));
    assert!(!events.is_empty());
    assert_eq!(
        events[0].iggy_status.as_deref(),
        Some(SOURCE_STATUS_AVAILABLE)
    );
    assert_eq!(
        events[0].arroyo_status.as_deref(),
        Some(SOURCE_STATUS_UNAVAILABLE)
    );
    assert_eq!(
        events[0].delta_status.as_deref(),
        Some(SOURCE_STATUS_UNAVAILABLE)
    );
}

#[tokio::test]
async fn test_telemetry_streams_disconnect_preempts_and_aborts_tasks_under_50ms() {
    let (state, _publisher, _validator, _event_broadcaster) = AppState::with_in_memory_live();
    let bench_broadcaster = Arc::clone(&state.benchmark_broadcaster);
    let pipeline_hub = Arc::clone(&state.pipeline_telemetry);
    let app = create_router_with_app_state(state);

    // 1. Benchmark stream disconnect aborts background benchmark task in <50ms
    let bench_req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/bench-abort-run/stream")
        .body(Body::empty())
        .unwrap();
    let bench_res = app.clone().oneshot(bench_req).await.unwrap();
    assert_eq!(bench_res.status(), StatusCode::OK);
    let mut bench_body = bench_res.into_body();

    let bench_task = bench_broadcaster.spawn_benchmark_run(
        "bench-abort-run",
        200,
        10500,
        2800,
        Duration::from_millis(5),
    );
    let _ = next_sse_chunk(&mut bench_body).await.unwrap();

    let start_bench_abort = Instant::now();
    drop(bench_body);
    bench_task.wait_for_termination().await;
    let bench_abort_latency = start_bench_abort.elapsed();

    assert!(bench_task.is_cancelled());
    assert!(bench_task.is_terminated());
    assert!(
        bench_abort_latency < Duration::from_millis(50),
        "Benchmark SSE disconnect abort latency {:?} exceeded <50ms SLA",
        bench_abort_latency
    );

    // 2. Pipeline telemetry stream disconnect aborts background poller in <50ms
    let pipe_req = Request::builder()
        .method("GET")
        .uri("/api/v1/telemetry/pipeline/stream")
        .body(Body::empty())
        .unwrap();
    let pipe_res = app.oneshot(pipe_req).await.unwrap();
    assert_eq!(pipe_res.status(), StatusCode::OK);
    let mut pipe_body = pipe_res.into_body();

    let poller_task = pipeline_hub.spawn_poller(Duration::from_millis(10));
    let _ = next_sse_chunk(&mut pipe_body).await.unwrap();

    let start_pipe_abort = Instant::now();
    drop(pipe_body);
    poller_task.wait_for_termination().await;
    let pipe_abort_latency = start_pipe_abort.elapsed();

    assert!(poller_task.is_cancelled());
    assert!(poller_task.is_terminated());
    assert!(
        pipe_abort_latency < Duration::from_millis(50),
        "Pipeline telemetry SSE disconnect abort latency {:?} exceeded <50ms SLA",
        pipe_abort_latency
    );
}

#[tokio::test]
async fn test_no_endpoint_starts_pauses_or_controls_replayer() {
    let (state, _publisher, _validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    for forbidden_route in [
        "/api/v1/replayer/start",
        "/api/v1/replayer/pause",
        "/api/v1/replayer/stop",
        "/api/v1/replayer/speed",
        "/api/v1/telemetry/pipeline/control",
    ] {
        let req = Request::builder()
            .method("POST")
            .uri(forbidden_route)
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            res.status(),
            StatusCode::NOT_FOUND,
            "Replayer control route {forbidden_route} must not exist (FR-30)"
        );
    }
}

async fn spawn_chunked_json_server(payload: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut req_buf = [0u8; 1024];
            let _ = socket.read(&mut req_buf).await;
            let chunk_hex = format!("{:x}", payload.len());
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{chunk_hex}\r\n{payload}\r\n0\r\n\r\n"
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        }
    });
    format!("http://{addr}/stats")
}

#[tokio::test]
async fn test_pipeline_telemetry_streams_live_broadcast_frames_over_sse() {
    let iggy_url = spawn_chunked_json_server(
        r#"{"iggy_commit_lag":3,"iggy_fsync_latency_us":190,"olist_replay_rate_eps":31.5,"olist_high_watermark":7777,"olist_consumer_lag":2}"#,
    )
    .await;
    let arroyo_url = spawn_mock_json_http_server(json!({"arroyo_watermark_lag_ms": 9})).await;

    let (state, _publisher, _validator, _broadcaster) = AppState::with_in_memory_live();
    state
        .pipeline_telemetry
        .set_collector_config(PipelineCollectorConfig {
            iggy_stats_url: Some(iggy_url),
            arroyo_api_url: Some(arroyo_url),
            delta_data_dir: None,
            http_timeout: Duration::from_millis(500),
        });

    let hub = state.pipeline_telemetry.clone();
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/telemetry/pipeline/stream")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let mut body = res.into_body();

    // First frame is the initial snapshot (id: pipeline-1).
    let initial_chunk = next_sse_chunk(&mut body).await.unwrap();
    assert!(initial_chunk.contains("id: pipeline-1"));

    // Spawn background poller and verify live broadcast frames beyond initial_item arrive.
    let poller = hub.spawn_background_poller(Duration::from_millis(15));

    let live_chunk_1 = next_sse_chunk(&mut body).await.unwrap();
    let live_chunk_2 = next_sse_chunk(&mut body).await.unwrap();
    assert!(live_chunk_1.contains("event: pipeline_telemetry"));
    assert!(live_chunk_1.contains("id: pipeline-2"));
    assert!(live_chunk_2.contains("id: pipeline-3"));

    let data_line = live_chunk_1
        .lines()
        .find(|l| l.starts_with("data:"))
        .unwrap()
        .trim_start_matches("data:")
        .trim();
    let parsed: serde_json::Value = serde_json::from_str(data_line).unwrap();
    assert_eq!(parsed["iggy_commit_lag"], 3);
    assert_eq!(parsed["iggy_fsync_latency_us"], 190);
    assert_eq!(parsed["arroyo_watermark_lag_ms"], 9);
    assert_eq!(parsed["iggy_status"], "available");
    assert_eq!(parsed["arroyo_status"], "available");
    assert_eq!(parsed["delta_status"], "unavailable");

    hub.cancel_all();
    poller.wait_for_termination().await;
    assert!(poller.is_terminated());
}

#[tokio::test]
async fn test_pipeline_telemetry_rejects_https_and_non_200_probe_urls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut req_buf = [0u8; 1024];
            let _ = socket.read(&mut req_buf).await;
            let _ = socket
                .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                .await;
        }
    });

    let hub = PipelineTelemetryHub::new();
    hub.set_collector_config(PipelineCollectorConfig {
        iggy_stats_url: Some("https://127.0.0.1:3000/stats".to_string()),
        arroyo_api_url: Some(format!("http://{addr}/api/v1/pipelines")),
        delta_data_dir: None,
        http_timeout: Duration::from_millis(500),
    });

    let snapshot = hub.poll_sources_once().await;
    assert_eq!(snapshot.iggy_status.as_deref(), Some("unavailable"));
    assert_eq!(snapshot.arroyo_status.as_deref(), Some("unavailable"));
    assert_eq!(snapshot.delta_status.as_deref(), Some("unavailable"));
}
