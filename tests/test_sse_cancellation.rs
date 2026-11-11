//! Integration tests for Story 8.4: Native Axum Server-Sent Events (SSE) &
//! Tokio `CancellationToken` Preempt-and-Abort (`<50ms` SLA).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    auth::{JwtValidator, SCOPE_API_WRITE},
    create_router_with_app_state,
    generated::models::{Claims, CommandAcceptedResponse, DomainEventStreamItem, ProblemDetails},
    publisher::{IggyPublisher, InMemoryIggyPublisher},
    sse::EventStreamBroadcaster,
    state::AppState,
};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

fn issue_write_bearer(validator: &JwtValidator) -> String {
    let claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_WRITE);
    let token = validator.issue_token(&claims).unwrap();
    format!("Bearer {token}")
}

/// Parses SSE `data: <json>` lines from raw SSE frame text into `DomainEventStreamItem` records.
fn parse_sse_domain_events(raw_sse: &str) -> Vec<DomainEventStreamItem> {
    raw_sse
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|json_str| serde_json::from_str::<DomainEventStreamItem>(json_str).ok())
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

#[tokio::test]
async fn test_live_domain_event_and_llm_token_streaming_over_sse() {
    let (state, _publisher, _validator, broadcaster) = AppState::with_in_memory_live();
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream")
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

    // Publish domain events and LLM agent thought/token/proposal chunks
    broadcaster.publish_domain_event(
        "OrderCreated",
        "ord-live-1",
        json!({ "total_amount_cents": 4500 }),
    );
    broadcaster.publish_agent_thought("ord-live-1", "Evaluating customer courtesy credit policy");
    broadcaster.publish_agent_token("ord-live-1", "Approving ", 1);
    broadcaster.publish_agent_token("ord-live-1", "$15.00 credit", 2);
    broadcaster.publish_agent_proposal(
        "ord-live-1",
        json!({ "action": "IssueCourtesyCredit", "amount_cents": 1500 }),
    );

    let mut body = response.into_body();
    let mut collected_events = Vec::new();

    while collected_events.len() < 6 {
        let chunk = next_sse_chunk(&mut body).await.expect("Expected SSE chunk");
        assert!(
            chunk.contains("event: domain_event"),
            "Chunk must use event: domain_event header: {chunk}"
        );
        collected_events.extend(parse_sse_domain_events(&chunk));
    }

    let event_types: Vec<&str> = collected_events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(
        event_types,
        vec![
            "StreamConnected",
            "OrderCreated",
            "AgentThought",
            "AgentToken",
            "AgentToken",
            "AgentProposal"
        ]
    );
    assert_eq!(collected_events[1].aggregate_id, "ord-live-1");
    assert_eq!(
        collected_events[2].payload["thought"],
        "Evaluating customer courtesy credit policy"
    );
    assert_eq!(collected_events[3].payload["token"], "Approving ");
    assert_eq!(collected_events[4].payload["token"], "$15.00 credit");
    assert_eq!(
        collected_events[5].payload["action"],
        "IssueCourtesyCredit"
    );
}

#[tokio::test]
async fn test_filtered_sse_subscription_by_order_id_and_event_types() {
    let (state, _publisher, _validator) = AppState::with_in_memory();
    let broadcaster = Arc::clone(&state.event_broadcaster);
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream?order_id=ord-100&event_types=CreditIssued,AgentToken")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Publish a mix of matching and non-matching events across orders and event types
    broadcaster.publish_domain_event(
        "CreditIssued",
        "ord-200", // wrong order_id
        json!({ "amount_cents": 999 }),
    );
    broadcaster.publish_agent_thought(
        "ord-100", // right order_id, excluded event_type
        "Internal reasoning step",
    );
    broadcaster.publish_domain_event(
        "CreditIssued",
        "ord-100", // matches both order_id and event_types
        json!({ "amount_cents": 2500 }),
    );
    broadcaster.publish_domain_event(
        "OrderCancelled",
        "ord-100", // right order_id, excluded event_type
        json!({ "reason": "Ignored type" }),
    );
    broadcaster.publish_agent_token(
        "ord-100", // matches both order_id and event_types
        "Credit confirmed",
        1,
    );

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    let events = parse_sse_domain_events(&body_str);

    assert_eq!(events.len(), 3, "Expected StreamConnected + 2 matching events, got: {events:?}");
    assert_eq!(events[0].event_type, "StreamConnected");
    assert_eq!(events[0].aggregate_id, "ord-100");

    assert_eq!(events[1].event_type, "CreditIssued");
    assert_eq!(events[1].aggregate_id, "ord-100");
    assert_eq!(events[1].payload["amount_cents"], 2500);

    assert_eq!(events[2].event_type, "AgentToken");
    assert_eq!(events[2].aggregate_id, "ord-100");
    assert_eq!(events[2].payload["token"], "Credit confirmed");
}

#[tokio::test]
async fn test_blank_order_id_or_event_types_returns_400_rfc7807_problem_details() {
    let (state, _publisher, _validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    for invalid_uri in [
        "/api/v1/events/stream?order_id=%20%20",
        "/api/v1/events/stream?event_types=%20",
        "/api/v1/events/stream?order_id=ord-100&event_types=CreditIssued,",
        "/api/v1/events/stream?order_id=&event_types=",
    ] {
        let req = Request::builder()
            .method("GET")
            .uri(invalid_uri)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "Expected 400 Bad Request for URI {invalid_uri}"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(problem.status, 400);
        assert_eq!(problem.title, "Bad Request");
        assert_eq!(
            problem.instance.as_deref(),
            Some("/api/v1/events/stream")
        );
        assert!(problem.invalid_params.is_some());
    }
}

#[tokio::test]
async fn test_multi_subscriber_isolation_and_global_disconnect_aborts_order_scoped_task_under_50ms() {
    let (state, _publisher, _validator, broadcaster) = AppState::with_in_memory_live();
    let app = create_router_with_app_state(state);

    // Part 1: Two concurrent subscribers on the same scope ("ord-shared-sub")
    let req1 = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream?order_id=ord-shared-sub")
        .body(Body::empty())
        .unwrap();
    let req2 = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream?order_id=ord-shared-sub")
        .body(Body::empty())
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(resp2.status(), StatusCode::OK);

    let mut body1 = resp1.into_body();
    let mut body2 = resp2.into_body();

    // Both receive StreamConnected
    let init1 = parse_sse_domain_events(&next_sse_chunk(&mut body1).await.unwrap());
    let init2 = parse_sse_domain_events(&next_sse_chunk(&mut body2).await.unwrap());
    assert_eq!(init1[0].event_type, "StreamConnected");
    assert_eq!(init2[0].event_type, "StreamConnected");
    assert_eq!(broadcaster.active_subscriber_count(), 2);

    // Drop the first subscriber; the second subscriber on the same scope must stay alive
    drop(body1);
    assert_eq!(broadcaster.active_subscriber_count(), 1);

    broadcaster.publish_agent_token("ord-shared-sub", "still-alive-token", 1);
    let sub2_events = parse_sse_domain_events(&next_sse_chunk(&mut body2).await.unwrap());
    assert_eq!(sub2_events.len(), 1);
    assert_eq!(sub2_events[0].event_type, "AgentToken");
    assert_eq!(sub2_events[0].payload["token"], "still-alive-token");

    // Drop the second subscriber so 0 subscribers remain before Part 2
    drop(body2);
    assert_eq!(broadcaster.active_subscriber_count(), 0);

    // Part 2: Connect an unscoped global subscriber and spawn an order-scoped agent task
    let global_req = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream")
        .body(Body::empty())
        .unwrap();
    let global_resp = app.oneshot(global_req).await.unwrap();
    assert_eq!(global_resp.status(), StatusCode::OK);
    let mut global_body = global_resp.into_body();

    let agent_handle = broadcaster
        .spawn_continuous_agent_stream("ord-scoped-under-global", Duration::from_millis(5));

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let chunk = next_sse_chunk(&mut global_body).await.unwrap();
        seen.extend(parse_sse_domain_events(&chunk));
    }
    assert_eq!(seen[0].event_type, "StreamConnected");
    assert_eq!(seen[1].aggregate_id, "ord-scoped-under-global");
    assert_eq!(seen[1].event_type, "AgentThought");
    assert_eq!(seen[1].payload["thought"], "token-1");

    // Dropping the last unscoped global subscriber must cancel and terminate the order-scoped agent task in <50ms
    let abort_start = Instant::now();
    drop(global_body);
    agent_handle.wait_for_termination().await;
    let abort_latency = abort_start.elapsed();

    assert!(
        abort_latency < Duration::from_millis(50),
        "Global subscriber disconnect abort latency {:?} exceeded <50ms SLA",
        abort_latency
    );
    assert!(agent_handle.is_cancelled());
    assert!(agent_handle.is_terminated());
    assert_eq!(broadcaster.active_task_count(), 0);
    assert_eq!(broadcaster.active_subscriber_count(), 0);
}

#[tokio::test]
async fn test_client_disconnect_preempts_and_aborts_agent_task_under_50ms() {
    let (state, _publisher, _validator, broadcaster) = AppState::with_in_memory_live();
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream?order_id=ord-abort-disconnect")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();

    // Spawn a continuous agent task linked to the same order scope
    let agent_handle = broadcaster
        .spawn_continuous_agent_stream("ord-abort-disconnect", Duration::from_millis(5));
    assert!(!agent_handle.is_cancelled());
    assert!(!agent_handle.is_terminated());

    // Consume StreamConnected + at least one live agent frame to confirm streaming is active
    let mut seen_events = Vec::new();
    while seen_events.len() < 2 {
        let chunk = next_sse_chunk(&mut body)
            .await
            .expect("Expected active SSE frame before disconnect");
        seen_events.extend(parse_sse_domain_events(&chunk));
    }
    assert_eq!(seen_events[0].event_type, "StreamConnected");
    assert!(
        seen_events[1].event_type == "AgentThought" || seen_events[1].event_type == "AgentToken"
    );

    // Client disconnects by dropping the SSE HTTP response body stream
    let abort_start = Instant::now();
    drop(body);

    // Wait for the spawned agent task to exit via its CancellationToken
    agent_handle.wait_for_termination().await;
    let abort_latency = abort_start.elapsed();

    assert!(
        agent_handle.is_cancelled(),
        "CancellationToken must be cancelled immediately when SSE body is dropped"
    );
    assert!(
        agent_handle.is_terminated(),
        "Agent task must be terminated after client disconnect"
    );
    assert!(
        abort_latency < Duration::from_millis(50),
        "Client disconnect preempt-and-abort latency {:?} exceeded <50ms SLA",
        abort_latency
    );

    // Verify zero wasted compute: emitted_count must remain frozen after termination
    let count_at_abort = agent_handle.emitted_count();
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(
        agent_handle.emitted_count(),
        count_at_abort,
        "Agent task must not emit any tokens after CancellationToken abort"
    );
    assert_eq!(broadcaster.active_task_count(), 0);
    assert_eq!(broadcaster.active_subscriber_count(), 0);
}

#[tokio::test]
async fn test_explicit_cancellation_token_abort_terminates_task_and_stream_under_50ms() {
    let (state, _publisher, _validator, broadcaster) = AppState::with_in_memory_live();
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream?order_id=ord-explicit-cancel")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();

    // Spawn finite multi-step agent token stream with 100 steps
    let chunks: Vec<(&str, String)> = (1..=100)
        .map(|i| ("AgentToken", format!("chunk-{i}")))
        .collect();
    let agent_handle = broadcaster.spawn_agent_token_stream(
        "ord-explicit-cancel",
        chunks,
        Duration::from_millis(5),
    );

    // Read initial frames so both stream and agent task are actively running
    let mut received = Vec::new();
    while received.len() < 2 {
        let chunk = next_sse_chunk(&mut body).await.unwrap();
        received.extend(parse_sse_domain_events(&chunk));
    }

    // Explicitly trigger CancellationToken abort via cancel_scope
    let abort_start = Instant::now();
    let cancelled = broadcaster.cancel_scope("ord-explicit-cancel");
    assert!(cancelled, "Expected active scope token to be cancelled");

    agent_handle.wait_for_termination().await;
    let remaining_bytes = body.collect().await.unwrap().to_bytes();
    let abort_latency = abort_start.elapsed();

    assert!(
        abort_latency < Duration::from_millis(50),
        "Explicit CancellationToken abort latency {:?} exceeded <50ms SLA",
        abort_latency
    );
    assert!(agent_handle.is_cancelled());
    assert!(agent_handle.is_terminated());
    assert!(
        agent_handle.emitted_count() < 100,
        "Agent task must abort before finishing all 100 chunks (emitted {})",
        agent_handle.emitted_count()
    );
    let _ = remaining_bytes;
}

#[tokio::test]
async fn test_orders_command_handlers_broadcast_credit_issued_and_order_cancelled_over_sse() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    let app = create_router_with_app_state(state);

    // 1. Open SSE subscription filtered to `order_id`
    let sse_req = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/events/stream?order_id={order_id}"))
        .body(Body::empty())
        .unwrap();
    let sse_response = app.clone().oneshot(sse_req).await.unwrap();
    assert_eq!(sse_response.status(), StatusCode::OK);

    // 2. Dispatch IssueCourtesyCredit command for `order_id`
    let credit_req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": order_id,
                "amount_cents": 3200,
                "reason": "Expedited shipping delay refund"
            })
            .to_string(),
        ))
        .unwrap();
    let credit_res = app.clone().oneshot(credit_req).await.unwrap();
    assert_eq!(credit_res.status(), StatusCode::ACCEPTED);
    let credit_accepted: CommandAcceptedResponse = serde_json::from_slice(
        &credit_res.into_body().collect().await.unwrap().to_bytes(),
    )
    .unwrap();

    // 3. Dispatch CancelOrder command for `order_id`
    let cancel_req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": order_id,
                "reason": "Customer requested full cancellation"
            })
            .to_string(),
        ))
        .unwrap();
    let cancel_res = app.oneshot(cancel_req).await.unwrap();
    assert_eq!(cancel_res.status(), StatusCode::ACCEPTED);
    let cancel_accepted: CommandAcceptedResponse = serde_json::from_slice(
        &cancel_res.into_body().collect().await.unwrap().to_bytes(),
    )
    .unwrap();

    // Verify both commands were committed to Iggy WAL
    assert_eq!(publisher.messages().len(), 2);

    // 4. Collect SSE stream and verify StreamConnected, CreditIssued, and OrderCancelled frames
    let sse_bytes = sse_response.into_body().collect().await.unwrap().to_bytes();
    let sse_text = String::from_utf8_lossy(&sse_bytes);
    let events = parse_sse_domain_events(&sse_text);

    assert_eq!(events.len(), 3, "Expected 3 SSE events, got: {events:?}");
    assert_eq!(events[0].event_type, "StreamConnected");
    assert_eq!(events[0].aggregate_id, order_id);

    assert_eq!(events[1].event_type, "CreditIssued");
    assert_eq!(events[1].event_id, credit_accepted.command_id);
    assert_eq!(events[1].aggregate_id, order_id);
    assert_eq!(events[1].payload["amount_cents"], 3200);
    assert_eq!(
        events[1].payload["reason"],
        "Expedited shipping delay refund"
    );

    assert_eq!(events[2].event_type, "OrderCancelled");
    assert_eq!(events[2].event_id, cancel_accepted.command_id);
    assert_eq!(events[2].aggregate_id, order_id);
    assert_eq!(
        events[2].payload["reason"],
        "Customer requested full cancellation"
    );
}

#[tokio::test]
async fn test_lagged_receiver_recovers_gracefully_without_panicking() {
    let broadcaster = Arc::new(EventStreamBroadcaster::with_capacity(16, false));
    let publisher = Arc::new(InMemoryIggyPublisher::new());
    let validator = Arc::new(JwtValidator::default());
    let state = AppState::with_event_broadcaster(
        publisher as Arc<dyn IggyPublisher>,
        validator,
        Arc::clone(&broadcaster),
    );
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/events/stream?order_id=ord-lag")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Overflow the 16-slot broadcast ring buffer before polling the response stream
    for step in 1..=40 {
        broadcaster.publish_agent_token("ord-lag", format!("tok-{step}"), step);
    }

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    let events = parse_sse_domain_events(&body_str);

    assert!(
        events.len() >= 2,
        "Lagged stream should still emit StreamConnected and latest buffered events"
    );
    assert_eq!(events[0].event_type, "StreamConnected");
    assert_eq!(
        events.last().unwrap().payload["token"],
        "tok-40",
        "Stream must recover from lag and deliver the most recent event"
    );
}
