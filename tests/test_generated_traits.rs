//! Integration tests validating that handlers satisfy generated server traits.

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    create_default_router,
    generated::models::{CommandAcceptedResponse, OrderDetails, OrderListResponse, ProblemDetails},
};
use http_body_util::BodyExt;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn test_issue_credit_valid_payload_returns_202() {
    let app = create_default_router();
    let order_id = Uuid::now_v7().to_string();

    let request_body = serde_json::json!({
        "order_id": order_id,
        "amount_cents": 2500,
        "reason": "Late arrival compensation"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(request_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: CommandAcceptedResponse = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body.status, "accepted");
    assert!(!body.command_id.is_nil());
    assert_eq!(
        body.command_id.get_version(),
        Some(uuid::Version::SortRand),
        "command_id must be a monotonically ordered UUIDv7"
    );
}

#[tokio::test]
async fn test_issue_credit_non_uuid_order_id_returns_422() {
    let app = create_default_router();

    let invalid_body = serde_json::json!({
        "order_id": "not-a-valid-uuid",
        "amount_cents": 1000,
        "reason": "Compensation"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(invalid_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 422);
    let params = problem.invalid_params.unwrap();
    assert!(params.iter().any(|p| p.name == "order_id"));
}

#[tokio::test]
async fn test_issue_credit_missing_required_field_returns_400_rfc7807() {
    let app = create_default_router();

    // Missing 'amount_cents'
    let malformed_body = serde_json::json!({
        "order_id": Uuid::now_v7().to_string(),
        "reason": "Missing amount"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(malformed_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(problem.status, 400);
    assert_eq!(problem.title, "Bad Request");
}

#[tokio::test]
async fn test_issue_credit_negative_or_zero_amount_returns_422_rfc7807() {
    let app = create_default_router();

    // Negative amount_cents violates validation rule
    let invalid_body = serde_json::json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": -500,
        "reason": "Negative amount test"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(invalid_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(problem.status, 422);
    assert_eq!(problem.title, "Unprocessable Entity");
    let invalid_params = problem.invalid_params.unwrap();
    assert!(invalid_params.iter().any(|p| p.name == "amount_cents"));
}

#[tokio::test]
async fn test_cancel_order_valid_payload_returns_202() {
    let app = create_default_router();
    let order_id = Uuid::now_v7().to_string();

    let request_body = serde_json::json!({
        "order_id": order_id,
        "reason": "Customer changed mind before dispatch"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(request_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: CommandAcceptedResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body.status, "accepted");
    assert_eq!(
        body.command_id.get_version(),
        Some(uuid::Version::SortRand),
        "command_id must be a monotonically ordered UUIDv7"
    );
}

#[tokio::test]
async fn test_cancel_order_blank_fields_returns_422() {
    let app = create_default_router();

    let empty_body = serde_json::json!({
        "order_id": "",
        "reason": ""
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(empty_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 422);
    assert_eq!(problem.title, "Unprocessable Entity");
    assert!(problem.invalid_params.is_some());
}

#[tokio::test]
async fn test_cancel_order_non_uuid_order_id_returns_422() {
    let app = create_default_router();

    let invalid_body = serde_json::json!({
        "order_id": "invalid-uuid-format",
        "reason": "Cancel order reason"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(invalid_body.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 422);
    let params = problem.invalid_params.unwrap();
    assert!(params.iter().any(|p| p.name == "order_id"));
}

#[tokio::test]
async fn test_get_order_existing_returns_200() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/ord-98234-x")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let order: OrderDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(order.order_id, "ord-98234-x");
}

#[tokio::test]
async fn test_get_order_not_found_returns_404_rfc7807() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/ord-not-found")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 404);
}

#[tokio::test]
async fn test_list_orders_returns_200() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders?limit=10&offset=0")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let list: OrderListResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(list.limit, 10);
    assert_eq!(list.offset, 0);
    assert!(!list.orders.is_empty());
}

#[tokio::test]
async fn test_list_orders_invalid_pagination_returns_400() {
    let app = create_default_router();

    // limit exceeds 1000
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders?limit=5000&offset=0")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 400);

    // negative offset
    let app2 = create_default_router();
    let req2 = Request::builder()
        .method("GET")
        .uri("/api/v1/orders?limit=50&offset=-10")
        .body(Body::empty())
        .unwrap();

    let response2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(response2.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_event_stream_returns_text_event_stream_and_payload() {
    let app = create_default_router();

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

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("event: domain_event"),
        "SSE stream must include event header: {body_str}"
    );
    assert!(
        body_str.contains("data: "),
        "SSE stream must include data header: {body_str}"
    );
    assert!(
        body_str.contains("StreamConnected"),
        "SSE data payload must contain event type: {body_str}"
    );
}

#[tokio::test]
async fn test_benchmark_stream_returns_text_event_stream_and_payload() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/bench-run-1/stream")
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

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("event: benchmark_telemetry"),
        "SSE stream must include event header: {body_str}"
    );
    assert!(
        body_str.contains("data: "),
        "SSE stream must include data header: {body_str}"
    );
    assert!(
        body_str.contains("bench-run-1"),
        "SSE data payload must contain run_id: {body_str}"
    );
}

#[tokio::test]
async fn test_benchmark_stream_blank_run_id_returns_400() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/benchmarks/%20/stream")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");
}

#[tokio::test]
async fn test_pipeline_telemetry_stream_returns_text_event_stream_and_payload() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/telemetry/pipeline/stream")
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

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("event: pipeline_telemetry"),
        "SSE stream must include event header: {body_str}"
    );
    assert!(
        body_str.contains("data: "),
        "SSE stream must include data header: {body_str}"
    );
    assert!(
        body_str.contains("delta_commit_count"),
        "SSE data payload must contain pipeline metrics: {body_str}"
    );
}

#[tokio::test]
async fn test_health_probes_return_200() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let app2 = create_default_router();
    let req2 = Request::builder()
        .method("GET")
        .uri("/health/ready")
        .body(Body::empty())
        .unwrap();
    let response2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(response2.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_fallback_returns_404_rfc7807() {
    let app = create_default_router();

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/nonexistent/endpoint")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/problem+json");

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 404);
}
