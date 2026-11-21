//! Integration tests for `OrdersHandler` Topology A synchronous command handling,
//! SierraDB validation, Apache Iggy `orders/events` publication, strict `order_id` partitioning,
//! and RFC 7807 problem details error mapping.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    auth::{JwtValidator, SCOPE_API_WRITE},
    create_router_with_app_state,
    generated::models::{Claims, CommandAcceptedResponse, ProblemDetails},
    publisher::{
        IggyPublisher, InMemoryIggyPublisher, PublishedMessage, TcpIggyPublisher,
        ORDERS_CONSOLIDATED_TOPIC,
    },
    state::AppState,
};
use ferrous_cqrses::commands::OrderCommand;
use ferrous_cqrses::events::DomainEvent;
use http_body_util::BodyExt;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tower::ServiceExt;
use uuid::Uuid;

fn issue_write_bearer(validator: &JwtValidator) -> String {
    let claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_WRITE);
    let token = validator.issue_token(&claims).unwrap();
    format!("Bearer {token}")
}

#[tokio::test]
async fn test_valid_courtesy_credit_commits_and_publishes_to_orders_events() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // 1. Seed existing order in state with $0 existing credit
    state.seed_order(&order_id).await;

    let app = create_router_with_app_state(state.clone());
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1500, // $15 <= $20 limit
        "reason": "Damaged outer packaging courtesy credit"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let start = Instant::now();
    let response = app.oneshot(req).await.unwrap();
    let latency = start.elapsed();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(
        latency < Duration::from_millis(50),
        "Command execution latency {:?} exceeded SLA",
        latency
    );

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let accepted: CommandAcceptedResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(accepted.status, "accepted");
    assert_eq!(
        accepted.command_id.get_version(),
        Some(uuid::Version::SortRand),
        "command_id must be a monotonic UUIDv7"
    );

    // Verify committed event was published to Apache Iggy orders/events
    let messages = publisher.messages();
    assert_eq!(messages.len(), 1);
    let published = &messages[0];
    assert_eq!(published.topic, ORDERS_CONSOLIDATED_TOPIC);
    assert_eq!(published.partition_key, order_id);

    let decoded = published.decode_event().unwrap();
    match decoded {
        DomainEvent::CreditIssued(ref e) => {
            assert_eq!(e.order_id, order_id);
            assert_eq!(e.amount_cents, 1500);
            assert_eq!(e.reason, "Damaged outer packaging courtesy credit");
        }
        other => panic!("Expected DomainEvent::CreditIssued, got {other:?}"),
    }
}

#[tokio::test]
async fn test_excessive_courtesy_credit_returns_400_courtesy_credit_exceeded() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // 1. Seed existing order with $0 existing credit
    state.seed_order(&order_id).await;

    let app = create_router_with_app_state(state);
    // Attempting $25 courtesy credit (> $20 ceiling)
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 2500,
        "reason": "Excessive credit concession"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 400);
    assert_eq!(problem.title, "CourtesyCreditExceeded");
    assert_eq!(problem.code.as_deref(), Some("CourtesyCreditExceeded"));

    // Invariant check prevents event publication to Iggy or SSE
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_credit_attempt_on_cancelled_order_returns_400_order_cancelled() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // 1. Seed existing order and cancel it
    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        let _ = handler
            .handle_command(OrderCommand::CancelOrder(
                ferrous_cqrses::commands::CancelOrderCommand::new(
                    &order_id,
                    "Pre-test cancellation",
                ),
            ))
            .await
            .unwrap();
    }

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1000,
        "reason": "Attempting credit on cancelled order"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 400);
    assert_eq!(problem.title, "OrderCancelled");
    assert_eq!(problem.code.as_deref(), Some("OrderCancelled"));

    // No event published to Iggy
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_credit_attempt_on_non_existent_order_returns_404_order_not_found() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let unknown_order_id = Uuid::now_v7().to_string();

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": unknown_order_id,
        "amount_cents": 1000,
        "reason": "Attempting credit on non-existent order"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 404);
    assert_eq!(problem.title, "OrderNotFound");
    assert_eq!(problem.code.as_deref(), Some("OrderNotFound"));

    // No event published
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_cancel_order_commits_and_publishes_to_orders_events() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // 1. Seed existing order
    state.seed_order(&order_id).await;

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "reason": "Customer requested cancellation prior to carrier pickup"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let accepted: CommandAcceptedResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(accepted.status, "accepted");
    assert_eq!(
        accepted.command_id.get_version(),
        Some(uuid::Version::SortRand)
    );

    let messages = publisher.messages();
    assert_eq!(messages.len(), 1);
    let published = &messages[0];
    assert_eq!(published.topic, ORDERS_CONSOLIDATED_TOPIC);
    assert_eq!(published.partition_key, order_id);

    let decoded = published.decode_event().unwrap();
    match decoded {
        DomainEvent::OrderCancelled(ref cmd) => {
            assert_eq!(cmd.order_id, order_id);
            assert_eq!(
                cmd.reason,
                "Customer requested cancellation prior to carrier pickup"
            );
        }
        other => panic!("Expected DomainEvent::OrderCancelled, got {other:?}"),
    }
}

#[tokio::test]
async fn test_cancel_non_existent_order_returns_404_order_not_found() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "reason": "Cancellation of unknown order"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 404);
    assert_eq!(problem.title, "OrderNotFound");
    assert_eq!(problem.code.as_deref(), Some("OrderNotFound"));
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_cancel_already_cancelled_order_returns_400_order_cancelled() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        let _ = handler
            .handle_command(OrderCommand::CancelOrder(
                ferrous_cqrses::commands::CancelOrderCommand::new(
                    &order_id,
                    "First cancellation",
                ),
            ))
            .await
            .unwrap();
    }

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "reason": "Attempting second cancellation"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 400);
    assert_eq!(problem.title, "OrderCancelled");
    assert_eq!(problem.code.as_deref(), Some("OrderCancelled"));
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_cancel_broker_publish_failure_returns_500_problem_details() {
    let publisher = Arc::new(InMemoryIggyPublisher::failing());
    let validator = Arc::new(JwtValidator::default());
    let order_id = Uuid::now_v7().to_string();
    let state = AppState::new(
        Arc::clone(&publisher) as Arc<dyn IggyPublisher>,
        Arc::clone(&validator),
    );
    state.seed_order(&order_id).await;

    let app = create_router_with_app_state(state.clone());
    let auth_header = issue_write_bearer(&validator);

    let payload = json!({
        "order_id": order_id,
        "reason": "Test cancel broker failure handling"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 500);
    assert_eq!(problem.r#type, "urn:ferrous:error:broker-unavailable");
}

#[tokio::test]
async fn test_sequential_commands_for_same_order_id_share_partition_key_and_monotonic_uuidv7() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let shared_order_id = Uuid::now_v7().to_string();

    state.seed_order(&shared_order_id).await;

    // Dispatch 1: IssueCourtesyCredit
    let app1 = create_router_with_app_state(state.clone());
    let req1 = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": shared_order_id,
                "amount_cents": 1000,
                "reason": "First concession"
            })
            .to_string(),
        ))
        .unwrap();
    let res1 = app1.oneshot(req1).await.unwrap();
    assert_eq!(res1.status(), StatusCode::ACCEPTED);
    let body1: CommandAcceptedResponse =
        serde_json::from_slice(&res1.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // Dispatch 2: CancelOrder for the exact same order_id
    let app2 = create_router_with_app_state(state);
    let req2 = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": shared_order_id,
                "reason": "Follow-up cancellation"
            })
            .to_string(),
        ))
        .unwrap();
    let res2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::ACCEPTED);
    let body2: CommandAcceptedResponse =
        serde_json::from_slice(&res2.into_body().collect().await.unwrap().to_bytes()).unwrap();

    assert!(
        body1.command_id < body2.command_id,
        "UUIDv7 command_ids must be monotonically increasing: {} vs {}",
        body1.command_id,
        body2.command_id
    );

    let messages = publisher.messages();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].partition_key, shared_order_id);
    assert_eq!(messages[1].partition_key, shared_order_id);
    assert_eq!(messages[0].topic, ORDERS_CONSOLIDATED_TOPIC);
    assert_eq!(messages[1].topic, ORDERS_CONSOLIDATED_TOPIC);
}

#[tokio::test]
async fn test_broker_publish_failure_returns_500_problem_details() {
    let publisher = Arc::new(InMemoryIggyPublisher::failing());
    let validator = Arc::new(JwtValidator::default());
    let order_id = Uuid::now_v7().to_string();
    let state = AppState::new(
        Arc::clone(&publisher) as Arc<dyn IggyPublisher>,
        Arc::clone(&validator),
    );
    state.seed_order(&order_id).await;

    let app = create_router_with_app_state(state.clone());
    let auth_header = issue_write_bearer(&validator);

    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1500,
        "reason": "Test broker failure handling"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 500);
    assert_eq!(problem.r#type, "urn:ferrous:error:broker-unavailable");
}

#[tokio::test]
async fn test_tcp_iggy_publisher_delivers_framed_event_over_loopback_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_le_bytes(len_buf) as usize;

        let mut payload_buf = vec![0u8; frame_len];
        socket.read_exact(&mut payload_buf).await.unwrap();
        let msg: PublishedMessage = serde_json::from_slice(&payload_buf).unwrap();
        msg
    });

    let tcp_publisher = TcpIggyPublisher::new(&addr);
    let order_id = Uuid::now_v7().to_string();
    let event = DomainEvent::CreditIssued(ferrous_cqrses::events::CreditIssuedEvent::new(
        &order_id,
        1250,
        "TCP loopback test",
        1,
    ));

    tcp_publisher.publish_event(&event).await.unwrap();

    let received = server_task.await.unwrap();
    assert_eq!(received.topic, ORDERS_CONSOLIDATED_TOPIC);
    assert_eq!(received.partition_key, order_id);
    assert_eq!(received.decode_event().unwrap(), event);
}
