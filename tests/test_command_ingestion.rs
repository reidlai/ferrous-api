//! Integration tests for `OrdersHandler` command ingestion, Apache Iggy `orders/intake` WAL
//! publication, strict `order_id` partitioning, monotonic UUIDv7 generation, and <5ms latency SLA.

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
        ORDERS_INTAKE_TOPIC,
    },
    state::AppState,
};
use ferrous_cqrses::commands::{IssueCourtesyCreditCommand, OrderCommand};
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
async fn test_issue_credit_publishes_order_command_to_iggy_intake_under_5ms() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // Warm up router path once
    {
        let warmup_app = create_router_with_app_state(state.clone());
        let warmup_req = Request::builder()
            .method("POST")
            .uri("/api/v1/commands/orders/issue-credit")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, &auth_header)
            .body(Body::from(
                json!({
                    "order_id": Uuid::now_v7().to_string(),
                    "amount_cents": 500,
                    "reason": "Warmup request"
                })
                .to_string(),
            ))
            .unwrap();
        let _ = warmup_app.oneshot(warmup_req).await.unwrap();
        publisher.clear();
    }

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1850,
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
        latency < Duration::from_millis(5),
        "Command ingestion latency {:?} exceeded <5ms SLA",
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

    let messages = publisher.messages();
    assert_eq!(messages.len(), 1);
    let published = &messages[0];
    assert_eq!(published.topic, ORDERS_INTAKE_TOPIC);
    assert_eq!(published.partition_key, order_id);
    assert_eq!(published.command_id, accepted.command_id);

    let decoded = published.decode_command().unwrap();
    match decoded {
        OrderCommand::IssueCourtesyCredit(cmd) => {
            assert_eq!(cmd.command_id, accepted.command_id);
            assert_eq!(cmd.order_id, order_id);
            assert_eq!(cmd.amount_cents, 1850);
            assert_eq!(cmd.reason, "Damaged outer packaging courtesy credit");
        }
        other => panic!("Expected OrderCommand::IssueCourtesyCredit, got {other:?}"),
    }
}

#[tokio::test]
async fn test_cancel_order_publishes_order_command_to_iggy_intake_under_5ms() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // Warm up router path once
    {
        let warmup_app = create_router_with_app_state(state.clone());
        let warmup_req = Request::builder()
            .method("POST")
            .uri("/api/v1/commands/orders/cancel")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, &auth_header)
            .body(Body::from(
                json!({
                    "order_id": Uuid::now_v7().to_string(),
                    "reason": "Warmup cancel request"
                })
                .to_string(),
            ))
            .unwrap();
        let _ = warmup_app.oneshot(warmup_req).await.unwrap();
        publisher.clear();
    }

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

    let start = Instant::now();
    let response = app.oneshot(req).await.unwrap();
    let latency = start.elapsed();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(
        latency < Duration::from_millis(5),
        "Cancel command ingestion latency {:?} exceeded <5ms SLA",
        latency
    );

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
    assert_eq!(published.topic, ORDERS_INTAKE_TOPIC);
    assert_eq!(published.partition_key, order_id);
    assert_eq!(published.command_id, accepted.command_id);

    let decoded = published.decode_command().unwrap();
    match decoded {
        OrderCommand::CancelOrder(cmd) => {
            assert_eq!(cmd.command_id, accepted.command_id);
            assert_eq!(cmd.order_id, order_id);
            assert_eq!(
                cmd.reason,
                "Customer requested cancellation prior to carrier pickup"
            );
        }
        other => panic!("Expected OrderCommand::CancelOrder, got {other:?}"),
    }
}

#[tokio::test]
async fn test_sequential_commands_for_same_order_id_share_partition_key_and_monotonic_uuidv7() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let shared_order_id = Uuid::now_v7().to_string();

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
    assert_eq!(messages[0].partition_key, messages[1].partition_key);
    assert_eq!(messages[0].topic, ORDERS_INTAKE_TOPIC);
    assert_eq!(messages[1].topic, ORDERS_INTAKE_TOPIC);
}

#[tokio::test]
async fn test_broker_publish_failure_returns_500_problem_details() {
    let publisher = Arc::new(InMemoryIggyPublisher::failing());
    let validator = Arc::new(JwtValidator::default());
    let state = AppState::new(
        Arc::clone(&publisher) as Arc<dyn IggyPublisher>,
        Arc::clone(&validator),
    );
    let app = create_router_with_app_state(state.clone());
    let auth_header = issue_write_bearer(&validator);

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
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

    // Also verify POST /api/v1/commands/orders/cancel returns 500 ProblemDetails on broker failure
    let app_cancel = create_router_with_app_state(state);
    let cancel_payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "reason": "Test cancel broker failure handling"
    });

    let cancel_req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/cancel")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(cancel_payload.to_string()))
        .unwrap();

    let cancel_response = app_cancel.oneshot(cancel_req).await.unwrap();
    assert_eq!(cancel_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        cancel_response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let cancel_bytes = cancel_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let cancel_problem: ProblemDetails = serde_json::from_slice(&cancel_bytes).unwrap();
    assert_eq!(cancel_problem.status, 500);
    assert_eq!(
        cancel_problem.r#type,
        "urn:ferrous:error:broker-unavailable"
    );
}

#[tokio::test]
async fn test_tcp_iggy_publisher_delivers_framed_command_over_loopback_socket() {
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
    let cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        &order_id,
        1250,
        "TCP loopback test",
    ));

    tcp_publisher.publish_command(&cmd).await.unwrap();

    let received = server_task.await.unwrap();
    assert_eq!(received.topic, ORDERS_INTAKE_TOPIC);
    assert_eq!(received.partition_key, order_id);
    assert_eq!(received.command_id, cmd.command_id());
    assert_eq!(received.decode_command().unwrap(), cmd);
}
