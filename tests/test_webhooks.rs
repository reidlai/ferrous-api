//! Integration tests for Story 18.1: Webhook Ingress Gateway in ferrous-api.
//!
//! Verifies:
//! - Authenticated webhook ingress (`POST /api/v1/webhooks/orders/settlement-callback`)
//! - Tower PEP authorization (`write:orders` and `SCOPE_API_WRITE`)
//! - Invariant validation & direct CommandHandler execution against SierraDB
//! - Post-commit event publication to Apache Iggy (`orders/events`)
//! - Post-commit real-time SSE broadcasting
//! - RFC 7807 Problem Details error mappings (404 OrderNotFound, 400 InvalidAmount, 401 Unauthorized, 403 Forbidden, 422 Unprocessable Entity)

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    auth::{JwtValidator, SCOPE_API_READ, SCOPE_API_WRITE},
    create_router_with_app_state,
    generated::models::{Claims, ProblemDetails},
    handlers::webhooks::SettlementCallbackResponse,
    publisher::ORDERS_EVENTS_TOPIC,
    state::AppState,
};
use ferrous_cqrses::{
    commands::{IssueCourtesyCreditCommand, OrderCommand},
    domain::order::CreditSettlementStatus,
    events::DomainEvent,
};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

fn issue_write_bearer(validator: &JwtValidator) -> String {
    let claims = Claims::new("svc_enterprise_workflow").with_scope(SCOPE_API_WRITE);
    let token = validator.issue_token(&claims).unwrap();
    format!("Bearer {token}")
}

fn issue_custom_bearer(validator: &JwtValidator, scope: &str) -> String {
    let claims = Claims::new("svc_enterprise_workflow").with_scope(scope);
    let token = validator.issue_token(&claims).unwrap();
    format!("Bearer {token}")
}

#[tokio::test]
async fn test_successful_credit_settlement_callback() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // 1. Seed order and grant $15.00 courtesy credit (1500 cents)
    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1500, "Concession"),
            ))
            .await
            .unwrap();
    }

    // Subscribe to SSE broadcaster before dispatching webhook
    let mut sse_rx = state.event_broadcaster.subscribe();

    let app = create_router_with_app_state(state.clone());
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1500,
        "status": "settled",
        "transaction_ref": "tx-123"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let callback_resp: SettlementCallbackResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(callback_resp.status, "ok");
    assert_eq!(callback_resp.order_id, order_id);

    // 2. Verify SierraDB aggregate state transitions
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.courtesy_credit_cents, 1500);
        assert_eq!(agg.settled_credit_cents, 1500);
        assert_eq!(agg.settlement_status, CreditSettlementStatus::Settled);
        assert_eq!(agg.version, 3);
    }

    // 3. Verify publication to Apache Iggy orders/events
    let messages = publisher.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].topic, ORDERS_EVENTS_TOPIC);
    assert_eq!(messages[0].partition_key, order_id);

    let decoded = messages[0].decode_event().unwrap();
    match decoded {
        DomainEvent::CourtesyCreditSettled(ref e) => {
            assert_eq!(e.order_id, order_id);
            assert_eq!(e.amount_cents, 1500);
            assert_eq!(e.transaction_ref, "tx-123");
            assert_eq!(e.version, 3);
        }
        other => panic!("Expected DomainEvent::CourtesyCreditSettled, got {other:?}"),
    }

    // 4. Verify SSE broadcast notification
    let sse_item = sse_rx.try_recv().unwrap();
    assert_eq!(sse_item.event_type, "CourtesyCreditSettled");
    assert_eq!(sse_item.aggregate_id, order_id);
    assert_eq!(sse_item.payload["amount_cents"], 1500);
    assert_eq!(sse_item.payload["transaction_ref"], "tx-123");
}

#[tokio::test]
async fn test_settlement_failure_callback() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1500, "Concession"),
            ))
            .await
            .unwrap();
    }

    let mut sse_rx = state.event_broadcaster.subscribe();

    let app = create_router_with_app_state(state.clone());
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1500,
        "status": "failed",
        "reason": "Insufficient account balance"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify SierraDB aggregate state
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.courtesy_credit_cents, 1500);
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(
            agg.settlement_status,
            CreditSettlementStatus::SettlementFailed
        );
        assert_eq!(agg.version, 3);
    }

    // Verify Iggy event
    let messages = publisher.messages();
    assert_eq!(messages.len(), 1);
    let decoded = messages[0].decode_event().unwrap();
    match decoded {
        DomainEvent::CourtesyCreditSettlementFailed(ref e) => {
            assert_eq!(e.order_id, order_id);
            assert_eq!(e.amount_cents, 1500);
            assert_eq!(e.reason, "Insufficient account balance");
            assert_eq!(e.version, 3);
        }
        other => panic!("Expected DomainEvent::CourtesyCreditSettlementFailed, got {other:?}"),
    }

    // Verify SSE item
    let sse_item = sse_rx.try_recv().unwrap();
    assert_eq!(sse_item.event_type, "CourtesyCreditSettlementFailed");
    assert_eq!(sse_item.aggregate_id, order_id);
    assert_eq!(sse_item.payload["reason"], "Insufficient account balance");
}

#[tokio::test]
async fn test_excessive_settlement_amount_returns_400_invalid_amount() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    // Order with $10 credit (1000 cents)
    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1000, "Concession"),
            ))
            .await
            .unwrap();
    }

    let app = create_router_with_app_state(state.clone());
    // Attempting to settle 2500 cents (exceeds $10)
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 2500,
        "status": "settled",
        "transaction_ref": "tx-excess"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 400);
    assert_eq!(problem.code.as_deref(), Some("InvalidAmount"));
    assert_eq!(
        problem.instance.as_deref(),
        Some("/api/v1/webhooks/orders/settlement-callback")
    );

    // Verify SierraDB was untouched
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(agg.version, 2);
    }

    // Verify no events published to Iggy
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_nonexistent_order_returns_404_order_not_found() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let non_existent_order_id = Uuid::now_v7().to_string();

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": non_existent_order_id,
        "amount_cents": 1500,
        "status": "settled",
        "transaction_ref": "tx-nonexistent"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 404);
    assert_eq!(problem.code.as_deref(), Some("OrderNotFound"));
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_unauthenticated_request_rejected_with_401() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": 1500,
        "status": "settled"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_insufficient_scope_rejected_with_403() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let read_only_auth = issue_custom_bearer(&validator, SCOPE_API_READ);
    let app = create_router_with_app_state(state);

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": 1500,
        "status": "settled"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &read_only_auth)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_write_orders_scope_accepted() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let orders_write_auth = issue_custom_bearer(&validator, "write:orders");
    let order_id = Uuid::now_v7().to_string();

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1000, "Concession"),
            ))
            .await
            .unwrap();
    }

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1000,
        "status": "settled",
        "transaction_ref": "tx-scope-ok"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &orders_write_auth)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(publisher.messages().len(), 1);
}

#[tokio::test]
async fn test_malformed_webhook_payload_rejected() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);

    // 1. Blank order_id -> 422 Unprocessable Entity
    let app = create_router_with_app_state(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": "   ",
                "amount_cents": 1500,
                "status": "settled"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // 2. Negative amount -> 422 Unprocessable Entity
    let app = create_router_with_app_state(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": "ORD-123",
                "amount_cents": -500,
                "status": "settled"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // 3. Unrecognized status -> 400 Bad Request
    let app = create_router_with_app_state(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": "ORD-123",
                "amount_cents": 1500,
                "status": "pending_external_approval"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 4. Missing transaction_ref on settled status -> 422 Unprocessable Entity
    let app = create_router_with_app_state(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": "ORD-123",
                "amount_cents": 1500,
                "status": "settled"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // 5. Zero amount (boundary) -> 422 Unprocessable Entity
    let app = create_router_with_app_state(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(
            json!({
                "order_id": "ORD-123",
                "amount_cents": 0,
                "status": "settled",
                "transaction_ref": "tx-zero"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_idempotent_duplicate_settlement_callback_returns_200() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1500, "Concession"),
            ))
            .await
            .unwrap();
    }

    let payload = json!({
        "order_id": order_id,
        "amount_cents": 500,
        "status": "settled",
        "transaction_ref": "tx-idemp-1"
    });

    // 1. First settlement request
    let app = create_router_with_app_state(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let resp1 = app.oneshot(req).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(publisher.messages().len(), 1);

    // 2. Retried duplicate request with identical transaction_ref
    let app2 = create_router_with_app_state(state.clone());
    let req2 = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let resp2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // Verify no double-settlement occurred in SierraDB
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 500);
        assert_eq!(agg.version, 3);
    }
    // Verify no duplicate message published to Iggy
    assert_eq!(publisher.messages().len(), 1);
}

#[tokio::test]
async fn test_settlement_on_cancelled_order_returns_400_order_cancelled() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1000, "Concession"),
            ))
            .await
            .unwrap();
        handler
            .handle_command(OrderCommand::CancelOrder(
                ferrous_cqrses::commands::CancelOrderCommand::new(&order_id, "User requested cancel"),
            ))
            .await
            .unwrap();
    }

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 500,
        "status": "settled",
        "transaction_ref": "tx-cancel"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.code.as_deref(), Some("OrderCancelled"));
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_settlement_on_fraud_locked_order_returns_400_order_fraud_locked() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    {
        let mut handler = state.command_handler.lock().await;
        let mut agg = ferrous_cqrses::domain::order::OrderAggregate::new(&order_id, "CUST-F", 1000, chrono::Utc::now());
        agg.status = ferrous_cqrses::domain::order::OrderStatus::FraudLocked;
        agg.courtesy_credit_cents = 1000;
        agg.version = 2;
        handler.insert_aggregate(agg);
    }

    let app = create_router_with_app_state(state);
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 500,
        "status": "settled",
        "transaction_ref": "tx-fraud"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.code.as_deref(), Some("OrderFraudLocked"));
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_iggy_publish_failure_returns_500_and_rolls_back() {
    let publisher = std::sync::Arc::new(ferrous_api::publisher::InMemoryIggyPublisher::failing());
    let validator = std::sync::Arc::new(JwtValidator::default());
    let auth_header = issue_write_bearer(&validator);
    let order_id = Uuid::now_v7().to_string();

    let state = AppState::new(
        publisher.clone(),
        validator,
    );

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1000, "Concession"),
            ))
            .await
            .unwrap();
    }

    let app = create_router_with_app_state(state.clone());
    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1000,
        "status": "settled",
        "transaction_ref": "tx-rollback"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, &auth_header)
        .body(Body::from(payload.to_string()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Verify rollback in command handler
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(agg.version, 2);
    }
}
