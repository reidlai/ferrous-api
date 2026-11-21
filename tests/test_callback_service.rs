//! Integration tests for Callback Service in ferrous-api.
//!
//! Verifies:
//! - Asynchronous callback processing via `CallbackService`
//! - Invariant validation & direct CommandHandler execution against SierraDB
//! - Idempotent acknowledgment of duplicate callbacks
//! - Post-commit event publication to Apache Iggy (`orders/events`)
//! - Post-commit real-time SSE broadcasting
//! - Payload validation (blank fields, negative/zero amounts, missing refs/reasons)
//! - Rollback on publisher failures
//! - Complete absence of HTTP webhook routes (`/api/v1/webhooks/*`) on Axum router

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use ferrous_api::{
    auth::JwtValidator,
    callback::{
        CallbackError, CallbackOutcome, CallbackService, SettlementCallbackMessage,
        SettlementStatus,
    },
    create_router_with_app_state,
    state::AppState,
};
use ferrous_cqrses::{
    commands::{IssueCourtesyCreditCommand, OrderCommand},
    domain::order::{CreditSettlementStatus, DomainError},
};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn test_successful_credit_settlement_callback() {
    let (state, publisher, _validator) = AppState::with_in_memory();
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

    // Subscribe to SSE broadcaster before processing callback
    let mut sse_rx = state.event_broadcaster.subscribe();

    let callback_service: CallbackService = state.clone().into();
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-123".to_string()),
        reason: None,
    };

    let outcome = callback_service.process_callback(message).await.unwrap();
    match outcome {
        CallbackOutcome::Processed {
            command_id: _,
            order_id: ref oid,
            ref event_type,
        } => {
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettled");
        }
        other => panic!("Expected CallbackOutcome::Processed, got {other:?}"),
    }

    // 2. Verify SierraDB aggregate state transitions
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.courtesy_credit_cents, 1500);
        assert_eq!(agg.settled_credit_cents, 1500);
        assert_eq!(agg.settlement_status, CreditSettlementStatus::Settled);
        assert_eq!(agg.version, 3);
    }

    // 3. Verify CallbackService does NOT re-publish to orders/consolidated (prevents Projection <-> Callback loop)
    assert!(publisher.is_empty());

    // 4. Verify SSE broadcast notification
    let sse_item = sse_rx.try_recv().unwrap();
    assert_eq!(sse_item.event_type, "CourtesyCreditSettled");
    assert_eq!(sse_item.aggregate_id, order_id);
    assert_eq!(sse_item.payload["amount_cents"], 1500);
    assert_eq!(sse_item.payload["transaction_ref"], "tx-123");
}

#[tokio::test]
async fn test_settlement_failure_callback() {
    let (state, publisher, _validator) = AppState::with_in_memory();
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

    let callback_service: CallbackService = state.clone().into();
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1500,
        status: SettlementStatus::Failed,
        transaction_ref: None,
        reason: Some("Insufficient account balance".to_string()),
    };

    let outcome = callback_service.process_callback(message).await.unwrap();
    match outcome {
        CallbackOutcome::Processed {
            command_id: _,
            order_id: ref oid,
            ref event_type,
        } => {
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettlementFailed");
        }
        other => panic!("Expected CallbackOutcome::Processed, got {other:?}"),
    }

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

    // Verify no loop-back publish to Iggy
    assert!(publisher.is_empty());

    // Verify SSE item
    let sse_item = sse_rx.try_recv().unwrap();
    assert_eq!(sse_item.event_type, "CourtesyCreditSettlementFailed");
    assert_eq!(sse_item.aggregate_id, order_id);
    assert_eq!(sse_item.payload["reason"], "Insufficient account balance");
}

#[tokio::test]
async fn test_excessive_settlement_amount_rejected() {
    let (state, publisher, _validator) = AppState::with_in_memory();
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

    let callback_service: CallbackService = state.clone().into();
    // Attempting to settle 2500 cents (exceeds $10)
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 2500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-excess".to_string()),
        reason: None,
    };

    let result = callback_service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::InvalidAmount(amount))) => {
            assert_eq!(amount, 2500);
        }
        other => panic!("Expected CallbackError::Domain(InvalidAmount), got {other:?}"),
    }

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
async fn test_nonexistent_order_rejected() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let non_existent_order_id = Uuid::now_v7().to_string();

    let callback_service: CallbackService = state.clone().into();
    let message = SettlementCallbackMessage {
        order_id: non_existent_order_id.clone(),
        amount_cents: 1500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-nonexistent".to_string()),
        reason: None,
    };

    let result = callback_service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::OrderNotFound(oid))) => {
            assert_eq!(oid, non_existent_order_id);
        }
        other => panic!("Expected CallbackError::Domain(OrderNotFound), got {other:?}"),
    }

    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_malformed_callback_payload_rejected() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let callback_service: CallbackService = state.clone().into();

    // 1. Blank order_id
    let msg1 = SettlementCallbackMessage {
        order_id: "   ".to_string(),
        amount_cents: 1500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-1".to_string()),
        reason: None,
    };
    match callback_service.process_callback(msg1).await {
        Err(CallbackError::InvalidPayload(msg)) => {
            assert!(msg.contains("order_id"));
        }
        other => panic!("Expected InvalidPayload for blank order_id, got {other:?}"),
    }

    // 2. Negative amount
    let msg2 = SettlementCallbackMessage {
        order_id: "ORD-123".to_string(),
        amount_cents: -500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-1".to_string()),
        reason: None,
    };
    match callback_service.process_callback(msg2).await {
        Err(CallbackError::InvalidPayload(msg)) => {
            assert!(msg.contains("amount_cents"));
        }
        other => panic!("Expected InvalidPayload for negative amount, got {other:?}"),
    }

    // 3. Zero amount
    let msg3 = SettlementCallbackMessage {
        order_id: "ORD-123".to_string(),
        amount_cents: 0,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-1".to_string()),
        reason: None,
    };
    match callback_service.process_callback(msg3).await {
        Err(CallbackError::InvalidPayload(msg)) => {
            assert!(msg.contains("amount_cents"));
        }
        other => panic!("Expected InvalidPayload for zero amount, got {other:?}"),
    }

    // 4. Missing transaction_ref on settled status
    let msg4 = SettlementCallbackMessage {
        order_id: "ORD-123".to_string(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: None,
        reason: None,
    };
    match callback_service.process_callback(msg4).await {
        Err(CallbackError::InvalidPayload(msg)) => {
            assert!(msg.contains("transaction_ref"));
        }
        other => panic!("Expected InvalidPayload for missing transaction_ref, got {other:?}"),
    }

    // 5. Missing reason on failed status
    let msg5 = SettlementCallbackMessage {
        order_id: "ORD-123".to_string(),
        amount_cents: 1000,
        status: SettlementStatus::Failed,
        transaction_ref: None,
        reason: None,
    };
    match callback_service.process_callback(msg5).await {
        Err(CallbackError::InvalidPayload(msg)) => {
            assert!(msg.contains("reason"));
        }
        other => panic!("Expected InvalidPayload for missing reason, got {other:?}"),
    }

    // 6. Bad JSON bytes via process_message
    match callback_service.process_message(b"invalid json bytes").await {
        Err(CallbackError::Deserialization(_)) => {}
        other => panic!("Expected Deserialization error for bad json, got {other:?}"),
    }

    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_idempotent_duplicate_settlement_callback_acknowledged() {
    let (state, publisher, _validator) = AppState::with_in_memory();
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

    let callback_service: CallbackService = state.clone().into();
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-idemp-1".to_string()),
        reason: None,
    };

    // 1. First settlement request
    let outcome1 = callback_service
        .process_callback(message.clone())
        .await
        .unwrap();
    match outcome1 {
        CallbackOutcome::Processed {
            order_id: ref oid,
            ref event_type,
            ..
        } => {
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettled");
        }
        other => panic!("Expected CallbackOutcome::Processed, got {other:?}"),
    }
    assert!(publisher.is_empty());

    // 2. Retried duplicate request with identical transaction_ref
    let outcome2 = callback_service.process_callback(message).await.unwrap();
    match outcome2 {
        CallbackOutcome::DuplicateIgnored {
            command_id: _,
            order_id: ref oid,
        } => {
            assert_eq!(oid, &order_id);
        }
        other => panic!("Expected CallbackOutcome::DuplicateIgnored, got {other:?}"),
    }

    // Verify no double-settlement occurred in SierraDB
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 500);
        assert_eq!(agg.version, 3);
    }
    // Verify no loop-back message published to Iggy
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_settlement_on_cancelled_order_rejected() {
    let (state, publisher, _validator) = AppState::with_in_memory();
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
                ferrous_cqrses::commands::CancelOrderCommand::new(
                    &order_id,
                    "User requested cancel",
                ),
            ))
            .await
            .unwrap();
    }

    let callback_service: CallbackService = state.clone().into();
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-cancel".to_string()),
        reason: None,
    };

    let result = callback_service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::OrderCancelled)) => {}
        other => panic!("Expected CallbackError::Domain(OrderCancelled), got {other:?}"),
    }

    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_settlement_on_fraud_locked_order_rejected() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let order_id = Uuid::now_v7().to_string();

    {
        let mut handler = state.command_handler.lock().await;
        let mut agg = ferrous_cqrses::domain::order::OrderAggregate::new(
            &order_id,
            "CUST-F",
            1000,
            chrono::Utc::now(),
        );
        agg.status = ferrous_cqrses::domain::order::OrderStatus::FraudLocked;
        agg.courtesy_credit_cents = 1000;
        agg.version = 2;
        handler.insert_aggregate(agg);
    }

    let callback_service: CallbackService = state.clone().into();
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-fraud".to_string()),
        reason: None,
    };

    let result = callback_service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::OrderFraudLocked)) => {}
        other => panic!("Expected CallbackError::Domain(OrderFraudLocked), got {other:?}"),
    }

    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_iggy_publish_failure_rolls_back_sierradb() {
    let publisher = std::sync::Arc::new(ferrous_api::publisher::InMemoryIggyPublisher::failing());
    let validator = std::sync::Arc::new(JwtValidator::default());
    let order_id = Uuid::now_v7().to_string();

    let state = AppState::new(publisher.clone(), validator);

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

    let callback_service: CallbackService = CallbackService::from(state.clone()).with_publisher(
        std::sync::Arc::new(ferrous_api::state::IggyCallbackPublisherAdapter(
            publisher.clone(),
        )),
    );
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-rollback".to_string()),
        reason: None,
    };

    let result = callback_service.process_callback(message).await;
    match result {
        Err(CallbackError::Publisher(_)) => {}
        other => panic!("Expected CallbackError::Publisher, got {other:?}"),
    }

    // Verify rollback in command handler: aggregate returns to version 2 with settled_credit_cents: 0
    {
        let handler = state.command_handler.lock().await;
        let agg = handler.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(agg.version, 2);
    }
}

#[tokio::test]
async fn test_raw_json_message_bytes_processing() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let order_id = Uuid::now_v7().to_string();

    state.seed_order(&order_id).await;
    {
        let mut handler = state.command_handler.lock().await;
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1200, "Concession"),
            ))
            .await
            .unwrap();
    }

    let callback_service: CallbackService = state.clone().into();
    let raw_payload = json!({
        "order_id": order_id,
        "amount_cents": 1200,
        "status": "settled",
        "transaction_ref": "tx-raw-bytes"
    });

    let outcome = callback_service
        .process_message(raw_payload.to_string().as_bytes())
        .await
        .unwrap();

    match outcome {
        CallbackOutcome::Processed {
            command_id: _,
            order_id: ref oid,
            ref event_type,
        } => {
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettled");
        }
        other => panic!("Expected CallbackOutcome::Processed, got {other:?}"),
    }
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_http_router_does_not_expose_webhooks() {
    let (state, _publisher, _validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/orders/settlement-callback")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "order_id": "ORD-123",
                "amount_cents": 1000,
                "status": "settled",
                "transaction_ref": "tx-123"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // Since the webhook router is completely retired, Axum returns 404 Not Found
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
