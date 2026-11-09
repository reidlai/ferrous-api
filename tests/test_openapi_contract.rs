//! Integration tests validating OpenAPI 3.0 schema and JSON roundtrip.

use chrono::Utc;
use ferrous_api::generated::models::{
    BenchmarkTelemetryItem, CancelOrderRequest, CommandAcceptedResponse, DomainEventStreamItem,
    InvalidParam, IssueCreditRequest, OrderDetails, OrderItem, OrderListResponse, OrderSummary,
    PipelineTelemetryItem, ProblemDetails, ValidatePayload,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

fn get_openapi_spec_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("gen").join("http").join("openapi3.json")
}

#[test]
fn test_openapi_file_exists_and_is_valid_json() {
    let spec_path = get_openapi_spec_path();
    assert!(
        spec_path.exists(),
        "OpenAPI spec does not exist at {:?}",
        spec_path
    );

    let content = fs::read_to_string(&spec_path).expect("Failed to read openapi3.json");
    let json: Value = serde_json::from_str(&content).expect("openapi3.json is not valid JSON");

    assert!(json.is_object(), "Root of openapi3.json must be an object");
}

#[test]
fn test_openapi_metadata_and_version() {
    let spec_path = get_openapi_spec_path();
    let content = fs::read_to_string(&spec_path).expect("Failed to read openapi3.json");
    let json: Value = serde_json::from_str(&content).unwrap();

    let openapi_ver = json
        .get("openapi")
        .and_then(|v| v.as_str())
        .expect("Missing openapi version");
    assert!(
        openapi_ver.starts_with("3.0"),
        "Expected OpenAPI 3.0.x, found {}",
        openapi_ver
    );

    let title = json
        .get("info")
        .and_then(|i| i.get("title"))
        .and_then(|t| t.as_str())
        .expect("Missing info.title");
    assert_eq!(title, "FerrousCX Contract-First Axum API Service");

    let version = json
        .get("info")
        .and_then(|i| i.get("version"))
        .and_then(|v| v.as_str())
        .expect("Missing info.version");
    assert_eq!(version, "1.0.0");
}

#[test]
fn test_openapi_required_paths() {
    let spec_path = get_openapi_spec_path();
    let content = fs::read_to_string(&spec_path).expect("Failed to read openapi3.json");
    let json: Value = serde_json::from_str(&content).unwrap();

    let paths = json
        .get("paths")
        .and_then(|p| p.as_object())
        .expect("Missing paths object");

    let required_paths = [
        "/api/v1/commands/orders/issue-credit",
        "/api/v1/commands/orders/cancel",
        "/api/v1/orders/{order_id}",
        "/api/v1/orders",
        "/api/v1/events/stream",
        "/api/v1/benchmarks/{run_id}/stream",
        "/api/v1/telemetry/pipeline/stream",
    ];

    for path in required_paths {
        assert!(
            paths.contains_key(path),
            "Missing required API path: {}",
            path
        );
    }

    // Verify operations and status codes
    let issue_credit = &paths["/api/v1/commands/orders/issue-credit"]["post"];
    assert!(issue_credit.get("requestBody").is_some());
    assert!(issue_credit["responses"].get("202").is_some());
    assert!(issue_credit["responses"].get("400").is_some());
    assert!(issue_credit["responses"].get("422").is_some());

    let cancel_order = &paths["/api/v1/commands/orders/cancel"]["post"];
    assert!(cancel_order.get("requestBody").is_some());
    assert!(cancel_order["responses"].get("202").is_some());

    let get_order = &paths["/api/v1/orders/{order_id}"]["get"];
    assert!(get_order["responses"].get("200").is_some());
    assert!(get_order["responses"].get("404").is_some());

    let stream = &paths["/api/v1/events/stream"]["get"];
    assert!(stream["responses"]["200"]["content"]
        .get("text/event-stream")
        .is_some());
}

#[test]
fn test_openapi_components_schemas() {
    let spec_path = get_openapi_spec_path();
    let content = fs::read_to_string(&spec_path).expect("Failed to read openapi3.json");
    let json: Value = serde_json::from_str(&content).unwrap();

    let schemas = json
        .get("components")
        .and_then(|c| c.get("schemas"))
        .and_then(|s| s.as_object())
        .expect("Missing components.schemas object");

    let required_schemas = [
        "IssueCreditRequest",
        "CancelOrderRequest",
        "CommandAcceptedResponse",
        "ProblemDetails",
        "InvalidParam",
        "OrderItem",
        "OrderDetails",
        "OrderSummary",
        "OrderListResponse",
        "DomainEventStreamItem",
        "BenchmarkTelemetryItem",
        "PipelineTelemetryItem",
    ];

    for schema in required_schemas {
        assert!(
            schemas.contains_key(schema),
            "Missing component schema: {}",
            schema
        );
    }
}

#[test]
fn test_problem_details_rfc7807_roundtrip() {
    let problem = ProblemDetails {
        r#type: "urn:ferrouscx:error:validation".to_string(),
        title: "Validation Error".to_string(),
        status: 422,
        detail: Some("amount_cents is invalid".to_string()),
        instance: Some("/api/v1/commands/orders/issue-credit".to_string()),
        code: Some("VALIDATION_ERROR".to_string()),
        invalid_params: Some(vec![InvalidParam::new(
            "amount_cents",
            "must be greater than 0",
        )]),
    };

    let serialized = serde_json::to_string(&problem).unwrap();
    let deserialized: ProblemDetails = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized.r#type, "urn:ferrouscx:error:validation");
    assert_eq!(deserialized.status, 422);
    assert_eq!(deserialized.invalid_params.as_ref().unwrap().len(), 1);
    assert_eq!(
        deserialized.invalid_params.unwrap()[0].name,
        "amount_cents"
    );
}

#[test]
fn test_command_payload_validation() {
    let valid_uuid = Uuid::now_v7().to_string();

    // Valid IssueCreditRequest
    let valid_req = IssueCreditRequest {
        order_id: valid_uuid.clone(),
        amount_cents: 1500,
        reason: "Customer goodwill credit".to_string(),
    };
    assert!(valid_req.validate().is_ok());

    // Non-UUID order_id
    let invalid_uuid_req = IssueCreditRequest {
        order_id: "not-a-valid-uuid".to_string(),
        amount_cents: 1000,
        reason: "Valid reason".to_string(),
    };
    let uuid_err = invalid_uuid_req.validate().unwrap_err();
    assert_eq!(uuid_err.status, 422);
    assert!(uuid_err.invalid_params.unwrap().iter().any(|p| p.name == "order_id"));

    // Negative amount_cents
    let neg_req = IssueCreditRequest {
        order_id: valid_uuid.clone(),
        amount_cents: -50,
        reason: "Concession".to_string(),
    };
    let err = neg_req.validate().unwrap_err();
    assert_eq!(err.status, 422);
    let params = err.invalid_params.unwrap();
    assert!(params.iter().any(|p| p.name == "amount_cents"));

    // Zero amount_cents
    let zero_req = IssueCreditRequest {
        order_id: valid_uuid.clone(),
        amount_cents: 0,
        reason: "Concession".to_string(),
    };
    assert!(zero_req.validate().is_err());

    // Empty order_id
    let empty_order = IssueCreditRequest {
        order_id: "   ".to_string(),
        amount_cents: 100,
        reason: "Valid reason".to_string(),
    };
    assert!(empty_order.validate().is_err());

    // Valid CancelOrderRequest
    let cancel = CancelOrderRequest {
        order_id: valid_uuid.clone(),
        reason: "Customer cancellation request".to_string(),
    };
    assert!(cancel.validate().is_ok());

    // Empty reason CancelOrderRequest
    let invalid_cancel = CancelOrderRequest {
        order_id: valid_uuid,
        reason: "".to_string(),
    };
    assert!(invalid_cancel.validate().is_err());
}

#[test]
fn test_models_json_roundtrip() {
    // CommandAcceptedResponse
    let cmd_id = Uuid::now_v7();
    let resp = CommandAcceptedResponse::new(cmd_id);
    let serialized = serde_json::to_string(&resp).unwrap();
    let de: CommandAcceptedResponse = serde_json::from_str(&serialized).unwrap();
    assert_eq!(de.command_id, cmd_id);
    assert_eq!(de.status, "accepted");

    // OrderDetails
    let details = OrderDetails {
        order_id: "ord-100".to_string(),
        customer_id: "cust-200".to_string(),
        status: "shipped".to_string(),
        total_amount_cents: 9999,
        currency: "USD".to_string(),
        items: vec![OrderItem {
            item_id: "item-1".to_string(),
            product_id: "sku-abc".to_string(),
            price_cents: 9999,
            quantity: 1,
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let json_details = serde_json::to_string(&details).unwrap();
    let de_details: OrderDetails = serde_json::from_str(&json_details).unwrap();
    assert_eq!(de_details.order_id, "ord-100");
    assert_eq!(de_details.items.len(), 1);

    // OrderListResponse
    let list_resp = OrderListResponse {
        orders: vec![OrderSummary {
            order_id: "ord-100".to_string(),
            customer_id: "cust-200".to_string(),
            status: "shipped".to_string(),
            total_amount_cents: 9999,
            currency: "USD".to_string(),
            created_at: Utc::now(),
        }],
        total_count: 1,
        limit: 50,
        offset: 0,
    };
    let json_list = serde_json::to_string(&list_resp).unwrap();
    let de_list: OrderListResponse = serde_json::from_str(&json_list).unwrap();
    assert_eq!(de_list.total_count, 1);
    assert_eq!(de_list.orders[0].order_id, "ord-100");

    // DomainEventStreamItem
    let stream_item = DomainEventStreamItem {
        event_id: cmd_id,
        event_type: "CourtesyCreditIssued".to_string(),
        aggregate_id: "ord-100".to_string(),
        payload: serde_json::json!({"amount": 100}),
        timestamp: Utc::now(),
    };
    let json_item = serde_json::to_string(&stream_item).unwrap();
    let de_item: DomainEventStreamItem = serde_json::from_str(&json_item).unwrap();
    assert_eq!(de_item.event_type, "CourtesyCreditIssued");

    // BenchmarkTelemetryItem
    let bench = BenchmarkTelemetryItem {
        run_id: "bench-1".to_string(),
        step: 5,
        ttft_micros: 10000,
        itl_micros: 2500,
        token_delta: 2,
        timestamp: Utc::now(),
    };
    let json_bench = serde_json::to_string(&bench).unwrap();
    let de_bench: BenchmarkTelemetryItem = serde_json::from_str(&json_bench).unwrap();
    assert_eq!(de_bench.ttft_micros, 10000);

    // PipelineTelemetryItem
    let pipe = PipelineTelemetryItem {
        iggy_commit_lag: 0,
        arroyo_watermark_lag_ms: 15,
        delta_commit_count: 42,
        timestamp: Utc::now(),
    };
    let json_pipe = serde_json::to_string(&pipe).unwrap();
    let de_pipe: PipelineTelemetryItem = serde_json::from_str(&json_pipe).unwrap();
    assert_eq!(de_pipe.arroyo_watermark_lag_ms, 15);
}
