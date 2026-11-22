//! Integration tests for `ferrous-api` stateless Streamable HTTP `/mcp` endpoint (Story 20.3, AD-8, AD-33).

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    auth::SCOPE_API_READ,
    create_router_with_app_state,
    generated::Claims,
    state::AppState,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn post_mcp(app: axum::Router, token: Option<&str>, payload: Value) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let req = builder
        .body(Body::from(payload.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn test_mcp_requires_kanidm_read_jwt_and_lists_only_ad8_safe_tools() {
    let (state, _publisher, validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    // 1. Unauthenticated call is blocked by Kanidm PEP middleware
    let (unauth_status, _) = post_mcp(
        app.clone(),
        None,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);

    // 2. Authenticated M2M svc_ferrous_agent token with api:read succeeds
    let claims = Claims::new("svc_ferrous_agent").with_scope(SCOPE_API_READ);
    let token = validator.issue_token(&claims).unwrap();
    let (status, body) = post_mcp(
        app.clone(),
        Some(&token),
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let names: Vec<&str> = body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert_eq!(names, vec!["get_order", "list_orders", "propose_courtesy_credit"]);

    // 3. Direct financial mutation tool is strictly rejected (AD-8)
    let (mut_status, mut_body) = post_mcp(
        app.clone(),
        Some(&token),
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": { "name": "issue_credit", "arguments": { "order_id": "ord-1", "amount_cents": 500 } }
        }),
    )
    .await;
    assert_eq!(mut_status, StatusCode::OK);
    assert_eq!(mut_body["error"]["code"], -32601);

    // 4. propose_courtesy_credit returns pending_human_approval HitlProposalCard without mutating state
    let (prop_status, prop_body) = post_mcp(
        app.clone(),
        Some(&token),
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "propose_courtesy_credit",
                "arguments": { "order_id": "ord-100", "amount_cents": 1500, "reason": "Carrier delay" }
            }
        }),
    )
    .await;
    assert_eq!(prop_status, StatusCode::OK);
    let text = prop_body["result"]["content"][0]["text"].as_str().unwrap();
    let card: Value = serde_json::from_str(text).unwrap();
    assert_eq!(card["type"], "HitlProposalCard");
    assert_eq!(card["mutated"], false);
    assert_eq!(card["amount_cents"], 1500);

    // 5. list_orders executes in-process against OrderQueryEngine
    let (list_status, list_body) = post_mcp(
        app,
        Some(&token),
        json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": { "name": "list_orders", "arguments": { "limit": 5 } }
        }),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert!(list_body["result"]["content"][0]["text"].as_str().is_some());
}
