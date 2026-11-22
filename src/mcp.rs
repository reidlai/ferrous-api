//! Stateless Streamable HTTP MCP Server endpoint (`/mcp`) for `ferrous-api` (Story 20.3, AD-33).
//!
//! Exposes strictly AD-8-safe tools in stateless mode (horizontally scalable across multi-replica
//! Kubernetes `Deployment`s):
//! - `get_order` — read-only CQRS `GetOrderQuery` via `AppState::query_engine`
//! - `list_orders` — read-only CQRS `ListOrdersQuery` via `AppState::query_engine`
//! - `propose_courtesy_credit` — returns a structured `HitlProposalCard` JSON proposal ONLY
//!   (never mutates SierraDB or invokes `CommandHandler`, enforcing AD-8)

use axum::{extract::State, routing::post, Json, Router};
use ferrous_cqrses::queries::{GetOrderQuery, ListOrdersQuery};
use serde_json::{json, Value};
use tracing::info;

use crate::state::AppState;

/// MCP protocol version advertised by the stateless Streamable HTTP endpoint.
pub const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

/// Builds the stateless `/mcp` Axum router bound to `AppState`.
pub fn build_mcp_router(state: AppState) -> Router {
    Router::new()
        .route("/mcp", post(handle_mcp_rpc))
        .with_state(state)
}

async fn handle_mcp_rpc(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = body
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let params = body.get("params").cloned().unwrap_or_else(|| json!({}));

    match method {
        "initialize" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": { "listChanged": false }
                },
                "serverInfo": {
                    "name": "ferrous-api-mcp",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        })),
        "notifications/initialized" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {}
        })),
        "tools/list" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "tools": [
                    {
                        "name": "get_order",
                        "description": "Retrieve read-only materialized order details by order_id (CQRS GetOrderQuery)",
                        "inputSchema": {
                            "type": "object",
                            "required": ["order_id"],
                            "properties": {
                                "order_id": { "type": "string", "description": "Unique order identifier" }
                            }
                        }
                    },
                    {
                        "name": "list_orders",
                        "description": "List read-only materialized orders with optional customer_id/status filters (CQRS ListOrdersQuery)",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "customer_id": { "type": "string" },
                                "status": { "type": "string" },
                                "limit": { "type": "integer", "minimum": 1, "maximum": 500 },
                                "offset": { "type": "integer", "minimum": 0 }
                            }
                        }
                    },
                    {
                        "name": "propose_courtesy_credit",
                        "description": "Draft a Human-in-the-Loop (HITL) courtesy credit proposal card without mutating order state (AD-8)",
                        "inputSchema": {
                            "type": "object",
                            "required": ["order_id", "amount_cents", "reason"],
                            "properties": {
                                "order_id": { "type": "string" },
                                "amount_cents": { "type": "integer", "minimum": 1, "maximum": 2000 },
                                "reason": { "type": "string" }
                            }
                        }
                    }
                ]
            }
        })),
        "tools/call" => handle_tool_call(&state, id, params).await,
        _ => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": format!("Unknown MCP method '{method}'")
            }
        })),
    }
}

async fn handle_tool_call(state: &AppState, id: Value, params: Value) -> Json<Value> {
    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    info!(tool = %tool_name, "Executing stateless MCP tools/call on ferrous-api");

    match tool_name {
        "get_order" => {
            let order_id = args
                .get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let query = GetOrderQuery::new(order_id);
            match state.query_engine.get_order(&query).await {
                Ok(details) => mcp_text_result(id, serde_json::to_string(&details).unwrap_or_default()),
                Err(err) => mcp_error(id, -32602, err.to_string()),
            }
        }
        "list_orders" => {
            let customer_id = args
                .get("customer_id")
                .and_then(|v| v.as_str())
                .map(String::from);
            let status = args
                .get("status")
                .and_then(|v| v.as_str())
                .map(String::from);
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok());
            let offset = args
                .get("offset")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok());
            let query = ListOrdersQuery::new(customer_id, status, limit, offset);
            match state.query_engine.list_orders(&query).await {
                Ok(list) => mcp_text_result(id, serde_json::to_string(&list).unwrap_or_default()),
                Err(err) => mcp_error(id, -32602, err.to_string()),
            }
        }
        "propose_courtesy_credit" => {
            let order_id = args
                .get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .trim();
            let amount_cents = args
                .get("amount_cents")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let reason = args
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .trim();

            if order_id.is_empty() || !(1..=2000).contains(&amount_cents) || reason.is_empty() {
                return mcp_error(
                    id,
                    -32602,
                    "Invalid HITL proposal: order_id and reason must be non-empty and amount_cents must be 1..=2000".into(),
                );
            }

            // AD-8: Proposal card ONLY — zero SierraDB mutation, requires human approval click
            let proposal = json!({
                "type": "HitlProposalCard",
                "status": "pending_human_approval",
                "order_id": order_id,
                "amount_cents": amount_cents,
                "reason": reason,
                "mutated": false
            });
            mcp_text_result(id, proposal.to_string())
        }
        _ => mcp_error(
            id,
            -32601,
            format!("Tool '{tool_name}' is not exposed on /mcp (AD-8 read-only & HITL proposal allowlist)"),
        ),
    }
}

fn mcp_text_result(id: Value, text: String) -> Json<Value> {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [{ "type": "text", "text": text }]
        }
    }))
}

fn mcp_error(id: Value, code: i32, message: String) -> Json<Value> {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    }))
}
