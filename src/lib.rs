//! # ferrous-api
//!
//! Contract-First Axum API Service for FerrousCX Platform.
//! Single source of truth driven by Goa Design v3 DSL and OpenAPI 3.0 specification.

pub mod generated;
pub mod handlers;

pub use generated::*;
pub use handlers::*;

use std::sync::Arc;
use axum::Router;

/// Creates default Axum router configured with standard pure-Rust trait handlers.
pub fn create_default_router() -> Router {
    let orders = Arc::new(OrdersHandler::new());
    let queries = Arc::new(QueriesHandler::new());
    let streams = Arc::new(StreamsHandler::new());

    let state = ApiState::new(orders, queries, streams);
    build_api_router(state).layer(tower_http::trace::TraceLayer::new_for_http())
}

/// Creates Axum router configured with a custom application state.
pub fn create_router_with_state(state: ApiState) -> Router {
    build_api_router(state)
}
