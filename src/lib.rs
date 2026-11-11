//! # ferrous-api
//!
//! Contract-First Axum API Service for FerrousCX Platform.
//! Single source of truth driven by Goa Design v3 DSL and OpenAPI 3.0 specification.

pub mod auth;
pub mod generated;
pub mod handlers;
pub mod lakehouse;
pub mod publisher;
pub mod state;

pub use auth::{AuthError, JwtValidator, RequireScope, SCOPE_API_READ, SCOPE_API_WRITE};
pub use generated::*;
pub use handlers::*;
pub use lakehouse::{
    write_delta_table_snapshot, DeltaLogMetadata, DeltaTableProvider, FederatedOrderDisputeRow,
    LakehouseError, OperationalDisputeRecord, OperationalTableProvider, OrderRecord,
    SessionContext, SqlQueryResult, StatusAggregation, TableProvider, DEFAULT_DELTA_ORDERS_PATH,
    OPERATIONAL_DISPUTES_TABLE, ORDERS_CURRENT_STATE_TABLE,
};
pub use publisher::{
    IggyPublisher, InMemoryIggyPublisher, PublishedMessage, PublisherError, TcpIggyPublisher,
    ORDERS_INTAKE_TOPIC,
};
pub use state::AppState;

use std::sync::Arc;
use axum::Router;

/// Creates default Axum router configured with standard pure-Rust trait handlers,
/// `AppState`, and Kanidm Tower PEP middleware on command routes.
pub fn create_default_router() -> Router {
    create_router_with_app_state(AppState::default())
}

/// Creates Axum router wired to a shared [`AppState`] with Kanidm Tower PEP middleware
/// mounted on `/api/v1/commands` routes and `Arc<SessionContext>` wired into `QueriesHandler`.
pub fn create_router_with_app_state(app_state: AppState) -> Router {
    let orders = Arc::new(OrdersHandler::with_state(app_state.clone()));
    let queries = Arc::new(QueriesHandler::with_state(app_state.clone()));
    let streams = Arc::new(StreamsHandler::new());

    let api_state = ApiState::new(orders, queries, streams);
    build_api_router(api_state)
        .layer(RequireScope::for_path_prefix(
            app_state.jwt_validator,
            "/api/v1/commands",
            SCOPE_API_WRITE,
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

/// Creates Axum router configured with a custom [`ApiState`] and default Kanidm PEP middleware.
pub fn create_router_with_state(state: ApiState) -> Router {
    build_api_router(state).layer(RequireScope::for_path_prefix(
        Arc::new(JwtValidator::default()),
        "/api/v1/commands",
        SCOPE_API_WRITE,
    ))
}
