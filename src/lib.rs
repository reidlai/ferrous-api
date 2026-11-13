//! # ferrous-api
//!
//! Contract-First Axum API Service for FerrousCX Platform.
//! Single source of truth driven by Goa Design v3 DSL and OpenAPI 3.0 specification.

pub mod auth;
pub mod generated;
pub mod handlers;
pub mod lakehouse;
pub mod publisher;
pub mod sse;
pub mod state;
pub mod telemetry;

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
pub use sse::{
    AgentTaskHandle, EventStreamBroadcaster, EventStreamFilter, SseAbortGuard,
    DEFAULT_IDLE_DRAIN_WINDOW, DEFAULT_KEEP_ALIVE_INTERVAL, DEFAULT_SSE_CHANNEL_CAPACITY,
    DOMAIN_EVENT_SSE_NAME, GLOBAL_STREAM_SCOPE,
};
pub use state::AppState;
pub use telemetry::{
    BenchmarkTelemetryBroadcaster, PipelineCollectorConfig, PipelineTelemetryHub,
    TelemetryTaskHandle, BENCHMARK_STREAM_INSTANCE, BENCHMARK_TELEMETRY_SSE_NAME,
    DEFAULT_ARROYO_API_URL, DEFAULT_IGGY_STATS_URL, DEFAULT_PIPELINE_POLL_INTERVAL,
    DEFAULT_SOURCE_HTTP_TIMEOUT, PIPELINE_STREAM_INSTANCE, PIPELINE_TELEMETRY_SSE_NAME,
    SOURCE_STATUS_AVAILABLE, SOURCE_STATUS_UNAVAILABLE,
};

use axum::Router;
use std::sync::Arc;

/// Creates default Axum router configured with standard pure-Rust trait handlers,
/// `AppState`, and Kanidm Tower PEP middleware on command routes.
pub fn create_default_router() -> Router {
    create_router_with_app_state(AppState::default())
}

/// Creates Axum router wired to a shared [`AppState`] with Kanidm Tower PEP middleware
/// mounted on `/api/v1/commands` routes, `Arc<SessionContext>` wired into `QueriesHandler`,
/// and `Arc<EventStreamBroadcaster>` wired into `StreamsHandler` and `OrdersHandler`.
pub fn create_router_with_app_state(app_state: AppState) -> Router {
    let orders = Arc::new(OrdersHandler::with_state(app_state.clone()));
    let queries = Arc::new(QueriesHandler::with_state(app_state.clone()));
    let streams = Arc::new(StreamsHandler::with_state(app_state.clone()));

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
