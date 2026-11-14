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
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default HTTP port for `ferrous-api` when `PORT` and `FERROUS_API_PORT` are unset.
pub const DEFAULT_PORT: u16 = 8080;

const MAX_HEALTHCHECK_STATUS_LINE_BYTES: u64 = 1024;

fn parse_valid_port(var_name: &str) -> Option<u16> {
    std::env::var(var_name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u16>().ok())
        .filter(|&port| port != 0)
}

/// Resolves the listening port from `PORT` or `FERROUS_API_PORT`, defaulting to `8080`.
pub fn resolve_port() -> u16 {
    parse_valid_port("PORT")
        .or_else(|| parse_valid_port("FERROUS_API_PORT"))
        .unwrap_or(DEFAULT_PORT)
}

/// Executes an embedded TCP/HTTP readiness probe against `GET /health/ready` on `addr`.
///
/// Returns `Ok(())` only when the endpoint responds with HTTP `200`. Used by the
/// `--healthcheck` CLI flag inside shell-less `FROM scratch` containers.
pub fn run_healthcheck_probe(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut stream = TcpStream::connect_timeout(&addr, timeout)
        .map_err(|err| format!("failed to connect to {addr}: {err}"))?;

    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| format!("healthcheck timed out connecting to {addr}"))?;

    stream
        .set_read_timeout(Some(remaining))
        .map_err(|err| format!("failed to set read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(remaining))
        .map_err(|err| format!("failed to set write timeout: {err}"))?;

    let request =
        format!("GET /health/ready HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("failed to write healthcheck request: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("failed to flush healthcheck request: {err}"))?;

    let remaining_read = deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| format!("healthcheck timed out before reading response from {addr}"))?;
    stream
        .set_read_timeout(Some(remaining_read))
        .map_err(|err| format!("failed to update read timeout: {err}"))?;

    let reader = BufReader::new(stream);
    let mut bounded_reader = std::io::Read::take(reader, MAX_HEALTHCHECK_STATUS_LINE_BYTES);
    let mut status_line = String::new();
    let bytes_read = bounded_reader
        .read_line(&mut status_line)
        .map_err(|err| format!("failed to read healthcheck response: {err}"))?;

    if bytes_read == 0 {
        return Err("healthcheck received empty response".to_string());
    }

    let trimmed = status_line.trim();
    let mut parts = trimmed.split_whitespace();
    let version = parts.next().unwrap_or_default();
    let status_code = parts.next().unwrap_or_default();

    if version.starts_with("HTTP/") && status_code == "200" {
        Ok(())
    } else {
        Err(format!("healthcheck returned non-200 status: {trimmed}"))
    }
}

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
