//! Strongly typed Axum server traits and response types generated from OpenAPI 3.0 specification.
//!
//! Generated automatically from `gen/http/openapi3.json` (Goa Design v3 DSL).
//! DO NOT EDIT MANUALLY.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{FromRequest, Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Extension, Json, Router,
};
use futures::Stream;
use serde::de::DeserializeOwned;

use super::models::*;

/// Strongly typed API errors formatted according to RFC 7807 Problem Details.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Bad Request: {0:?}")]
    BadRequest(Box<ProblemDetails>),

    #[error("Unauthorized: {0:?}")]
    Unauthorized(Box<ProblemDetails>),

    #[error("Forbidden: {0:?}")]
    Forbidden(Box<ProblemDetails>),

    #[error("Not Found: {0:?}")]
    NotFound(Box<ProblemDetails>),

    #[error("Unprocessable Entity: {0:?}")]
    UnprocessableEntity(Box<ProblemDetails>),

    #[error("Internal Server Error: {0:?}")]
    InternalServerError(Box<ProblemDetails>),
}

impl ApiError {
    pub fn bad_request(p: ProblemDetails) -> Self {
        Self::BadRequest(Box::new(p))
    }

    pub fn unauthorized(p: ProblemDetails) -> Self {
        Self::Unauthorized(Box::new(p))
    }

    pub fn forbidden(p: ProblemDetails) -> Self {
        Self::Forbidden(Box::new(p))
    }

    pub fn not_found(p: ProblemDetails) -> Self {
        Self::NotFound(Box::new(p))
    }

    pub fn unprocessable_entity(p: ProblemDetails) -> Self {
        Self::UnprocessableEntity(Box::new(p))
    }

    pub fn internal_error(p: ProblemDetails) -> Self {
        Self::InternalServerError(Box::new(p))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::BadRequest(p) => p.into_response(),
            Self::Unauthorized(p) => p.into_response(),
            Self::Forbidden(p) => p.into_response(),
            Self::NotFound(p) => p.into_response(),
            Self::UnprocessableEntity(p) => p.into_response(),
            Self::InternalServerError(p) => p.into_response(),
        }
    }
}

impl IntoResponse for ProblemDetails {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = serde_json::to_string(&self).unwrap_or_else(|_| "{}".to_string());
        let mut res = Response::builder()
            .status(status)
            .body(Body::from(body))
            .unwrap_or_default();
        res.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        res
    }
}

/// Custom JSON extractor that verifies JSON syntax and semantic payload constraints,
/// returning RFC 7807 Problem Details on failure.
pub struct ValidatedJson<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned + ValidatePayload + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        const MAX_BODY_SIZE: usize = 2 * 1024 * 1024;
        let bytes = axum::body::to_bytes(req.into_body(), MAX_BODY_SIZE)
            .await
            .map_err(|e| {
                ApiError::bad_request(ProblemDetails::bad_request(
                    format!("Failed to read request body or exceeded 2MB limit: {e}"),
                    None,
                ))
            })?;

        let value: T = serde_json::from_slice(&bytes).map_err(|e| {
            ApiError::bad_request(ProblemDetails::bad_request(
                format!("Malformed request JSON: {e}"),
                None,
            ))
        })?;

        value
            .validate()
            .map_err(ApiError::UnprocessableEntity)?;

        Ok(ValidatedJson(value))
    }
}

/// Response wrapper for IssueCredit command.
#[derive(Debug, Clone)]
pub enum IssueCreditResponse {
    Accepted(CommandAcceptedResponse),
}

impl IntoResponse for IssueCreditResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Accepted(payload) => (StatusCode::ACCEPTED, Json(payload)).into_response(),
        }
    }
}

/// Response wrapper for CancelOrder command.
#[derive(Debug, Clone)]
pub enum CancelOrderResponse {
    Accepted(CommandAcceptedResponse),
}

impl IntoResponse for CancelOrderResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Accepted(payload) => (StatusCode::ACCEPTED, Json(payload)).into_response(),
        }
    }
}

/// Response wrapper for GetOrder query.
#[derive(Debug, Clone)]
pub enum GetOrderResponse {
    Ok(OrderDetails),
    NotFound(ProblemDetails),
}

impl IntoResponse for GetOrderResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
            Self::NotFound(problem) => problem.into_response(),
        }
    }
}

/// Response wrapper for ListOrders query.
#[derive(Debug, Clone)]
pub enum ListOrdersResponse {
    Ok(OrderListResponse),
}

impl IntoResponse for ListOrdersResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
        }
    }
}

/// Pinned boxed dynamic stream for SSE endpoints.
pub type BoxedSseStream = Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>>;

/// Response wrapper for domain event SSE stream.
pub struct EventStreamResponse(pub Sse<BoxedSseStream>);

impl IntoResponse for EventStreamResponse {
    fn into_response(self) -> Response {
        self.0.into_response()
    }
}

/// Response wrapper for benchmark telemetry SSE stream.
pub enum BenchmarkStreamResponse {
    Ok(Sse<BoxedSseStream>),
    NotFound(ProblemDetails),
}

impl From<Sse<BoxedSseStream>> for BenchmarkStreamResponse {
    fn from(sse: Sse<BoxedSseStream>) -> Self {
        Self::Ok(sse)
    }
}

impl IntoResponse for BenchmarkStreamResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Ok(sse) => sse.into_response(),
            Self::NotFound(problem) => problem.into_response(),
        }
    }
}

/// Response wrapper for pipeline telemetry SSE stream.
pub struct PipelineTelemetryStreamResponse(pub Sse<BoxedSseStream>);

impl IntoResponse for PipelineTelemetryStreamResponse {
    fn into_response(self) -> Response {
        self.0.into_response()
    }
}

/// Server trait for order mutation command endpoints.
#[async_trait]
pub trait OrdersApi: Send + Sync + 'static {
    /// Ingests courtesy credit concession and commits to Iggy WAL.
    async fn issue_credit(
        &self,
        claims: Option<Claims>,
        payload: IssueCreditRequest,
    ) -> Result<IssueCreditResponse, ApiError>;

    /// Ingests order cancellation and commits to Iggy WAL.
    async fn cancel_order(
        &self,
        claims: Option<Claims>,
        payload: CancelOrderRequest,
    ) -> Result<CancelOrderResponse, ApiError>;
}

/// Server trait for lakehouse analytical query endpoints.
#[async_trait]
pub trait QueriesApi: Send + Sync + 'static {
    /// Queries single order by ID from in-process Delta Lake tables.
    async fn get_order(
        &self,
        claims: Option<Claims>,
        order_id: String,
    ) -> Result<GetOrderResponse, ApiError>;

    /// Scans paginated orders from in-process Delta Lake tables.
    async fn list_orders(
        &self,
        claims: Option<Claims>,
        params: ListOrdersParams,
    ) -> Result<ListOrdersResponse, ApiError>;
}

/// Server trait for real-time SSE streaming endpoints.
#[async_trait]
pub trait StreamsApi: Send + Sync + 'static {
    /// Streams domain events and AI agent thoughts over native SSE.
    async fn event_stream(
        &self,
        claims: Option<Claims>,
        params: EventStreamParams,
    ) -> Result<EventStreamResponse, ApiError>;

    /// Streams real-time agent benchmark metrics (TTFT, ITL).
    async fn benchmark_stream(
        &self,
        claims: Option<Claims>,
        run_id: String,
    ) -> Result<BenchmarkStreamResponse, ApiError>;

    /// Streams pipeline health and lag telemetry.
    async fn pipeline_telemetry_stream(
        &self,
        claims: Option<Claims>,
    ) -> Result<PipelineTelemetryStreamResponse, ApiError>;
}

/// Shared application state containing trait implementations.
#[derive(Clone)]
pub struct ApiState {
    pub orders: Arc<dyn OrdersApi>,
    pub queries: Arc<dyn QueriesApi>,
    pub streams: Arc<dyn StreamsApi>,
}

impl ApiState {
    pub fn new(
        orders: Arc<dyn OrdersApi>,
        queries: Arc<dyn QueriesApi>,
        streams: Arc<dyn StreamsApi>,
    ) -> Self {
        Self {
            orders,
            queries,
            streams,
        }
    }
}

// Route handlers delegating to server traits

async fn handle_issue_credit(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
    ValidatedJson(payload): ValidatedJson<IssueCreditRequest>,
) -> Result<IssueCreditResponse, ApiError> {
    state
        .orders
        .issue_credit(claims.map(|c| c.0), payload)
        .await
}

async fn handle_cancel_order(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
    ValidatedJson(payload): ValidatedJson<CancelOrderRequest>,
) -> Result<CancelOrderResponse, ApiError> {
    state
        .orders
        .cancel_order(claims.map(|c| c.0), payload)
        .await
}

async fn handle_get_order(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
    Path(order_id): Path<String>,
) -> Result<GetOrderResponse, ApiError> {
    state.queries.get_order(claims.map(|c| c.0), order_id).await
}

async fn handle_list_orders(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
    Query(params): Query<ListOrdersParams>,
) -> Result<ListOrdersResponse, ApiError> {
    state.queries.list_orders(claims.map(|c| c.0), params).await
}

async fn handle_event_stream(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
    Query(params): Query<EventStreamParams>,
) -> Result<EventStreamResponse, ApiError> {
    state
        .streams
        .event_stream(claims.map(|c| c.0), params)
        .await
}

async fn handle_benchmark_stream(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
    Path(run_id): Path<String>,
) -> Result<BenchmarkStreamResponse, ApiError> {
    state
        .streams
        .benchmark_stream(claims.map(|c| c.0), run_id)
        .await
}

async fn handle_pipeline_telemetry_stream(
    State(state): State<ApiState>,
    claims: Option<Extension<Claims>>,
) -> Result<PipelineTelemetryStreamResponse, ApiError> {
    state
        .streams
        .pipeline_telemetry_stream(claims.map(|c| c.0))
        .await
}

/// Fallback handler returning RFC 7807 Problem Details for unregistered routes.
async fn handle_fallback(req: Request) -> Response {
    let uri = req.uri().to_string();
    ProblemDetails::not_found(format!("Resource '{uri}' not found"), Some(uri)).into_response()
}

/// Builds the Axum router with contract-first route bindings.
pub fn build_api_router(state: ApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/commands/orders/issue-credit",
            post(handle_issue_credit),
        )
        .route("/api/v1/commands/orders/cancel", post(handle_cancel_order))
        .route("/api/v1/orders/:order_id", get(handle_get_order))
        .route("/api/v1/orders", get(handle_list_orders))
        .route("/api/v1/events/stream", get(handle_event_stream))
        .route(
            "/api/v1/benchmarks/:run_id/stream",
            get(handle_benchmark_stream),
        )
        .route(
            "/api/v1/telemetry/pipeline/stream",
            get(handle_pipeline_telemetry_stream),
        )
        .route("/healthz", get(|| async { (StatusCode::OK, "healthy\n") }))
        .route("/health/ready", get(|| async { (StatusCode::OK, "ready\n") }))
        .fallback(handle_fallback)
        .with_state(state)
}
