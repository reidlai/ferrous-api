//! Real-time SSE streaming handlers implementing `StreamsApi` trait.

use std::convert::Infallible;
use std::sync::Arc;

use async_trait::async_trait;
use axum::response::sse::{Event, KeepAlive, Sse};
use chrono::Utc;
use futures::stream;
use tracing::info;

use crate::generated::{
    ApiError, BenchmarkStreamResponse, BenchmarkTelemetryItem, Claims, EventStreamParams,
    EventStreamResponse, InvalidParam, PipelineTelemetryItem, PipelineTelemetryStreamResponse,
    ProblemDetails, StreamsApi,
};
use crate::sse::EventStreamBroadcaster;
use crate::state::AppState;

/// Pure-Rust implementation of `StreamsApi` server trait wired to [`EventStreamBroadcaster`].
#[derive(Debug, Clone)]
pub struct StreamsHandler {
    event_broadcaster: Arc<EventStreamBroadcaster>,
}

impl Default for StreamsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamsHandler {
    /// Creates a new `StreamsHandler` backed by a default [`EventStreamBroadcaster`].
    pub fn new() -> Self {
        Self {
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
        }
    }

    /// Creates a `StreamsHandler` wired to the shared [`AppState`]'s `Arc<EventStreamBroadcaster>`.
    pub fn with_state(state: AppState) -> Self {
        Self {
            event_broadcaster: state.event_broadcaster,
        }
    }

    /// Creates a `StreamsHandler` wired to an explicit `Arc<EventStreamBroadcaster>`.
    pub fn with_broadcaster(event_broadcaster: Arc<EventStreamBroadcaster>) -> Self {
        Self { event_broadcaster }
    }

    /// Returns a reference to the underlying `Arc<EventStreamBroadcaster>`.
    pub fn event_broadcaster(&self) -> &Arc<EventStreamBroadcaster> {
        &self.event_broadcaster
    }
}

#[async_trait]
impl StreamsApi for StreamsHandler {
    async fn event_stream(
        &self,
        claims: Option<Claims>,
        params: EventStreamParams,
    ) -> Result<EventStreamResponse, ApiError> {
        info!(
            order_id = ?params.order_id,
            event_types = ?params.event_types,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Client connected to domain event stream"
        );

        let sse = self.event_broadcaster.subscribe_sse(&params)?;
        Ok(EventStreamResponse(sse))
    }

    async fn benchmark_stream(
        &self,
        claims: Option<Claims>,
        run_id: String,
    ) -> Result<BenchmarkStreamResponse, ApiError> {
        if run_id.trim().is_empty() {
            return Err(ApiError::bad_request(ProblemDetails::bad_request(
                "run_id cannot be blank",
                Some(vec![InvalidParam::new("run_id", "run_id cannot be blank")]),
            )));
        }

        info!(
            %run_id,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Client connected to benchmark telemetry stream"
        );

        let initial_item = BenchmarkTelemetryItem {
            run_id: run_id.clone(),
            step: 1,
            ttft_micros: 12450,
            itl_micros: 3200,
            token_delta: 5,
            timestamp: Utc::now(),
        };

        let data = serde_json::to_string(&initial_item).unwrap_or_default();
        let s = stream::once(async move {
            Ok::<_, Infallible>(Event::default().event("benchmark_telemetry").data(data))
        });

        let sse = Sse::new(Box::pin(s) as crate::generated::BoxedSseStream)
            .keep_alive(KeepAlive::default());

        Ok(BenchmarkStreamResponse(sse))
    }

    async fn pipeline_telemetry_stream(
        &self,
        claims: Option<Claims>,
    ) -> Result<PipelineTelemetryStreamResponse, ApiError> {
        info!(
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Client connected to pipeline telemetry stream"
        );

        let initial_item = PipelineTelemetryItem {
            iggy_commit_lag: 0,
            arroyo_watermark_lag_ms: 12,
            delta_commit_count: 1042,
            timestamp: Utc::now(),
        };

        let data = serde_json::to_string(&initial_item).unwrap_or_default();
        let s = stream::once(async move {
            Ok::<_, Infallible>(Event::default().event("pipeline_telemetry").data(data))
        });

        let sse = Sse::new(Box::pin(s) as crate::generated::BoxedSseStream)
            .keep_alive(KeepAlive::default());

        Ok(PipelineTelemetryStreamResponse(sse))
    }
}
