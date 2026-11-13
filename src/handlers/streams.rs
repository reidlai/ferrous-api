//! Real-time SSE streaming handlers implementing `StreamsApi` trait.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use crate::generated::{
    ApiError, BenchmarkStreamResponse, Claims, EventStreamParams, EventStreamResponse,
    PipelineTelemetryStreamResponse, StreamsApi,
};
use crate::sse::EventStreamBroadcaster;
use crate::state::AppState;
use crate::telemetry::{BenchmarkTelemetryBroadcaster, PipelineTelemetryHub};

/// Pure-Rust implementation of `StreamsApi` server trait wired to [`EventStreamBroadcaster`],
/// [`BenchmarkTelemetryBroadcaster`], and [`PipelineTelemetryHub`].
#[derive(Debug, Clone)]
pub struct StreamsHandler {
    event_broadcaster: Arc<EventStreamBroadcaster>,
    benchmark_broadcaster: Arc<BenchmarkTelemetryBroadcaster>,
    pipeline_telemetry: Arc<PipelineTelemetryHub>,
}

impl Default for StreamsHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamsHandler {
    /// Creates a new `StreamsHandler` backed by default broadcasters.
    pub fn new() -> Self {
        Self {
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
        }
    }

    /// Creates a `StreamsHandler` wired to the shared [`AppState`]'s SSE broadcasters.
    pub fn with_state(state: AppState) -> Self {
        Self {
            event_broadcaster: state.event_broadcaster,
            benchmark_broadcaster: state.benchmark_broadcaster,
            pipeline_telemetry: state.pipeline_telemetry,
        }
    }

    /// Creates a `StreamsHandler` wired to an explicit `Arc<EventStreamBroadcaster>`.
    pub fn with_broadcaster(event_broadcaster: Arc<EventStreamBroadcaster>) -> Self {
        Self {
            event_broadcaster,
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
        }
    }

    /// Returns a reference to the underlying `Arc<EventStreamBroadcaster>`.
    pub fn event_broadcaster(&self) -> &Arc<EventStreamBroadcaster> {
        &self.event_broadcaster
    }

    /// Returns a reference to the underlying `Arc<BenchmarkTelemetryBroadcaster>`.
    pub fn benchmark_broadcaster(&self) -> &Arc<BenchmarkTelemetryBroadcaster> {
        &self.benchmark_broadcaster
    }

    /// Returns a reference to the underlying `Arc<PipelineTelemetryHub>`.
    pub fn pipeline_telemetry(&self) -> &Arc<PipelineTelemetryHub> {
        &self.pipeline_telemetry
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
        info!(
            %run_id,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Client connected to benchmark telemetry stream"
        );

        self.benchmark_broadcaster.subscribe_sse(&run_id)
    }

    async fn pipeline_telemetry_stream(
        &self,
        claims: Option<Claims>,
    ) -> Result<PipelineTelemetryStreamResponse, ApiError> {
        info!(
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Client connected to pipeline telemetry stream"
        );

        let sse = self.pipeline_telemetry.subscribe_sse()?;
        Ok(PipelineTelemetryStreamResponse(sse))
    }
}
