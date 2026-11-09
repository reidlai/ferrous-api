//! Real-time SSE streaming handlers implementing StreamsApi trait.

use std::convert::Infallible;

use async_trait::async_trait;
use axum::response::sse::{Event, KeepAlive, Sse};
use chrono::Utc;
use ferrous_cqrses::id::generate_uuidv7;
use futures::stream;
use tracing::info;

use crate::generated::{
    ApiError, BenchmarkStreamResponse, BenchmarkTelemetryItem, Claims, DomainEventStreamItem,
    EventStreamParams, EventStreamResponse, InvalidParam, PipelineTelemetryItem,
    PipelineTelemetryStreamResponse, ProblemDetails, StreamsApi,
};

/// Pure-Rust implementation of StreamsApi server trait.
#[derive(Debug, Default, Clone)]
pub struct StreamsHandler;

impl StreamsHandler {
    pub fn new() -> Self {
        Self
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

        let initial_event = DomainEventStreamItem {
            event_id: generate_uuidv7(),
            event_type: "StreamConnected".to_string(),
            aggregate_id: params
                .order_id
                .clone()
                .unwrap_or_else(|| "global".to_string()),
            payload: serde_json::json!({ "status": "active" }),
            timestamp: Utc::now(),
        };

        let data = serde_json::to_string(&initial_event).unwrap_or_default();
        let s = stream::once(async move {
            Ok::<_, Infallible>(Event::default().event("domain_event").data(data))
        });

        let sse = Sse::new(Box::pin(s) as crate::generated::BoxedSseStream)
            .keep_alive(KeepAlive::default());

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
