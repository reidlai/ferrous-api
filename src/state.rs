//! Shared application state holding Apache Iggy publisher, Kanidm JWT validator,
//! embedded Apache DataFusion `SessionContext`, SSE `EventStreamBroadcaster`,
//! `BenchmarkTelemetryBroadcaster`, and `PipelineTelemetryHub`.

use std::sync::Arc;

use crate::auth::JwtValidator;
use crate::lakehouse::SessionContext;
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};
use crate::sse::EventStreamBroadcaster;
use crate::telemetry::{BenchmarkTelemetryBroadcaster, PipelineTelemetryHub};

/// Shared gateway application state containing the `IggyPublisher`, `JwtValidator`,
/// embedded in-process `Arc<SessionContext>`, SSE `Arc<EventStreamBroadcaster>`,
/// `Arc<BenchmarkTelemetryBroadcaster>`, and `Arc<PipelineTelemetryHub>`.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Apache Iggy WAL publisher for dispatching `OrderCommand` events to `orders/intake`.
    pub publisher: Arc<dyn IggyPublisher>,
    /// Kanidm PEP JWT validator backed by an in-memory JWKS cache.
    pub jwt_validator: Arc<JwtValidator>,
    /// Embedded in-process Apache DataFusion `SessionContext` for <3ms lakehouse SQL queries.
    pub session_context: Arc<SessionContext>,
    /// Native Axum SSE event broadcaster and Tokio `CancellationToken` hub.
    pub event_broadcaster: Arc<EventStreamBroadcaster>,
    /// Real-time SSE benchmark telemetry broadcaster (`GET /api/v1/benchmarks/{run_id}/stream`).
    pub benchmark_broadcaster: Arc<BenchmarkTelemetryBroadcaster>,
    /// Read-only multi-source pipeline telemetry hub (`GET /api/v1/telemetry/pipeline/stream`).
    pub pipeline_telemetry: Arc<PipelineTelemetryHub>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::with_session_context(
            Arc::new(InMemoryIggyPublisher::new()),
            Arc::new(JwtValidator::default()),
            Arc::new(SessionContext::default()),
        )
    }
}

impl AppState {
    /// Creates a new `AppState` from an `IggyPublisher` and `JwtValidator`, initializing
    /// a default in-process `SessionContext`, `EventStreamBroadcaster`, `BenchmarkTelemetryBroadcaster`,
    /// and `PipelineTelemetryHub`.
    pub fn new(publisher: Arc<dyn IggyPublisher>, jwt_validator: Arc<JwtValidator>) -> Self {
        Self {
            publisher,
            jwt_validator,
            session_context: Arc::new(SessionContext::default()),
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
        }
    }

    /// Creates a new `AppState` with an explicit `Arc<SessionContext>`.
    pub fn with_session_context(
        publisher: Arc<dyn IggyPublisher>,
        jwt_validator: Arc<JwtValidator>,
        session_context: Arc<SessionContext>,
    ) -> Self {
        Self {
            publisher,
            jwt_validator,
            session_context,
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
        }
    }

    /// Creates a new `AppState` with an explicit `Arc<EventStreamBroadcaster>`.
    pub fn with_event_broadcaster(
        publisher: Arc<dyn IggyPublisher>,
        jwt_validator: Arc<JwtValidator>,
        event_broadcaster: Arc<EventStreamBroadcaster>,
    ) -> Self {
        Self {
            publisher,
            jwt_validator,
            session_context: Arc::new(SessionContext::default()),
            event_broadcaster,
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
        }
    }

    /// Creates a test-ready `AppState` alongside direct handles to its `InMemoryIggyPublisher`
    /// and `JwtValidator`.
    pub fn with_in_memory() -> (Self, Arc<InMemoryIggyPublisher>, Arc<JwtValidator>) {
        let publisher = Arc::new(InMemoryIggyPublisher::new());
        let validator = Arc::new(JwtValidator::default());
        let state = Self::new(
            Arc::clone(&publisher) as Arc<dyn IggyPublisher>,
            Arc::clone(&validator),
        );
        (state, publisher, validator)
    }

    /// Creates a test-ready `AppState` with persistent live `EventStreamBroadcaster`,
    /// `BenchmarkTelemetryBroadcaster`, and `PipelineTelemetryHub`.
    pub fn with_in_memory_live() -> (
        Self,
        Arc<InMemoryIggyPublisher>,
        Arc<JwtValidator>,
        Arc<EventStreamBroadcaster>,
    ) {
        let publisher = Arc::new(InMemoryIggyPublisher::new());
        let validator = Arc::new(JwtValidator::default());
        let broadcaster = Arc::new(EventStreamBroadcaster::new_live());
        let benchmark_broadcaster = Arc::new(BenchmarkTelemetryBroadcaster::new_live());
        let pipeline_telemetry = Arc::new(PipelineTelemetryHub::new_live());
        let state = Self {
            publisher: Arc::clone(&publisher) as Arc<dyn IggyPublisher>,
            jwt_validator: Arc::clone(&validator),
            session_context: Arc::new(SessionContext::default()),
            event_broadcaster: Arc::clone(&broadcaster),
            benchmark_broadcaster,
            pipeline_telemetry,
        };
        (state, publisher, validator, broadcaster)
    }

    /// Creates a test-ready `AppState` with a custom `Arc<SessionContext>`.
    pub fn with_in_memory_and_session(
        session_context: Arc<SessionContext>,
    ) -> (Self, Arc<InMemoryIggyPublisher>, Arc<JwtValidator>) {
        let publisher = Arc::new(InMemoryIggyPublisher::new());
        let validator = Arc::new(JwtValidator::default());
        let state = Self::with_session_context(
            Arc::clone(&publisher) as Arc<dyn IggyPublisher>,
            Arc::clone(&validator),
            session_context,
        );
        (state, publisher, validator)
    }
}
