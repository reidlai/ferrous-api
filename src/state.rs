//! Shared application state holding Apache Iggy publisher, Kanidm JWT validator,
//! embedded Apache DataFusion `SessionContext`, SSE `EventStreamBroadcaster`,
//! `BenchmarkTelemetryBroadcaster`, and `PipelineTelemetryHub`.

use std::sync::Arc;

use ferrous_cqrses::handler::CommandHandler;

use crate::auth::JwtValidator;
use crate::lakehouse::SessionContext;
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};
use crate::sse::EventStreamBroadcaster;
use crate::telemetry::{BenchmarkTelemetryBroadcaster, PipelineTelemetryHub};

/// Shared gateway application state containing the `IggyPublisher`, `JwtValidator`,
/// embedded in-process `Arc<SessionContext>`, SSE `Arc<EventStreamBroadcaster>`,
/// `Arc<BenchmarkTelemetryBroadcaster>`, `Arc<PipelineTelemetryHub>`, and
/// synchronous CQRS/ES `Arc<Mutex<CommandHandler>>`.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Apache Iggy WAL publisher for dispatching `OrderCommand` events to `orders/intake`
    /// and committed `DomainEvent`s to `orders/events`.
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
    /// Synchronous command handler backed by SierraDB / in-memory event store.
    pub command_handler: Arc<tokio::sync::Mutex<CommandHandler>>,
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
    /// `PipelineTelemetryHub`, and in-memory `CommandHandler`.
    pub fn new(publisher: Arc<dyn IggyPublisher>, jwt_validator: Arc<JwtValidator>) -> Self {
        Self {
            publisher,
            jwt_validator,
            session_context: Arc::new(SessionContext::default()),
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
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
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
        }
    }

    /// Creates a new `AppState` with an explicit `Arc<Mutex<CommandHandler>>`.
    pub fn with_command_handler(
        publisher: Arc<dyn IggyPublisher>,
        jwt_validator: Arc<JwtValidator>,
        session_context: Arc<SessionContext>,
        command_handler: Arc<tokio::sync::Mutex<CommandHandler>>,
    ) -> Self {
        Self {
            publisher,
            jwt_validator,
            session_context,
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            benchmark_broadcaster: Arc::new(BenchmarkTelemetryBroadcaster::new()),
            pipeline_telemetry: Arc::new(PipelineTelemetryHub::new()),
            command_handler,
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
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
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
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
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

    /// Seeds an order aggregate in state cache and underlying store (useful for testing states).
    pub async fn seed_order(&self, order_id: &str) {
        let mut handler = self.command_handler.lock().await;
        let _ = handler
            .handle_command(ferrous_cqrses::commands::OrderCommand::CreateOrder(
                ferrous_cqrses::commands::CreateOrderCommand::new(
                    order_id,
                    "cust-default",
                    1500,
                    chrono::Utc::now(),
                ),
            ))
            .await;
    }
}
