//! Shared application state holding Apache Iggy publisher, Kanidm JWT validator,
//! and embedded Apache DataFusion `SessionContext` instances.

use std::sync::Arc;

use crate::auth::JwtValidator;
use crate::lakehouse::SessionContext;
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};

/// Shared gateway application state containing the `IggyPublisher`, `JwtValidator`,
/// and embedded in-process `Arc<SessionContext>`.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Apache Iggy WAL publisher for dispatching `OrderCommand` events to `orders/intake`.
    pub publisher: Arc<dyn IggyPublisher>,
    /// Kanidm PEP JWT validator backed by an in-memory JWKS cache.
    pub jwt_validator: Arc<JwtValidator>,
    /// Embedded in-process Apache DataFusion `SessionContext` for <3ms lakehouse SQL queries.
    pub session_context: Arc<SessionContext>,
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
    /// a default in-process `SessionContext` with `orders_current_state` registered.
    pub fn new(publisher: Arc<dyn IggyPublisher>, jwt_validator: Arc<JwtValidator>) -> Self {
        Self {
            publisher,
            jwt_validator,
            session_context: Arc::new(SessionContext::default()),
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
