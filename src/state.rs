//! Shared application state holding Apache Iggy publisher and Kanidm JWT validator instances.

use std::sync::Arc;

use crate::auth::JwtValidator;
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};

/// Shared gateway application state containing the `IggyPublisher` and `JwtValidator`.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Apache Iggy WAL publisher for dispatching `OrderCommand` events to `orders/intake`.
    pub publisher: Arc<dyn IggyPublisher>,
    /// Kanidm PEP JWT validator backed by an in-memory JWKS cache.
    pub jwt_validator: Arc<JwtValidator>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new(
            Arc::new(InMemoryIggyPublisher::new()),
            Arc::new(JwtValidator::default()),
        )
    }
}

impl AppState {
    /// Creates a new `AppState` from an `IggyPublisher` and `JwtValidator`.
    pub fn new(publisher: Arc<dyn IggyPublisher>, jwt_validator: Arc<JwtValidator>) -> Self {
        Self {
            publisher,
            jwt_validator,
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
}
