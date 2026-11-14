//! Queries handler implementing `QueriesApi` trait backed by embedded in-process
//! Apache DataFusion (`Arc<SessionContext>`).

use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use crate::generated::{
    ApiError, Claims, GetOrderResponse, ListOrdersParams, ListOrdersResponse, ProblemDetails,
    QueriesApi, ValidatePayload,
};
use crate::lakehouse::SessionContext;
use crate::state::AppState;

/// Pure-Rust implementation of `QueriesApi` server trait backed by an embedded
/// `Arc<SessionContext>` registered with `orders_current_state`.
#[derive(Debug, Clone)]
pub struct QueriesHandler {
    session_context: Arc<SessionContext>,
}

impl Default for QueriesHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl QueriesHandler {
    /// Creates a `QueriesHandler` initialized with a default seeded `SessionContext`.
    pub fn new() -> Self {
        Self {
            session_context: Arc::new(SessionContext::default()),
        }
    }

    /// Creates a `QueriesHandler` wired to the shared [`AppState`]'s `Arc<SessionContext>`.
    pub fn with_state(state: AppState) -> Self {
        Self {
            session_context: state.session_context,
        }
    }

    /// Creates a `QueriesHandler` bound to an explicit `Arc<SessionContext>`.
    pub fn with_session_context(session_context: Arc<SessionContext>) -> Self {
        Self { session_context }
    }

    /// Returns a reference to the underlying `Arc<SessionContext>`.
    pub fn session_context(&self) -> &Arc<SessionContext> {
        &self.session_context
    }
}

#[async_trait]
impl QueriesApi for QueriesHandler {
    async fn get_order(
        &self,
        claims: Option<Claims>,
        order_id: String,
    ) -> Result<GetOrderResponse, ApiError> {
        info!(
            %order_id,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Executing vectorized SessionContext get_order query"
        );

        let instance = Some(format!("/api/v1/orders/{order_id}"));
        match self.session_context.query_order_by_id(&order_id) {
            Ok(Some(details)) => Ok(GetOrderResponse::Ok(details)),
            Ok(None) => {
                let problem = ProblemDetails::not_found(
                    format!("Order '{order_id}' was not found in lakehouse current state"),
                    instance,
                );
                Ok(GetOrderResponse::NotFound(problem))
            }
            Err(err) => Err(err.to_api_error(instance)),
        }
    }

    async fn list_orders(
        &self,
        claims: Option<Claims>,
        params: ListOrdersParams,
    ) -> Result<ListOrdersResponse, ApiError> {
        if let Err(problem) = params.validate() {
            return Err(ApiError::BadRequest(problem));
        }

        let limit = params.limit.unwrap_or(50);
        let offset = params.offset.unwrap_or(0);

        info!(
            limit,
            offset,
            customer_id = ?params.customer_id,
            status = ?params.status,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Executing vectorized SessionContext list_orders query"
        );

        let list_response = self
            .session_context
            .query_orders_list(&params)
            .map_err(|err| err.to_api_error(Some("/api/v1/orders".to_string())))?;

        Ok(ListOrdersResponse::Ok(list_response))
    }
}
