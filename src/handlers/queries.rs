//! Queries handler implementing `QueriesApi` trait backed by [`OrderQueryEngine`]
//! (in-memory test backend or embedded Apache DataFusion over Delta Lake, AD-30/AD-31/AD-32).

use std::sync::Arc;

use async_trait::async_trait;
use ferrous_cqrses::queries::{GetOrderQuery, ListOrdersQuery};
use tracing::info;

use crate::generated::{
    ApiError, Claims, GetOrderResponse, ListOrdersParams, ListOrdersResponse, ProblemDetails,
    QueriesApi, ValidatePayload,
};
use crate::query_engine::{InMemoryOrderQueryEngine, OrderQueryEngine, SessionContext};
use crate::state::AppState;

/// Pure-Rust implementation of `QueriesApi` server trait backed by an embedded
/// [`OrderQueryEngine`] over materialized Delta Lake views (`orders_current_state`).
#[derive(Debug, Clone)]
pub struct QueriesHandler {
    query_engine: Arc<dyn OrderQueryEngine>,
    session_context: Arc<SessionContext>,
}

impl Default for QueriesHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl QueriesHandler {
    /// Creates a `QueriesHandler` initialized with a default seeded [`InMemoryOrderQueryEngine`].
    pub fn new() -> Self {
        let session_context = Arc::new(SessionContext::default());
        Self {
            query_engine: Arc::new(InMemoryOrderQueryEngine::new(Arc::clone(&session_context))),
            session_context,
        }
    }

    /// Creates a `QueriesHandler` wired to the shared [`AppState`]'s [`OrderQueryEngine`].
    pub fn with_state(state: AppState) -> Self {
        Self {
            query_engine: state.query_engine,
            session_context: state.session_context,
        }
    }

    /// Creates a `QueriesHandler` bound to an explicit `Arc<SessionContext>`.
    pub fn with_session_context(session_context: Arc<SessionContext>) -> Self {
        Self {
            query_engine: Arc::new(InMemoryOrderQueryEngine::new(Arc::clone(&session_context))),
            session_context,
        }
    }

    /// Creates a `QueriesHandler` bound to an explicit [`OrderQueryEngine`] implementation.
    pub fn with_query_engine(query_engine: Arc<dyn OrderQueryEngine>) -> Self {
        Self {
            query_engine,
            session_context: Arc::new(SessionContext::default()),
        }
    }

    /// Returns a reference to the underlying `Arc<SessionContext>`.
    pub fn session_context(&self) -> &Arc<SessionContext> {
        &self.session_context
    }

    /// Returns a reference to the underlying `Arc<dyn OrderQueryEngine>`.
    pub fn query_engine(&self) -> &Arc<dyn OrderQueryEngine> {
        &self.query_engine
    }
}

#[async_trait]
impl QueriesApi for QueriesHandler {
    async fn get_order(
        &self,
        claims: Option<Claims>,
        order_id: String,
    ) -> Result<GetOrderResponse, ApiError> {
        let query = GetOrderQuery::new(&order_id);
        let instance = Some(format!("/api/v1/orders/{}", query.order_id));

        if let Err(err) = query.validate() {
            return Err(ApiError::bad_request(ProblemDetails::bad_request(
                err.to_string(),
                None,
            )));
        }

        info!(
            order_id = %query.order_id,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Executing CQRS GetOrderQuery via OrderQueryEngine"
        );

        match self.query_engine.get_order(&query).await {
            Ok(Some(details)) => Ok(GetOrderResponse::Ok(details)),
            Ok(None) => {
                let problem = ProblemDetails::not_found(
                    format!(
                        "Order '{}' was not found in materialized current state view",
                        query.order_id
                    ),
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

        let query = ListOrdersQuery::new(
            params.customer_id,
            params.status,
            params.limit.and_then(|v| u32::try_from(v).ok()),
            params.offset.and_then(|v| u32::try_from(v).ok()),
        );

        info!(
            limit = query.limit,
            offset = query.offset,
            customer_id = ?query.customer_id,
            status = ?query.status,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Executing CQRS ListOrdersQuery via OrderQueryEngine"
        );

        let list_response = self
            .query_engine
            .list_orders(&query)
            .await
            .map_err(|err| err.to_api_error(Some("/api/v1/orders".to_string())))?;

        Ok(ListOrdersResponse::Ok(list_response))
    }
}
