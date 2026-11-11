//! Orders command handler implementing `OrdersApi` trait and publishing `OrderCommand`
//! payloads to Apache Iggy WAL (`orders/intake`) partitioned strictly by `order_id`.

use std::sync::Arc;

use async_trait::async_trait;
use ferrous_cqrses::commands::{
    CancelOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
};
use tracing::info;

use crate::auth::{AuthError, SCOPE_API_WRITE};
use crate::generated::{
    ApiError, CancelOrderRequest, CancelOrderResponse, Claims, CommandAcceptedResponse,
    IssueCreditRequest, IssueCreditResponse, OrdersApi,
};
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};
use crate::state::AppState;

/// Pure-Rust implementation of `OrdersApi` server trait wired to `AppState` and `IggyPublisher`.
#[derive(Debug, Clone)]
pub struct OrdersHandler {
    publisher: Arc<dyn IggyPublisher>,
}

impl Default for OrdersHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl OrdersHandler {
    /// Creates a new `OrdersHandler` backed by a default `InMemoryIggyPublisher`.
    pub fn new() -> Self {
        Self {
            publisher: Arc::new(InMemoryIggyPublisher::new()),
        }
    }

    /// Creates a new `OrdersHandler` wired to the provided `AppState`.
    pub fn with_state(state: AppState) -> Self {
        Self {
            publisher: state.publisher,
        }
    }

    /// Creates a new `OrdersHandler` wired to a custom `IggyPublisher`.
    pub fn with_publisher(publisher: Arc<dyn IggyPublisher>) -> Self {
        Self { publisher }
    }

    /// Verifies that authenticated `Claims` with `api:write` scope are present before command dispatch.
    fn require_write_claims<'a>(
        claims: &'a Option<Claims>,
        instance: &str,
    ) -> Result<&'a Claims, ApiError> {
        let verified = claims.as_ref().ok_or_else(|| {
            ApiError::unauthorized(
                AuthError::MissingToken.into_problem_details(Some(instance.to_string())),
            )
        })?;

        if !verified.has_scope(SCOPE_API_WRITE) {
            return Err(ApiError::forbidden(
                AuthError::InsufficientScope {
                    required: SCOPE_API_WRITE.to_string(),
                }
                .into_problem_details(Some(instance.to_string())),
            ));
        }

        Ok(verified)
    }
}

#[async_trait]
impl OrdersApi for OrdersHandler {
    async fn issue_credit(
        &self,
        claims: Option<Claims>,
        payload: IssueCreditRequest,
    ) -> Result<IssueCreditResponse, ApiError> {
        const INSTANCE: &str = "/api/v1/commands/orders/issue-credit";
        let verified_claims = Self::require_write_claims(&claims, INSTANCE)?;

        let credit_cmd = IssueCourtesyCreditCommand::new(
            payload.order_id.trim(),
            payload.amount_cents,
            payload.reason.trim(),
        );
        let command_id = credit_cmd.command_id;
        let order_cmd = OrderCommand::IssueCourtesyCredit(credit_cmd);

        self.publisher
            .publish_command(&order_cmd)
            .await
            .map_err(|e| ApiError::internal_error(e.into_problem_details(Some(INSTANCE.to_string()))))?;

        info!(
            %command_id,
            order_id = %payload.order_id,
            amount_cents = payload.amount_cents,
            user = %verified_claims.sub,
            "Dispatched IssueCourtesyCredit command to Apache Iggy orders/intake"
        );

        Ok(IssueCreditResponse::Accepted(CommandAcceptedResponse::new(
            command_id,
        )))
    }

    async fn cancel_order(
        &self,
        claims: Option<Claims>,
        payload: CancelOrderRequest,
    ) -> Result<CancelOrderResponse, ApiError> {
        const INSTANCE: &str = "/api/v1/commands/orders/cancel";
        let verified_claims = Self::require_write_claims(&claims, INSTANCE)?;

        let cancel_cmd = CancelOrderCommand::new(payload.order_id.trim(), payload.reason.trim());
        let command_id = cancel_cmd.command_id;
        let order_cmd = OrderCommand::CancelOrder(cancel_cmd);

        self.publisher
            .publish_command(&order_cmd)
            .await
            .map_err(|e| ApiError::internal_error(e.into_problem_details(Some(INSTANCE.to_string()))))?;

        info!(
            %command_id,
            order_id = %payload.order_id,
            user = %verified_claims.sub,
            "Dispatched CancelOrder command to Apache Iggy orders/intake"
        );

        Ok(CancelOrderResponse::Accepted(CommandAcceptedResponse::new(
            command_id,
        )))
    }
}
