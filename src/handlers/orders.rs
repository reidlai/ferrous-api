//! Orders command handler implementing `OrdersApi` trait, publishing `OrderCommand`
//! payloads to Apache Iggy WAL (`orders/intake`) partitioned strictly by `order_id`,
//! and broadcasting real-time `DomainEventStreamItem` notifications over SSE.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use ferrous_cqrses::commands::{
    CancelOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
};
use serde_json::json;
use tracing::info;

use crate::auth::{AuthError, SCOPE_API_WRITE};
use crate::generated::{
    ApiError, CancelOrderRequest, CancelOrderResponse, Claims, CommandAcceptedResponse,
    DomainEventStreamItem, IssueCreditRequest, IssueCreditResponse, OrdersApi,
};
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};
use crate::sse::EventStreamBroadcaster;
use crate::state::AppState;

/// Pure-Rust implementation of `OrdersApi` server trait wired to `AppState`, `IggyPublisher`,
/// and `EventStreamBroadcaster`.
#[derive(Debug, Clone)]
pub struct OrdersHandler {
    publisher: Arc<dyn IggyPublisher>,
    event_broadcaster: Arc<EventStreamBroadcaster>,
}

impl Default for OrdersHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl OrdersHandler {
    /// Creates a new `OrdersHandler` backed by a default `InMemoryIggyPublisher` and `EventStreamBroadcaster`.
    pub fn new() -> Self {
        Self {
            publisher: Arc::new(InMemoryIggyPublisher::new()),
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
        }
    }

    /// Creates a new `OrdersHandler` wired to the provided `AppState`.
    pub fn with_state(state: AppState) -> Self {
        Self {
            publisher: state.publisher,
            event_broadcaster: state.event_broadcaster,
        }
    }

    /// Creates a new `OrdersHandler` wired to a custom `IggyPublisher`.
    pub fn with_publisher(publisher: Arc<dyn IggyPublisher>) -> Self {
        Self {
            publisher,
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
        }
    }

    /// Creates a new `OrdersHandler` wired to both a custom `IggyPublisher` and `EventStreamBroadcaster`.
    pub fn with_publisher_and_broadcaster(
        publisher: Arc<dyn IggyPublisher>,
        event_broadcaster: Arc<EventStreamBroadcaster>,
    ) -> Self {
        Self {
            publisher,
            event_broadcaster,
        }
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

        let trimmed_order_id = payload.order_id.trim();
        let trimmed_reason = payload.reason.trim();

        let credit_cmd = IssueCourtesyCreditCommand::new(
            trimmed_order_id,
            payload.amount_cents,
            trimmed_reason,
        );
        let command_id = credit_cmd.command_id;
        let order_cmd = OrderCommand::IssueCourtesyCredit(credit_cmd);

        self.publisher
            .publish_command(&order_cmd)
            .await
            .map_err(|e| ApiError::internal_error(e.into_problem_details(Some(INSTANCE.to_string()))))?;

        let _ = self.event_broadcaster.publish(DomainEventStreamItem {
            event_id: command_id,
            event_type: "CreditIssued".to_string(),
            aggregate_id: trimmed_order_id.to_string(),
            payload: json!({
                "command_id": command_id,
                "order_id": trimmed_order_id,
                "amount_cents": payload.amount_cents,
                "reason": trimmed_reason,
                "issued_by": verified_claims.sub,
            }),
            timestamp: Utc::now(),
        });

        info!(
            %command_id,
            order_id = %trimmed_order_id,
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

        let trimmed_order_id = payload.order_id.trim();
        let trimmed_reason = payload.reason.trim();

        let cancel_cmd = CancelOrderCommand::new(trimmed_order_id, trimmed_reason);
        let command_id = cancel_cmd.command_id;
        let order_cmd = OrderCommand::CancelOrder(cancel_cmd);

        self.publisher
            .publish_command(&order_cmd)
            .await
            .map_err(|e| ApiError::internal_error(e.into_problem_details(Some(INSTANCE.to_string()))))?;

        let _ = self.event_broadcaster.publish(DomainEventStreamItem {
            event_id: command_id,
            event_type: "OrderCancelled".to_string(),
            aggregate_id: trimmed_order_id.to_string(),
            payload: json!({
                "command_id": command_id,
                "order_id": trimmed_order_id,
                "reason": trimmed_reason,
                "cancelled_by": verified_claims.sub,
            }),
            timestamp: Utc::now(),
        });

        info!(
            %command_id,
            order_id = %trimmed_order_id,
            user = %verified_claims.sub,
            "Dispatched CancelOrder command to Apache Iggy orders/intake"
        );

        Ok(CancelOrderResponse::Accepted(CommandAcceptedResponse::new(
            command_id,
        )))
    }
}
