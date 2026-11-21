//! Orders command handler implementing `OrdersApi` trait, validating domain invariants
//! synchronously against SierraDB via `CommandHandler`, publishing committed `DomainEvent`s
//! to Apache Iggy (`orders/consolidated`), and broadcasting post-commit `DomainEventStreamItem`
//! notifications over SSE.

use std::sync::Arc;

use async_trait::async_trait;
use ferrous_cqrses::commands::{CancelOrderCommand, IssueCourtesyCreditCommand, OrderCommand};
use ferrous_cqrses::domain::order::DomainError;
use ferrous_cqrses::handler::CommandHandler;
use serde_json::json;
use tracing::info;

use crate::auth::{AuthError, SCOPE_API_WRITE};
use crate::generated::{
    models::ProblemDetails, ApiError, CancelOrderRequest, CancelOrderResponse, Claims,
    CommandAcceptedResponse, DomainEventStreamItem, IssueCreditRequest, IssueCreditResponse,
    OrdersApi,
};
use crate::publisher::{IggyPublisher, InMemoryIggyPublisher};
use crate::sse::EventStreamBroadcaster;
use crate::state::AppState;

/// Pure-Rust implementation of `OrdersApi` server trait wired to `AppState`, `IggyPublisher`,
/// `EventStreamBroadcaster`, and `CommandHandler`.
#[derive(Debug, Clone)]
pub struct OrdersHandler {
    publisher: Arc<dyn IggyPublisher>,
    event_broadcaster: Arc<EventStreamBroadcaster>,
    command_handler: Arc<tokio::sync::Mutex<CommandHandler>>,
}

impl Default for OrdersHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl OrdersHandler {
    /// Creates a new `OrdersHandler` backed by default in-memory components.
    pub fn new() -> Self {
        Self {
            publisher: Arc::new(InMemoryIggyPublisher::new()),
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
        }
    }

    /// Creates a new `OrdersHandler` wired to the provided `AppState`.
    pub fn with_state(state: AppState) -> Self {
        Self {
            publisher: state.publisher,
            event_broadcaster: state.event_broadcaster,
            command_handler: state.command_handler,
        }
    }

    /// Creates a new `OrdersHandler` wired to a custom `IggyPublisher`.
    pub fn with_publisher(publisher: Arc<dyn IggyPublisher>) -> Self {
        Self {
            publisher,
            event_broadcaster: Arc::new(EventStreamBroadcaster::new()),
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
        }
    }

    /// Creates a new `OrdersHandler` wired to a custom `IggyPublisher` and `EventStreamBroadcaster`.
    pub fn with_publisher_and_broadcaster(
        publisher: Arc<dyn IggyPublisher>,
        event_broadcaster: Arc<EventStreamBroadcaster>,
    ) -> Self {
        Self {
            publisher,
            event_broadcaster,
            command_handler: Arc::new(tokio::sync::Mutex::new(CommandHandler::new())),
        }
    }

    /// Creates a new `OrdersHandler` wired to custom publisher, broadcaster, and command handler.
    pub fn with_all(
        publisher: Arc<dyn IggyPublisher>,
        event_broadcaster: Arc<EventStreamBroadcaster>,
        command_handler: Arc<tokio::sync::Mutex<CommandHandler>>,
    ) -> Self {
        Self {
            publisher,
            event_broadcaster,
            command_handler,
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

/// Maps domain invariant and rehydration errors to strongly typed RFC 7807 `ApiError` responses.
fn map_domain_error(e: DomainError, instance: &str) -> ApiError {
    match e {
        DomainError::CourtesyCreditExceeded(cur, max) => {
            let mut problem = ProblemDetails::bad_request(
                format!("Courtesy credit of {cur} cents exceeds maximum allowed limit of {max} cents ($20.00)"),
                None,
            );
            problem.instance = Some(instance.to_string());
            problem.code = Some("CourtesyCreditExceeded".to_string());
            problem.title = "CourtesyCreditExceeded".to_string();
            problem.r#type = "urn:ferrouscx:error:courtesy-credit-exceeded".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::OrderCancelled => {
            let mut problem = ProblemDetails::bad_request("Order is cancelled", None);
            problem.instance = Some(instance.to_string());
            problem.code = Some("OrderCancelled".to_string());
            problem.title = "OrderCancelled".to_string();
            problem.r#type = "urn:ferrouscx:error:order-cancelled".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::OrderNotFound(order_id) => {
            let mut problem = ProblemDetails::not_found(
                format!("Order '{order_id}' not found"),
                Some(instance.to_string()),
            );
            problem.code = Some("OrderNotFound".to_string());
            problem.title = "OrderNotFound".to_string();
            problem.r#type = "urn:ferrouscx:error:order-not-found".to_string();
            ApiError::not_found(problem)
        }
        DomainError::OrderFraudLocked => {
            let mut problem = ProblemDetails::bad_request("Order is fraud-locked", None);
            problem.instance = Some(instance.to_string());
            problem.code = Some("OrderFraudLocked".to_string());
            problem.title = "OrderFraudLocked".to_string();
            problem.r#type = "urn:ferrouscx:error:order-fraud-locked".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::OrderDelivered => {
            let mut problem = ProblemDetails::bad_request("Order has already been delivered", None);
            problem.instance = Some(instance.to_string());
            problem.code = Some("OrderDelivered".to_string());
            problem.title = "OrderDelivered".to_string();
            problem.r#type = "urn:ferrouscx:error:order-delivered".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::DuplicateCommand(id) => {
            let mut problem = ProblemDetails::bad_request(format!("Duplicate command ID: {id}"), None);
            problem.instance = Some(instance.to_string());
            problem.code = Some("DuplicateCommand".to_string());
            problem.title = "DuplicateCommand".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::InvalidAmount(amount) => {
            let mut problem = ProblemDetails::bad_request(
                format!("Invalid amount: {amount} cents must be strictly positive"),
                None,
            );
            problem.instance = Some(instance.to_string());
            problem.code = Some("InvalidAmount".to_string());
            problem.title = "InvalidAmount".to_string();
            ApiError::bad_request(problem)
        }
        other => {
            let mut problem = ProblemDetails::internal_error(other.to_string());
            problem.instance = Some(instance.to_string());
            ApiError::internal_error(problem)
        }
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

        let credit_cmd =
            IssueCourtesyCreditCommand::new(trimmed_order_id, payload.amount_cents, trimmed_reason);
        let command_id = credit_cmd.command_id;
        let order_cmd = OrderCommand::IssueCourtesyCredit(credit_cmd);

        // Topology A execution:
        // 1. Validate domain invariants & persist to SierraDB via CommandHandler
        let domain_event = {
            let mut handler = self.command_handler.lock().await;
            handler
                .handle_command(order_cmd)
                .await
                .map_err(|e| map_domain_error(e, INSTANCE))?
        };

        // 2. Post-commit publication to Apache Iggy orders/consolidated
        self.publisher
            .publish_event(&domain_event)
            .await
            .map_err(|e| {
                ApiError::internal_error(e.into_problem_details(Some(INSTANCE.to_string())))
            })?;

        // 3. Post-commit SSE notification
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
            timestamp: domain_event.timestamp(),
        });

        info!(
            %command_id,
            order_id = %trimmed_order_id,
            amount_cents = payload.amount_cents,
            user = %verified_claims.sub,
            "Committed CourtesyCredit concession to SierraDB and published to orders/consolidated"
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

        // Topology A execution:
        // 1. Validate domain invariants & persist to SierraDB via CommandHandler
        let domain_event = {
            let mut handler = self.command_handler.lock().await;
            handler
                .handle_command(order_cmd)
                .await
                .map_err(|e| map_domain_error(e, INSTANCE))?
        };

        // 2. Post-commit publication to Apache Iggy orders/consolidated
        self.publisher
            .publish_event(&domain_event)
            .await
            .map_err(|e| {
                ApiError::internal_error(e.into_problem_details(Some(INSTANCE.to_string())))
            })?;

        // 3. Post-commit SSE notification
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
            timestamp: domain_event.timestamp(),
        });

        info!(
            %command_id,
            order_id = %trimmed_order_id,
            user = %verified_claims.sub,
            "Committed OrderCancelled to SierraDB and published to orders/consolidated"
        );

        Ok(CancelOrderResponse::Accepted(CommandAcceptedResponse::new(
            command_id,
        )))
    }
}
