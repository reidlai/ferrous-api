//! Webhook ingress gateway for external settlement callbacks.
//!
//! Exposes `POST /api/v1/webhooks/orders/settlement-callback`, enforcing JWT authentication
//! via `RequireScope` with `write:orders` (`SCOPE_API_WRITE`), translating incoming callback
//! payloads into domain commands (`SettleCourtesyCreditCommand`, `FailCourtesyCreditSettlementCommand`),
//! executing `CommandHandler` synchronously against SierraDB, publishing committed events
//! to Apache Iggy (`orders/events`), and broadcasting notifications over SSE.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use ferrous_cqrses::commands::{
    FailCourtesyCreditSettlementCommand, OrderCommand, SettleCourtesyCreditCommand,
};
use ferrous_cqrses::domain::order::DomainError;
use ferrous_cqrses::events::DomainEvent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;

use crate::auth::{AuthError, SCOPE_API_WRITE};
use crate::generated::{
    apis::{ApiError, ValidatedJson},
    models::{Claims, DomainEventStreamItem, InvalidParam, ProblemDetails, ValidatePayload},
};
use crate::state::AppState;

/// Status reported by external settlement callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementStatus {
    Settled,
    Failed,
}

/// Request payload for external courtesy credit settlement callback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementCallbackPayload {
    pub order_id: String,
    pub amount_cents: i64,
    pub status: SettlementStatus,
    pub transaction_ref: Option<String>,
    pub reason: Option<String>,
}

impl ValidatePayload for SettlementCallbackPayload {
    fn validate(&self) -> Result<(), Box<ProblemDetails>> {
        let mut errors = Vec::new();
        if self.order_id.trim().is_empty() {
            errors.push(InvalidParam::new("order_id", "order_id cannot be blank"));
        }
        if self.amount_cents <= 0 {
            errors.push(InvalidParam::new(
                "amount_cents",
                "amount_cents must be strictly greater than 0",
            ));
        }
        if self.status == SettlementStatus::Settled {
            match &self.transaction_ref {
                Some(ref tr) if !tr.trim().is_empty() => {}
                _ => {
                    errors.push(InvalidParam::new(
                        "transaction_ref",
                        "transaction_ref is required for settled status and cannot be blank",
                    ));
                }
            }
        }
        if self.status == SettlementStatus::Failed {
            match &self.reason {
                Some(ref r) if !r.trim().is_empty() => {}
                _ => {
                    errors.push(InvalidParam::new(
                        "reason",
                        "reason is required for failed status and cannot be blank",
                    ));
                }
            }
        }
        if !errors.is_empty() {
            let mut problem = ProblemDetails::unprocessable_entity(
                "Webhook payload validation failed",
                Some(errors),
            );
            problem.instance = Some("/api/v1/webhooks/orders/settlement-callback".to_string());
            return Err(Box::new(problem));
        }
        Ok(())
    }
}

/// Response returned on successful webhook callback processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementCallbackResponse {
    pub status: String,
    pub command_id: uuid::Uuid,
    pub order_id: String,
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
        DomainError::InvalidAmount(amount) => {
            let mut problem = ProblemDetails::bad_request(
                format!("Invalid amount: {amount} cents"),
                None,
            );
            problem.instance = Some(instance.to_string());
            problem.code = Some("InvalidAmount".to_string());
            problem.title = "InvalidAmount".to_string();
            problem.r#type = "urn:ferrouscx:error:invalid-amount".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::DuplicateCommand(id) => {
            let mut problem = ProblemDetails::bad_request(format!("Duplicate command ID: {id}"), None);
            problem.instance = Some(instance.to_string());
            problem.code = Some("DuplicateCommand".to_string());
            problem.title = "DuplicateCommand".to_string();
            ApiError::bad_request(problem)
        }
        DomainError::VersionMismatch { expected, actual } => {
            let mut problem = ProblemDetails::bad_request(
                format!("Version mismatch: expected {expected}, actual {actual}"),
                None,
            );
            problem.instance = Some(instance.to_string());
            problem.code = Some("VersionMismatch".to_string());
            problem.title = "VersionMismatch".to_string();
            problem.r#type = "urn:ferrouscx:error:version-mismatch".to_string();
            ApiError::bad_request(problem)
        }
        other => {
            let mut problem = ProblemDetails::internal_error(other.to_string());
            problem.instance = Some(instance.to_string());
            ApiError::internal_error(problem)
        }
    }
}

/// Ingress handler for `POST /api/v1/webhooks/orders/settlement-callback`.
pub async fn handle_settlement_callback(
    State(state): State<AppState>,
    claims: Option<Extension<Claims>>,
    ValidatedJson(payload): ValidatedJson<SettlementCallbackPayload>,
) -> Result<Response, ApiError> {
    const INSTANCE: &str = "/api/v1/webhooks/orders/settlement-callback";

    let verified_claims = claims.as_ref().map(|c| &c.0).ok_or_else(|| {
        ApiError::unauthorized(
            AuthError::MissingToken.into_problem_details(Some(INSTANCE.to_string())),
        )
    })?;

    if !verified_claims.has_scope(SCOPE_API_WRITE) {
        return Err(ApiError::forbidden(
            AuthError::InsufficientScope {
                required: SCOPE_API_WRITE.to_string(),
            }
            .into_problem_details(Some(INSTANCE.to_string())),
        ));
    }

    let trimmed_order_id = payload.order_id.trim();

    let (order_cmd, command_id) = match payload.status {
        SettlementStatus::Settled => {
            let tx_ref = payload.transaction_ref.as_deref().unwrap_or_default().trim();
            // Derive deterministic UUIDv5 for idempotency using (order_id, transaction_ref)
            let cmd_id = uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("{trimmed_order_id}:{tx_ref}").as_bytes(),
            );
            let cmd = SettleCourtesyCreditCommand::new(
                trimmed_order_id,
                payload.amount_cents,
                tx_ref,
            )
            .with_command_id(cmd_id);
            (OrderCommand::SettleCourtesyCredit(cmd), cmd_id)
        }
        SettlementStatus::Failed => {
            let reason = payload.reason.as_deref().unwrap_or_default().trim();
            // Derive deterministic UUIDv5 for idempotency using (order_id, reason, amount_cents)
            let cmd_id = uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("{trimmed_order_id}:{reason}:{}", payload.amount_cents).as_bytes(),
            );
            let cmd = FailCourtesyCreditSettlementCommand::new(
                trimmed_order_id,
                payload.amount_cents,
                reason,
            )
            .with_command_id(cmd_id);
            (OrderCommand::FailCourtesyCreditSettlement(cmd), cmd_id)
        }
    };

    // 1. Synchronously execute CommandHandler against SierraDB
    let domain_event = {
        let mut handler = state.command_handler.lock().await;
        match handler.handle_command(order_cmd).await {
            Ok(ev) => ev,
            Err(DomainError::DuplicateCommand(_)) => {
                // Idempotent retry: acknowledge duplicate with 200 OK
                info!(
                    %command_id,
                    order_id = %trimmed_order_id,
                    "Webhook callback idempotently acknowledged duplicate command"
                );
                return Ok((
                    StatusCode::OK,
                    Json(SettlementCallbackResponse {
                        status: "ok".to_string(),
                        command_id,
                        order_id: trimmed_order_id.to_string(),
                    }),
                )
                    .into_response());
            }
            Err(e) => return Err(map_domain_error(e, INSTANCE)),
        }
    };

    // 2. Post-commit publication to Apache Iggy orders/events
    if let Err(e) = state.publisher.publish_event(&domain_event).await {
        let mut handler = state.command_handler.lock().await;
        handler.rollback_last_event(trimmed_order_id, &command_id);
        return Err(ApiError::internal_error(
            e.into_problem_details(Some(INSTANCE.to_string())),
        ));
    }

    // 3. Post-commit SSE notification
    let (event_type, payload_json) = match &domain_event {
        DomainEvent::CourtesyCreditSettled(e) => (
            "CourtesyCreditSettled",
            json!({
                "command_id": command_id,
                "order_id": e.order_id,
                "amount_cents": e.amount_cents,
                "transaction_ref": e.transaction_ref,
                "settled_at": e.settled_at,
            }),
        ),
        DomainEvent::CourtesyCreditSettlementFailed(e) => (
            "CourtesyCreditSettlementFailed",
            json!({
                "command_id": command_id,
                "order_id": e.order_id,
                "amount_cents": e.amount_cents,
                "reason": e.reason,
                "failed_at": e.failed_at,
            }),
        ),
        _ => unreachable!(),
    };

    let _ = state.event_broadcaster.publish(DomainEventStreamItem {
        event_id: command_id,
        event_type: event_type.to_string(),
        aggregate_id: trimmed_order_id.to_string(),
        payload: payload_json,
        timestamp: domain_event.timestamp(),
    });

    info!(
        %command_id,
        order_id = %trimmed_order_id,
        amount_cents = payload.amount_cents,
        status = ?payload.status,
        user = %verified_claims.sub,
        "Committed settlement domain event to SierraDB and published to orders/events"
    );

    Ok((
        StatusCode::OK,
        Json(SettlementCallbackResponse {
            status: "ok".to_string(),
            command_id,
            order_id: trimmed_order_id.to_string(),
        }),
    )
        .into_response())
}
