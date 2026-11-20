//! Asynchronous Callback Service for enterprise settlement workflows.
//!
//! Replaces synchronous HTTP webhook endpoints with a resilient message-driven callback
//! service. Consumes settlement messages from Apache Iggy topic `orders/callbacks`
//! (consumer group `cg-order-callback`), executes deterministic invariant validation
//! against SierraDB aggregates via `CommandHandler`, commits resulting domain events
//! (`CourtesyCreditSettled` or `CourtesyCreditSettlementFailed`) via `EAPPEND`, publishes
//! committed events to Apache Iggy (`orders/events`), and broadcasts real-time updates
//! via Server-Sent Events (SSE).

use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;
use uuid::Uuid;

use ferrous_cqrses::commands::{
    FailCourtesyCreditSettlementCommand, OrderCommand, SettleCourtesyCreditCommand,
};
use ferrous_cqrses::domain::order::DomainError;
use ferrous_cqrses::events::DomainEvent;

use crate::generated::models::DomainEventStreamItem;
use crate::publisher::PublisherError;
use crate::state::AppState;

/// Default Apache Iggy topic name for incoming callback responses.
pub const CALLBACKS_TOPIC: &str = "callbacks";

/// Default Apache Iggy consumer group for the callback service.
pub const CALLBACKS_CONSUMER_GROUP: &str = "cg-order-callback";

/// Status reported by external enterprise settlement workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementStatus {
    Settled,
    Failed,
}

/// Incoming message payload from the callback queue (`orders/callbacks`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementCallbackMessage {
    pub order_id: String,
    pub amount_cents: i64,
    pub status: SettlementStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl SettlementCallbackMessage {
    /// Validates the structure and invariants of the callback payload.
    pub fn validate(&self) -> Result<(), CallbackError> {
        if self.order_id.trim().is_empty() {
            return Err(CallbackError::InvalidPayload(
                "order_id cannot be blank".to_string(),
            ));
        }
        if self.amount_cents <= 0 {
            return Err(CallbackError::InvalidPayload(
                "amount_cents must be strictly greater than 0".to_string(),
            ));
        }
        match self.status {
            SettlementStatus::Settled => match &self.transaction_ref {
                Some(tr) if !tr.trim().is_empty() => {}
                _ => {
                    return Err(CallbackError::InvalidPayload(
                        "transaction_ref is required for settled status and cannot be blank"
                            .to_string(),
                    ));
                }
            },
            SettlementStatus::Failed => match &self.reason {
                Some(r) if !r.trim().is_empty() => {}
                _ => {
                    return Err(CallbackError::InvalidPayload(
                        "reason is required for failed status and cannot be blank".to_string(),
                    ));
                }
            },
        }
        Ok(())
    }
}

/// Result of processing an incoming settlement callback message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackOutcome {
    Processed {
        command_id: Uuid,
        order_id: String,
        event_type: String,
    },
    DuplicateIgnored {
        command_id: Uuid,
        order_id: String,
    },
}

/// Errors that can occur during callback message processing.
#[derive(Debug, thiserror::Error)]
pub enum CallbackError {
    #[error("Invalid callback payload: {0}")]
    InvalidPayload(String),

    #[error("JSON deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    #[error("Domain invariant violation: {0}")]
    Domain(#[from] DomainError),

    #[error("Event publisher error: {0}")]
    Publisher(#[from] PublisherError),
}

/// Asynchronous callback processing service.
#[derive(Clone)]
pub struct CallbackService {
    app_state: AppState,
}

impl CallbackService {
    /// Creates a new `CallbackService` wired to the shared `AppState`.
    pub fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    /// Returns a reference to the underlying `AppState`.
    pub fn app_state(&self) -> &AppState {
        &self.app_state
    }

    /// Deserializes and processes raw JSON message bytes from the callback queue.
    pub async fn process_message(&self, payload_bytes: &[u8]) -> Result<CallbackOutcome, CallbackError> {
        let message: SettlementCallbackMessage = serde_json::from_slice(payload_bytes)?;
        self.process_callback(message).await
    }

    /// Validates and processes a typed settlement callback message.
    pub async fn process_callback(
        &self,
        message: SettlementCallbackMessage,
    ) -> Result<CallbackOutcome, CallbackError> {
        message.validate()?;

        let trimmed_order_id = message.order_id.trim();

        let (order_cmd, command_id) = match message.status {
            SettlementStatus::Settled => {
                let tx_ref = message.transaction_ref.as_deref().unwrap_or_default().trim();
                let cmd_id = Uuid::new_v5(
                    &Uuid::NAMESPACE_OID,
                    format!("{trimmed_order_id}:{tx_ref}").as_bytes(),
                );
                let cmd = SettleCourtesyCreditCommand::new(
                    trimmed_order_id,
                    message.amount_cents,
                    tx_ref,
                )
                .with_command_id(cmd_id);
                (OrderCommand::SettleCourtesyCredit(cmd), cmd_id)
            }
            SettlementStatus::Failed => {
                let reason = message.reason.as_deref().unwrap_or_default().trim();
                let cmd_id = Uuid::new_v5(
                    &Uuid::NAMESPACE_OID,
                    format!("{trimmed_order_id}:{reason}:{}", message.amount_cents).as_bytes(),
                );
                let cmd = FailCourtesyCreditSettlementCommand::new(
                    trimmed_order_id,
                    message.amount_cents,
                    reason,
                )
                .with_command_id(cmd_id);
                (OrderCommand::FailCourtesyCreditSettlement(cmd), cmd_id)
            }
        };

        // 1. Synchronously execute CommandHandler against SierraDB
        let domain_event = {
            let mut handler = self.app_state.command_handler.lock().await;
            match handler.handle_command(order_cmd).await {
                Ok(ev) => ev,
                Err(DomainError::DuplicateCommand(_)) => {
                    info!(
                        %command_id,
                        order_id = %trimmed_order_id,
                        "Callback queue consumer idempotently acknowledged duplicate command"
                    );
                    return Ok(CallbackOutcome::DuplicateIgnored {
                        command_id,
                        order_id: trimmed_order_id.to_string(),
                    });
                }
                Err(e) => return Err(CallbackError::Domain(e)),
            }
        };

        // 2. Post-commit publication to Apache Iggy orders/events
        if let Err(e) = self.app_state.publisher.publish_event(&domain_event).await {
            let mut handler = self.app_state.command_handler.lock().await;
            handler.rollback_last_event(trimmed_order_id, &command_id);
            return Err(CallbackError::Publisher(e));
        }

        // 3. Post-commit SSE notification broadcast
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

        let _ = self.app_state.event_broadcaster.publish(DomainEventStreamItem {
            event_id: command_id,
            event_type: event_type.to_string(),
            aggregate_id: trimmed_order_id.to_string(),
            payload: payload_json,
            timestamp: domain_event.timestamp(),
        });

        info!(
            %command_id,
            order_id = %trimmed_order_id,
            amount_cents = message.amount_cents,
            status = ?message.status,
            "CallbackService successfully committed settlement domain event to SierraDB and published to orders/events"
        );

        Ok(CallbackOutcome::Processed {
            command_id,
            order_id: trimmed_order_id.to_string(),
            event_type: event_type.to_string(),
        })
    }
}
