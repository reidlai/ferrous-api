//! Orders command handler implementing OrdersApi trait.

use async_trait::async_trait;
use ferrous_cqrses::id::generate_uuidv7;
use tracing::info;

use crate::generated::{
    ApiError, CancelOrderRequest, CancelOrderResponse, Claims, CommandAcceptedResponse,
    IssueCreditRequest, IssueCreditResponse, OrdersApi,
};

/// Pure-Rust implementation of OrdersApi server trait.
#[derive(Debug, Default, Clone)]
pub struct OrdersHandler;

impl OrdersHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl OrdersApi for OrdersHandler {
    async fn issue_credit(
        &self,
        claims: Option<Claims>,
        payload: IssueCreditRequest,
    ) -> Result<IssueCreditResponse, ApiError> {
        let command_id = generate_uuidv7();
        info!(
            %command_id,
            order_id = %payload.order_id,
            amount_cents = payload.amount_cents,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Dispatched IssueCourtesyCredit command"
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
        let command_id = generate_uuidv7();
        info!(
            %command_id,
            order_id = %payload.order_id,
            user = claims.as_ref().map(|c| c.sub.as_str()).unwrap_or("anonymous"),
            "Dispatched CancelOrder command"
        );

        Ok(CancelOrderResponse::Accepted(CommandAcceptedResponse::new(
            command_id,
        )))
    }
}
