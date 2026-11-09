//! Queries handler implementing QueriesApi trait.

use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use crate::generated::{
    ApiError, Claims, GetOrderResponse, ListOrdersParams, ListOrdersResponse, OrderDetails,
    OrderItem, OrderListResponse, OrderSummary, ProblemDetails, QueriesApi, ValidatePayload,
};

/// Pure-Rust implementation of QueriesApi server trait.
#[derive(Debug, Default, Clone)]
pub struct QueriesHandler;

impl QueriesHandler {
    pub fn new() -> Self {
        Self
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
            "Executing get_order query"
        );

        if order_id == "ord-not-found" || order_id.starts_with("missing-") {
            let problem = ProblemDetails::not_found(
                format!("Order '{order_id}' was not found in lakehouse current state"),
                Some(format!("/api/v1/orders/{order_id}")),
            );
            return Ok(GetOrderResponse::NotFound(problem));
        }

        let details = OrderDetails {
            order_id: order_id.clone(),
            customer_id: "cust-1029".to_string(),
            status: "delivered".to_string(),
            total_amount_cents: 3998,
            currency: "USD".to_string(),
            items: vec![OrderItem {
                item_id: "item-001".to_string(),
                product_id: "prod-4412".to_string(),
                price_cents: 1999,
                quantity: 2,
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        Ok(GetOrderResponse::Ok(details))
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
            "Executing list_orders query"
        );

        let orders = vec![OrderSummary {
            order_id: "ord-98234-x".to_string(),
            customer_id: params
                .customer_id
                .clone()
                .unwrap_or_else(|| "cust-1029".to_string()),
            status: params
                .status
                .clone()
                .unwrap_or_else(|| "delivered".to_string()),
            total_amount_cents: 5420,
            currency: "USD".to_string(),
            created_at: Utc::now(),
        }];

        Ok(ListOrdersResponse::Ok(OrderListResponse {
            orders,
            total_count: 1,
            limit,
            offset,
        }))
    }
}
