//! Transactional TiDB [`OrderQueryEngine`] implementation (Story 23.4, AD-36).
//!
//! Queries TiDB `orders_current_state` merged with `olist_orders` and hydrates line items
//! from `olist_order_items` via indexed MySQL-protocol SQL (`sqlx` + `tls-rustls`).

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};
use std::time::Duration;

use crate::generated::{OrderDetails, OrderItem, OrderListResponse, OrderSummary};
use crate::query_engine::{OrderQueryEngine, QueryEngineError};

/// Live TiDB transactional [`OrderQueryEngine`] backend (AD-36).
#[derive(Debug, Clone)]
pub struct TiDbOrderQueryEngine {
    pool: MySqlPool,
}

impl TiDbOrderQueryEngine {
    pub async fn connect(tidb_url: &str) -> Result<Self, QueryEngineError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(tidb_url)
            .await
            .map_err(|e| QueryEngineError::StorageIo(format!("TiDB connect failed: {e}")))?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }

    async fn fetch_olist_items(&self, order_id: &str) -> Vec<OrderItem> {
        let rows = sqlx::query(
            "SELECT order_item_id, product_id, price \
             FROM olist_order_items WHERE order_id = ? ORDER BY order_item_id ASC",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        rows.into_iter()
            .map(|r| {
                let seq: i32 = r.try_get("order_item_id").unwrap_or(1);
                let product_id: String = r.try_get("product_id").unwrap_or_default();
                let price: f64 = r.try_get("price").unwrap_or(0.0);
                OrderItem {
                    item_id: format!("{order_id}-{seq}"),
                    product_id,
                    price_cents: (price * 100.0).round() as i64,
                    quantity: 1,
                }
            })
            .collect()
    }
}

fn parse_tidb_dt(raw: Option<String>) -> DateTime<Utc> {
    let Some(s) = raw else {
        return Utc::now();
    };
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return dt.with_timezone(&Utc);
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s.trim(), fmt) {
            return ndt.and_utc();
        }
    }
    Utc::now()
}

#[async_trait::async_trait]
impl OrderQueryEngine for TiDbOrderQueryEngine {
    async fn get_order(
        &self,
        query: &ferrous_cqrses::queries::GetOrderQuery,
    ) -> Result<Option<OrderDetails>, QueryEngineError> {
        query
            .validate()
            .map_err(|e| QueryEngineError::InvalidQuery(e.to_string()))?;

        // 1. Check CQRS orders_current_state first
        let cqrs_row = sqlx::query(
            "SELECT order_id, customer_id, order_status, total_amount_cents, \
                    CAST(order_purchase_timestamp AS CHAR) AS created_str, \
                    CAST(updated_at AS CHAR) AS updated_str \
             FROM orders_current_state WHERE order_id = ?",
        )
        .bind(&query.order_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| QueryEngineError::StorageIo(format!("TiDB get_order(state): {e}")))?;

        if let Some(row) = cqrs_row {
            let order_id: String = row.try_get("order_id").unwrap_or_default();
            let customer_id: String = row.try_get("customer_id").unwrap_or_default();
            let status: String = row.try_get("order_status").unwrap_or_default();
            let mut total_amount_cents: i64 = row.try_get("total_amount_cents").unwrap_or(0);
            let created_at = parse_tidb_dt(row.try_get::<Option<String>, _>("created_str").ok().flatten());
            let updated_at = parse_tidb_dt(row.try_get::<Option<String>, _>("updated_str").ok().flatten());

            let items = self.fetch_olist_items(&order_id).await;
            if total_amount_cents == 0 && !items.is_empty() {
                total_amount_cents = items.iter().map(|i| i.price_cents).sum();
            }

            return Ok(Some(OrderDetails {
                order_id,
                customer_id,
                status,
                total_amount_cents,
                currency: "BRL".to_string(),
                items,
                created_at,
                updated_at,
            }));
        }

        // 2. Fall back to canonical olist_orders base table
        let olist_row = sqlx::query(
            "SELECT order_id, customer_id, order_status, \
                    CAST(order_purchase_timestamp AS CHAR) AS created_str \
             FROM olist_orders WHERE order_id = ?",
        )
        .bind(&query.order_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| QueryEngineError::StorageIo(format!("TiDB get_order(olist): {e}")))?;

        let Some(row) = olist_row else {
            return Ok(None);
        };

        let order_id: String = row.try_get("order_id").unwrap_or_default();
        let customer_id: String = row.try_get("customer_id").unwrap_or_default();
        let status: String = row.try_get("order_status").unwrap_or_default();
        let created_at = parse_tidb_dt(row.try_get::<Option<String>, _>("created_str").ok().flatten());
        let items = self.fetch_olist_items(&order_id).await;
        let total_amount_cents: i64 = items.iter().map(|i| i.price_cents).sum();

        Ok(Some(OrderDetails {
            order_id,
            customer_id,
            status,
            total_amount_cents,
            currency: "BRL".to_string(),
            items,
            created_at,
            updated_at: created_at,
        }))
    }

    async fn list_orders(
        &self,
        query: &ferrous_cqrses::queries::ListOrdersQuery,
    ) -> Result<OrderListResponse, QueryEngineError> {
        query
            .validate()
            .map_err(|e| QueryEngineError::InvalidQuery(e.to_string()))?;

        let customer_filter = query.customer_id.as_deref();
        let status_filter = query.status.as_deref();

        // Unified view: orders_current_state + non-overlapping olist_orders
        let union_cte = "WITH unified AS ( \
            SELECT order_id, customer_id, order_status, total_amount_cents, \
                   order_purchase_timestamp \
            FROM orders_current_state \
            UNION ALL \
            SELECT o.order_id, o.customer_id, o.order_status, \
                   COALESCE((SELECT CAST(ROUND(SUM(i.price) * 100) AS SIGNED) \
                             FROM olist_order_items i WHERE i.order_id = o.order_id), 0) AS total_amount_cents, \
                   o.order_purchase_timestamp \
            FROM olist_orders o \
            WHERE NOT EXISTS (SELECT 1 FROM orders_current_state s WHERE s.order_id = o.order_id) \
        ) ";

        let count_sql = format!(
            "{union_cte} SELECT COUNT(*) FROM unified \
             WHERE (? IS NULL OR customer_id = ?) AND (? IS NULL OR order_status = ?)"
        );
        let total_count: i64 = sqlx::query_scalar(&count_sql)
            .bind(customer_filter)
            .bind(customer_filter)
            .bind(status_filter)
            .bind(status_filter)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| QueryEngineError::StorageIo(format!("TiDB list_orders count: {e}")))?;

        let list_sql = format!(
            "{union_cte} SELECT order_id, customer_id, order_status, total_amount_cents, \
                                CAST(order_purchase_timestamp AS CHAR) AS created_str \
             FROM unified \
             WHERE (? IS NULL OR customer_id = ?) AND (? IS NULL OR order_status = ?) \
             ORDER BY order_purchase_timestamp DESC \
             LIMIT ? OFFSET ?"
        );
        let rows = sqlx::query(&list_sql)
            .bind(customer_filter)
            .bind(customer_filter)
            .bind(status_filter)
            .bind(status_filter)
            .bind(i64::from(query.limit))
            .bind(i64::from(query.offset))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| QueryEngineError::StorageIo(format!("TiDB list_orders scan: {e}")))?;

        let orders = rows
            .into_iter()
            .map(|r| OrderSummary {
                order_id: r.try_get("order_id").unwrap_or_default(),
                customer_id: r.try_get("customer_id").unwrap_or_default(),
                status: r.try_get("order_status").unwrap_or_default(),
                total_amount_cents: r.try_get("total_amount_cents").unwrap_or(0),
                currency: "BRL".to_string(),
                created_at: parse_tidb_dt(r.try_get::<Option<String>, _>("created_str").ok().flatten()),
            })
            .collect();

        Ok(OrderListResponse {
            orders,
            total_count,
            limit: i64::from(query.limit),
            offset: i64::from(query.offset),
        })
    }
}
