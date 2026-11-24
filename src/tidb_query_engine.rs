//! Transactional TiDB [`OrderQueryEngine`] implementation (Story 23.4, AD-36).
//!
//! Queries TiDB `orders_current_state` merged with `olist_orders` and hydrates line items
//! from `olist_order_items` via indexed MySQL-protocol SQL (`sqlx` + `tls-rustls`).

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};
use std::time::Duration;

use crate::generated::{
    OlistTableStat, OlistTablesResponse, OrderDetails, OrderItem, OrderListResponse, OrderPayment,
    OrderReview, OrderSummary,
};
use crate::query_engine::{OrderQueryEngine, QueryEngineError};

/// Canonical 10-table metadata: 9 Kaggle Olist tables + CQRS `orders_current_state`.
pub const TIDB_OLIST_TABLE_METADATA: &[(&str, &str, &str)] = &[
    ("olist_orders", "Canonical Order Fact", "stream: olist/orders_stream"),
    ("olist_order_items", "Order Line Items", "stream: olist/order_items_stream"),
    ("olist_order_payments", "Order Payment Legs", "stream: olist/order_payments_stream"),
    ("olist_order_reviews", "Customer Reviews", "stream: olist/order_reviews_stream"),
    ("olist_customers", "Customer Directory", "dimension: batch"),
    ("olist_products", "Product Catalog", "dimension: batch"),
    ("olist_sellers", "Seller Directory", "dimension: batch"),
    ("olist_geolocation", "CEP Geolocation", "dimension: batch"),
    ("olist_category_translation", "PT→EN Categories", "dimension: batch"),
    ("orders_current_state", "CQRS Materialized View", "stream: orders/consolidated"),
];

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
            "SELECT i.order_item_id, i.product_id, i.seller_id, i.price, i.freight_value, \
                    CAST(i.shipping_limit_date AS CHAR) AS shipping_limit_str, \
                    p.product_category_name, t.product_category_name_english, \
                    s.seller_city, s.seller_state \
             FROM olist_order_items i \
             LEFT JOIN olist_products p ON p.product_id = i.product_id \
             LEFT JOIN olist_category_translation t ON t.product_category_name = p.product_category_name \
             LEFT JOIN olist_sellers s ON s.seller_id = i.seller_id \
             WHERE i.order_id = ? \
             ORDER BY i.order_item_id ASC",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        rows.into_iter()
            .map(|r| {
                let seq: i32 = r.try_get("order_item_id").unwrap_or(1);
                let product_id: String = r.try_get("product_id").unwrap_or_default();
                let seller_id: Option<String> = r.try_get("seller_id").ok();
                let seller_city: Option<String> = r.try_get("seller_city").ok();
                let seller_state: Option<String> = r.try_get("seller_state").ok();
                let price: f64 = r.try_get("price").unwrap_or(0.0);
                let freight: f64 = r.try_get("freight_value").unwrap_or(0.0);
                let category_name: Option<String> = r.try_get("product_category_name").ok();
                let category_english: Option<String> = r.try_get("product_category_name_english").ok();
                let shipping_limit_date = parse_opt_tidb_dt(
                    r.try_get::<Option<String>, _>("shipping_limit_str").ok().flatten(),
                );
                OrderItem {
                    item_id: format!("{order_id}-{seq}"),
                    product_id,
                    price_cents: (price * 100.0).round() as i64,
                    quantity: 1,
                    seller_id,
                    seller_city,
                    seller_state,
                    freight_cents: Some((freight * 100.0).round() as i64),
                    category_name,
                    category_english,
                    shipping_limit_date,
                }
            })
            .collect()
    }

    async fn fetch_olist_payments(&self, order_id: &str) -> Vec<OrderPayment> {
        let rows = sqlx::query(
            "SELECT payment_sequential, payment_type, payment_installments, payment_value \
             FROM olist_order_payments WHERE order_id = ? ORDER BY payment_sequential ASC",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        rows.into_iter()
            .map(|r| {
                let payment_sequential: i32 = r.try_get("payment_sequential").unwrap_or(1);
                let payment_type: String = r.try_get("payment_type").unwrap_or_default();
                let payment_installments: i32 = r.try_get("payment_installments").unwrap_or(1);
                let payment_value: f64 = r.try_get("payment_value").unwrap_or(0.0);
                OrderPayment {
                    payment_sequential,
                    payment_type,
                    payment_installments,
                    payment_value_cents: (payment_value * 100.0).round() as i64,
                }
            })
            .collect()
    }

    async fn fetch_olist_review(&self, order_id: &str) -> Option<OrderReview> {
        let row = sqlx::query(
            "SELECT review_id, review_score, review_comment_title, review_comment_message, \
                    CAST(review_creation_date AS CHAR) AS creation_str, \
                    CAST(review_answer_timestamp AS CHAR) AS answer_str \
             FROM olist_order_reviews WHERE order_id = ? \
             ORDER BY review_creation_date DESC LIMIT 1",
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()?;

        Some(OrderReview {
            review_id: row.try_get("review_id").unwrap_or_default(),
            review_score: row.try_get("review_score").unwrap_or(0),
            review_comment_title: row.try_get("review_comment_title").ok(),
            review_comment_message: row.try_get("review_comment_message").ok(),
            review_creation_date: parse_opt_tidb_dt(
                row.try_get::<Option<String>, _>("creation_str").ok().flatten(),
            ),
            review_answer_timestamp: parse_opt_tidb_dt(
                row.try_get::<Option<String>, _>("answer_str").ok().flatten(),
            ),
        })
    }

    async fn fetch_olist_customer(
        &self,
        customer_id: &str,
    ) -> Option<(String, String, String, String)> {
        let row = sqlx::query(
            "SELECT customer_unique_id, customer_zip_code_prefix, customer_city, customer_state \
             FROM olist_customers WHERE customer_id = ? LIMIT 1",
        )
        .bind(customer_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()?;

        Some((
            row.try_get("customer_unique_id").unwrap_or_default(),
            row.try_get("customer_zip_code_prefix").unwrap_or_default(),
            row.try_get("customer_city").unwrap_or_default(),
            row.try_get("customer_state").unwrap_or_default(),
        ))
    }
}

fn parse_opt_tidb_dt(raw: Option<String>) -> Option<DateTime<Utc>> {
    let s = raw?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(ndt.and_utc());
        }
    }
    None
}

fn parse_tidb_dt(raw: Option<String>) -> DateTime<Utc> {
    parse_opt_tidb_dt(raw).unwrap_or_else(Utc::now)
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
            "SELECT order_id, customer_id, order_status, total_amount_cents, courtesy_credit_cents, \
                    CAST(order_purchase_timestamp AS CHAR) AS created_str, \
                    CAST(order_approved_at AS CHAR) AS approved_str, \
                    CAST(order_delivered_carrier_date AS CHAR) AS carrier_str, \
                    CAST(order_delivered_customer_date AS CHAR) AS delivered_str, \
                    CAST(order_estimated_delivery_date AS CHAR) AS estimated_str, \
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
            let courtesy_credit_cents: Option<i64> = row.try_get("courtesy_credit_cents").ok();
            let created_at = parse_tidb_dt(row.try_get::<Option<String>, _>("created_str").ok().flatten());
            let updated_at = parse_tidb_dt(row.try_get::<Option<String>, _>("updated_str").ok().flatten());
            let order_approved_at = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("approved_str").ok().flatten());
            let order_delivered_carrier_date = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("carrier_str").ok().flatten());
            let order_delivered_customer_date = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("delivered_str").ok().flatten());
            let order_estimated_delivery_date = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("estimated_str").ok().flatten());

            let (items, payments, review, customer) = tokio::join!(
                self.fetch_olist_items(&order_id),
                self.fetch_olist_payments(&order_id),
                self.fetch_olist_review(&order_id),
                self.fetch_olist_customer(&customer_id),
            );
            if total_amount_cents == 0 && !items.is_empty() {
                total_amount_cents = items
                    .iter()
                    .map(|i| i.price_cents + i.freight_cents.unwrap_or(0))
                    .sum();
            }
            let (customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) =
                match customer {
                    Some((uid, zip, city, state)) => (Some(uid), Some(zip), Some(city), Some(state)),
                    None => (None, None, None, None),
                };

            return Ok(Some(OrderDetails {
                order_id,
                customer_id,
                status,
                total_amount_cents,
                currency: "BRL".to_string(),
                items,
                created_at,
                updated_at,
                order_purchase_timestamp: Some(created_at),
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                courtesy_credit_cents,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                payments,
                review,
            }));
        }

        // 2. Fall back to canonical olist_orders base table
        let olist_row = sqlx::query(
            "SELECT order_id, customer_id, order_status, \
                    CAST(order_purchase_timestamp AS CHAR) AS created_str, \
                    CAST(order_approved_at AS CHAR) AS approved_str, \
                    CAST(order_delivered_carrier_date AS CHAR) AS carrier_str, \
                    CAST(order_delivered_customer_date AS CHAR) AS delivered_str, \
                    CAST(order_estimated_delivery_date AS CHAR) AS estimated_str \
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
        let order_approved_at = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("approved_str").ok().flatten());
        let order_delivered_carrier_date = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("carrier_str").ok().flatten());
        let order_delivered_customer_date = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("delivered_str").ok().flatten());
        let order_estimated_delivery_date = parse_opt_tidb_dt(row.try_get::<Option<String>, _>("estimated_str").ok().flatten());

        let (items, payments, review, customer) = tokio::join!(
            self.fetch_olist_items(&order_id),
            self.fetch_olist_payments(&order_id),
            self.fetch_olist_review(&order_id),
            self.fetch_olist_customer(&customer_id),
        );
        let total_amount_cents: i64 = if !payments.is_empty() {
            payments.iter().map(|p| p.payment_value_cents).sum()
        } else {
            items
                .iter()
                .map(|i| i.price_cents + i.freight_cents.unwrap_or(0))
                .sum()
        };
        let (customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) =
            match customer {
                Some((uid, zip, city, state)) => (Some(uid), Some(zip), Some(city), Some(state)),
                None => (None, None, None, None),
            };

        Ok(Some(OrderDetails {
            order_id,
            customer_id,
            status,
            total_amount_cents,
            currency: "BRL".to_string(),
            items,
            created_at,
            updated_at: created_at,
            order_purchase_timestamp: Some(created_at),
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            courtesy_credit_cents: Some(0),
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            payments,
            review,
        }))
    }

    async fn olist_table_stats(&self) -> Result<OlistTablesResponse, QueryEngineError> {
        let mut tables = Vec::with_capacity(TIDB_OLIST_TABLE_METADATA.len());
        let mut total_rows: i64 = 0;

        for &(table_name, role, ingestion_mode) in TIDB_OLIST_TABLE_METADATA {
            let sql = format!("SELECT COUNT(*) FROM `{table_name}`");
            let count = sqlx::query_scalar::<_, i64>(&sql)
                .fetch_one(&self.pool)
                .await
                .unwrap_or(0);
            total_rows += count;
            tables.push(OlistTableStat {
                table_name: table_name.to_string(),
                role: role.to_string(),
                ingestion_mode: ingestion_mode.to_string(),
                row_count: count,
                status: if count > 0 { "synced" } else { "empty" }.to_string(),
            });
        }

        Ok(OlistTablesResponse {
            storage_engine: "tidb".to_string(),
            tables,
            total_rows,
        })
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
