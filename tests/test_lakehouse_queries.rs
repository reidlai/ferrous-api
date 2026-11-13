//! Integration and <3ms SLA verification tests for embedded in-process Apache DataFusion
//! (`Arc<SessionContext>`), Delta Lake `_delta_log` registration, vectorized 100,000-record
//! scans/aggregations, and `TableProvider` federation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use ferrous_api::{
    create_router_with_app_state,
    generated::models::{ListOrdersParams, OrderDetails, OrderListResponse, ProblemDetails},
    lakehouse::{
        write_delta_table_snapshot, DeltaTableProvider, LakehouseError, OperationalDisputeRecord,
        OrderRecord, SessionContext, SqlQueryResult, OPERATIONAL_DISPUTES_TABLE,
        ORDERS_CURRENT_STATE_TABLE,
    },
    state::AppState,
};
use http_body_util::BodyExt;
use tower::ServiceExt;
use uuid::Uuid;

/// Computes the p95 duration from a slice of elapsed measurements.
fn percentile_95(mut samples: Vec<Duration>) -> Duration {
    assert!(!samples.is_empty(), "samples must not be empty");
    samples.sort_unstable();
    let idx = ((samples.len() * 95) / 100).min(samples.len() - 1);
    samples[idx]
}

#[tokio::test]
async fn test_order_lookup_over_100k_records_under_3ms_p95() {
    let ctx = Arc::new(SessionContext::new_with_generated_orders(100_000));
    let (state, _pub, _val) = AppState::with_in_memory_and_session(Arc::clone(&ctx));
    let app = create_router_with_app_state(state);

    // Verify end-to-end HTTP GET /api/v1/orders/{order_id} over the 100k dataset
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/ord-054321")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let details: OrderDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(details.order_id, "ord-054321");
    assert_eq!(details.currency, "USD");

    // Measure p95 latency across 40 end-to-end HTTP GET /api/v1/orders/{order_id} requests
    let mut http_durations = Vec::with_capacity(40);
    for i in 0..40 {
        let target_id = format!("ord-{:06}", (i * 997) % 99_999 + 1);
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/orders/{target_id}"))
            .body(Body::empty())
            .unwrap();
        let start = Instant::now();
        let resp = app.clone().oneshot(req).await.unwrap();
        http_durations.push(start.elapsed());
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let http_p95 = percentile_95(http_durations);
    assert!(
        http_p95 < Duration::from_millis(3),
        "100k HTTP point lookup p95 SLA (<3ms) exceeded: {:?}",
        http_p95
    );
}

#[tokio::test]
async fn test_order_lookup_not_found_and_error_mappings_return_rfc7807() {
    let ctx = Arc::new(SessionContext::new_with_generated_orders(100_000));
    let (state, _pub, _val) = AppState::with_in_memory_and_session(ctx);
    let app = create_router_with_app_state(state);

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/ord-not-found")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(problem.status, 404);
    assert_eq!(problem.r#type, "urn:ferrouscx:error:not_found");
    assert_eq!(
        problem.instance.as_deref(),
        Some("/api/v1/orders/ord-not-found")
    );

    // Whitespace-only order_id must trigger LakehouseError::InvalidQuery -> HTTP 400 Bad Request
    let blank_req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/%20")
        .body(Body::empty())
        .unwrap();
    let blank_resp = app.oneshot(blank_req).await.unwrap();
    assert_eq!(blank_resp.status(), StatusCode::BAD_REQUEST);
    let blank_bytes = blank_resp.into_body().collect().await.unwrap().to_bytes();
    let blank_problem: ProblemDetails = serde_json::from_slice(&blank_bytes).unwrap();
    assert_eq!(blank_problem.status, 400);

    // Empty SessionContext without orders_current_state must map TableNotFound -> HTTP 404 Not Found
    let empty_ctx = Arc::new(SessionContext::new());
    let (empty_state, _pub2, _val2) = AppState::with_in_memory_and_session(Arc::clone(&empty_ctx));
    let empty_app = create_router_with_app_state(empty_state);
    let list_req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders?limit=10&offset=0")
        .body(Body::empty())
        .unwrap();
    let list_resp = empty_app.oneshot(list_req).await.unwrap();
    assert_eq!(list_resp.status(), StatusCode::NOT_FOUND);

    // Direct query_orders_list validation error check
    let invalid_direct = empty_ctx.query_orders_list(&ListOrdersParams {
        customer_id: None,
        status: None,
        limit: Some(0),
        offset: Some(0),
    });
    assert!(matches!(
        invalid_direct,
        Err(LakehouseError::InvalidQuery(_))
    ));
}

#[tokio::test]
async fn test_paginated_and_filtered_order_scan_over_100k_records_under_3ms_p95() {
    let ctx = Arc::new(SessionContext::new_with_generated_orders(100_000));
    let (state, _pub, _val) = AppState::with_in_memory_and_session(Arc::clone(&ctx));
    let app = create_router_with_app_state(state.clone());

    let mut durations = Vec::with_capacity(20);
    for _ in 0..20 {
        let req = Request::builder()
            .method("GET")
            .uri("/api/v1/orders?status=delivered&customer_id=cust-1029&limit=20&offset=0")
            .body(Body::empty())
            .unwrap();

        let start = Instant::now();
        let response = app.clone().oneshot(req).await.unwrap();
        durations.push(start.elapsed());
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let list: OrderListResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(list.limit, 20);
        assert_eq!(list.offset, 0);
        assert_eq!(list.orders.len(), 20);
        assert_eq!(list.total_count, 4_000);
        assert!(list
            .orders
            .iter()
            .all(|o| o.customer_id == "cust-1029" && o.status == "delivered"));
    }

    let p95 = percentile_95(durations);
    assert!(
        p95 < Duration::from_millis(3),
        "100k filtered scan p95 SLA (<3ms) exceeded: {:?}",
        p95
    );

    // Verify invalid pagination is rejected with HTTP 400 Bad Request
    let app_invalid = create_router_with_app_state(state);
    let invalid_req = Request::builder()
        .method("GET")
        .uri("/api/v1/orders?limit=0&offset=-5")
        .body(Body::empty())
        .unwrap();
    let invalid_resp = app_invalid.oneshot(invalid_req).await.unwrap();
    assert_eq!(invalid_resp.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_vectorized_aggregation_and_sql_queries_over_100k_records_under_3ms_p95() {
    let ctx = SessionContext::new_with_generated_orders(100_000);

    let mut durations = Vec::with_capacity(40);
    for _ in 0..40 {
        let start = Instant::now();
        let res = ctx
            .sql(
                "SELECT status, COUNT(1), SUM(total_amount_cents), SUM(credit_issued_cents), AVG(total_amount_cents) \
                 FROM orders_current_state GROUP BY status",
            )
            .unwrap();
        durations.push(start.elapsed());

        match res {
            SqlQueryResult::StatusAggregations(groups) => {
                assert_eq!(groups.len(), 5);
                let total_rows: usize = groups.iter().map(|g| g.order_count).sum();
                assert_eq!(total_rows, 100_000);
                for g in &groups {
                    assert_eq!(g.order_count, 20_000);
                    assert!(g.total_amount_cents_sum > 0);
                }
            }
            other => panic!("Expected StatusAggregations, got {other:?}"),
        }
    }

    let p95 = percentile_95(durations);
    assert!(
        p95 < Duration::from_millis(3),
        "100k vectorized aggregation p95 SLA (<3ms) exceeded: {:?}",
        p95
    );

    // Verify scalar aggregation SQL query with customer_id and status predicates
    let scalar = ctx
        .sql(
            "SELECT COUNT(*), SUM(total_amount_cents) \
             FROM orders_current_state WHERE status = 'delivered' AND customer_id = 'cust-1029'",
        )
        .unwrap();
    match scalar {
        SqlQueryResult::Scalar {
            row_count,
            sum_total_amount_cents,
            ..
        } => {
            assert_eq!(row_count, 4_000);
            assert!(sum_total_amount_cents > 0);
        }
        other => panic!("Expected Scalar result, got {other:?}"),
    }

    // Verify SQL point lookup and filtered/paginated SELECT returning SqlQueryResult::Orders
    let point_res = ctx
        .sql("SELECT * FROM orders_current_state WHERE order_id = 'ord-054321'")
        .unwrap();
    match point_res {
        SqlQueryResult::Orders(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].order_id, "ord-054321");
        }
        other => panic!("Expected Orders result, got {other:?}"),
    }

    let paged_res = ctx
        .sql(
            "SELECT order_id, status FROM orders_current_state \
             WHERE status = 'delivered' AND customer_id = 'cust-1029' LIMIT 5 OFFSET 2",
        )
        .unwrap();
    match paged_res {
        SqlQueryResult::Orders(rows) => {
            assert_eq!(rows.len(), 5);
            assert_eq!(rows[0].order_id, "ord-000050");
        }
        other => panic!("Expected Orders result, got {other:?}"),
    }

    // Verify SQL injection / multi-statement / comment guards and invalid column/table errors
    assert!(matches!(
        ctx.sql("SELECT * FROM orders_current_state; DROP TABLE orders_current_state"),
        Err(LakehouseError::InvalidQuery(_))
    ));
    assert!(matches!(
        ctx.sql("SELECT * FROM orders_current_state -- comment"),
        Err(LakehouseError::InvalidQuery(_))
    ));
    assert!(matches!(
        ctx.sql("SELECT COUNT)( FROM orders_current_state"),
        Err(LakehouseError::InvalidQuery(_))
    ));
    assert!(matches!(
        ctx.sql("SELECT * FROM orders_current_state LIMIT not_a_number"),
        Err(LakehouseError::InvalidQuery(_))
    ));

    let bad_table = ctx.sql("SELECT * FROM nonexistent_table");
    assert_eq!(
        bad_table,
        Err(LakehouseError::TableNotFound(
            "nonexistent_table".to_string()
        ))
    );

    let bad_col = ctx.sql("SELECT unknown_column FROM orders_current_state");
    assert_eq!(
        bad_col,
        Err(LakehouseError::ColumnNotFound {
            table: "orders_current_state".to_string(),
            column: "unknown_column".to_string(),
        })
    );

    let bad_where_col =
        ctx.sql("SELECT * FROM orders_current_state WHERE unknown_filter = 'value'");
    assert_eq!(
        bad_where_col,
        Err(LakehouseError::ColumnNotFound {
            table: "orders_current_state".to_string(),
            column: "unknown_filter".to_string(),
        })
    );
}

#[test]
fn test_federated_join_execution_plan_between_delta_and_operational_tables() {
    let ctx = SessionContext::new_with_generated_orders(100_000);

    // Verify federated join fails cleanly with TableNotFound before operational table registration
    let missing_join =
        ctx.execute_federated_join(ORDERS_CURRENT_STATE_TABLE, OPERATIONAL_DISPUTES_TABLE);
    assert_eq!(
        missing_join,
        Err(LakehouseError::TableNotFound(
            OPERATIONAL_DISPUTES_TABLE.to_string()
        ))
    );

    // Register operational disputes table via TableProvider federation
    let disputes = vec![
        OperationalDisputeRecord {
            dispute_id: "dsp-001".to_string(),
            order_id: "ord-000004".to_string(),
            dispute_reason: "Damaged item on delivery".to_string(),
            dispute_status: "open".to_string(),
            disputed_amount_cents: 1200,
            opened_at_ms: 1_730_000_010_000,
        },
        OperationalDisputeRecord {
            dispute_id: "dsp-002".to_string(),
            order_id: "ord-050004".to_string(),
            dispute_reason: "Late shipment SLA breach".to_string(),
            dispute_status: "escalated".to_string(),
            disputed_amount_cents: 1200,
            opened_at_ms: 1_730_000_020_000,
        },
        OperationalDisputeRecord {
            dispute_id: "dsp-003".to_string(),
            order_id: "ord-999999-missing".to_string(),
            dispute_reason: "Unmatched dispute".to_string(),
            dispute_status: "closed".to_string(),
            disputed_amount_cents: 500,
            opened_at_ms: 1_730_000_030_000,
        },
    ];
    ctx.register_operational_table(OPERATIONAL_DISPUTES_TABLE, disputes);

    let start = Instant::now();
    let sql_res = ctx
        .sql(
            "SELECT * FROM orders_current_state \
             JOIN operational_disputes ON orders_current_state.order_id = operational_disputes.order_id",
        )
        .unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(3),
        "Federated join plan over 100k Delta rows took too long: {:?}",
        elapsed
    );

    match sql_res {
        SqlQueryResult::FederatedJoin(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].order_id, "ord-000004");
            assert_eq!(rows[0].dispute_id, "dsp-001");
            assert_eq!(rows[0].order_status, "disputed");
            assert_eq!(rows[1].order_id, "ord-050004");
            assert_eq!(rows[1].dispute_id, "dsp-002");
            assert_eq!(rows[1].dispute_status, "escalated");
        }
        other => panic!("Expected FederatedJoin result, got {other:?}"),
    }

    // Verify reversed join order (`operational_disputes JOIN orders_current_state`) and LIMIT
    let reversed_res = ctx
        .sql(
            "SELECT * FROM operational_disputes \
             JOIN orders_current_state ON operational_disputes.order_id = orders_current_state.order_id LIMIT 1",
        )
        .unwrap();
    match reversed_res {
        SqlQueryResult::FederatedJoin(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].order_id, "ord-000004");
        }
        other => panic!("Expected FederatedJoin result, got {other:?}"),
    }
}

#[test]
fn test_delta_log_directory_registration_tombstones_and_deduplication() {
    let temp_root = std::env::temp_dir().join(format!("ferrous-delta-test-{}", Uuid::now_v7()));
    let missing_dir = temp_root.join("missing_delta_table");
    std::fs::create_dir_all(&missing_dir).unwrap();

    let ctx = SessionContext::new();

    // Missing _delta_log directory must return LakehouseError::DeltaLogNotFound
    let err = ctx
        .register_delta_table(ORDERS_CURRENT_STATE_TABLE, &missing_dir)
        .unwrap_err();
    assert!(matches!(err, LakehouseError::DeltaLogNotFound(_)));

    // Write valid _delta_log/00000000000000000000.json + snapshot data file
    let valid_dir = temp_root.join("orders_current_state");
    let custom_records = vec![
        OrderRecord {
            order_id: "ord-delta-777".to_string(),
            customer_id: "cust-delta-1".to_string(),
            status: "delivered".to_string(),
            total_amount_cents: 12_501,
            credit_issued_cents: 1_500,
            currency: "USD".to_string(),
            created_at_ms: 1_730_000_000_000,
            updated_at_ms: 1_730_000_005_000,
            items: Vec::new(),
        },
        OrderRecord {
            order_id: "ord-delta-888".to_string(),
            customer_id: "cust-delta-2".to_string(),
            status: "shipped".to_string(),
            total_amount_cents: 6_400,
            credit_issued_cents: 0,
            currency: "USD".to_string(),
            created_at_ms: 1_730_000_010_000,
            updated_at_ms: 1_730_000_015_000,
            items: Vec::new(),
        },
    ];
    let written_meta = write_delta_table_snapshot(&valid_dir, &custom_records).unwrap();
    assert_eq!(written_meta.version, 0);
    assert_eq!(written_meta.commit_files_count, 1);

    // Register Delta directory into SessionContext
    let loaded_meta = ctx
        .register_delta_table(ORDERS_CURRENT_STATE_TABLE, &valid_dir)
        .unwrap();
    assert_eq!(
        loaded_meta.table_id.as_deref(),
        Some("delta-orders-current-state")
    );
    assert_eq!(loaded_meta.min_reader_version, 1);
    assert_eq!(loaded_meta.min_writer_version, 2);
    assert_eq!(loaded_meta.active_files.len(), 1);

    // Query registered Delta table and verify odd-cent item synthesis preserves exact total
    let order = ctx
        .query_order_by_id("ord-delta-777")
        .unwrap()
        .expect("ord-delta-777 should exist in registered Delta table");
    assert_eq!(order.customer_id, "cust-delta-1");
    assert_eq!(order.total_amount_cents, 12_501);
    assert_eq!(
        order.items[0].price_cents * i64::from(order.items[0].quantity),
        12_501
    );

    // Write commit version 1 removing part-00000-orders-snapshot.json and adding part-00001.json
    let v1_file = "part-00001-orders-snapshot.json";
    let v1_records = vec![OrderRecord {
        order_id: "ord-delta-999".to_string(),
        customer_id: "cust-delta-9".to_string(),
        status: "processing".to_string(),
        total_amount_cents: 4_200,
        credit_issued_cents: 0,
        currency: "USD".to_string(),
        created_at_ms: 1_730_000_020_000,
        updated_at_ms: 1_730_000_025_000,
        items: Vec::new(),
    }];
    std::fs::write(
        valid_dir.join(v1_file),
        serde_json::to_string(&v1_records).unwrap(),
    )
    .unwrap();
    let commit_v1 = valid_dir.join("_delta_log/00000000000000000001.json");
    let commit_v1_lines = format!(
        "{{\"remove\":{{\"path\":\"part-00000-orders-snapshot.json\",\"deletionTimestamp\":1730000020000,\"dataChange\":true}}}}\n\
         {{\"add\":{{\"path\":\"{v1_file}\",\"size\":128,\"modificationTime\":1730000020000,\"dataChange\":true}}}}\n"
    );
    std::fs::write(&commit_v1, commit_v1_lines).unwrap();

    let meta_v1 = ctx
        .register_delta_table(ORDERS_CURRENT_STATE_TABLE, &valid_dir)
        .unwrap();
    assert_eq!(meta_v1.version, 1);
    assert_eq!(meta_v1.commit_files_count, 2);
    assert_eq!(meta_v1.active_files, vec![v1_file.to_string()]);
    assert!(ctx.query_order_by_id("ord-delta-777").unwrap().is_none());
    assert!(ctx.query_order_by_id("ord-delta-999").unwrap().is_some());

    // Verify duplicate order_id records in DeltaTableProvider::from_records upsert in place
    let dedup = DeltaTableProvider::from_records(
        ORDERS_CURRENT_STATE_TABLE,
        vec![
            OrderRecord {
                order_id: "ord-dup-1".to_string(),
                customer_id: "cust-1".to_string(),
                status: "processing".to_string(),
                total_amount_cents: 1000,
                credit_issued_cents: 0,
                currency: "USD".to_string(),
                created_at_ms: 1_730_000_000_000,
                updated_at_ms: 1_730_000_000_000,
                items: Vec::new(),
            },
            OrderRecord {
                order_id: "ord-dup-1".to_string(),
                customer_id: "cust-1".to_string(),
                status: "delivered".to_string(),
                total_amount_cents: 1000,
                credit_issued_cents: 200,
                currency: "USD".to_string(),
                created_at_ms: 1_730_000_000_000,
                updated_at_ms: 1_730_000_005_000,
                items: Vec::new(),
            },
        ],
    );
    assert_eq!(dedup.order_ids.len(), 1);
    assert_eq!(
        dedup.get_order_by_id("ord-dup-1").unwrap().status,
        "delivered"
    );
    let groups = dedup.aggregate_by_status();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].status, "delivered");
    assert_eq!(groups[0].order_count, 1);

    let _ = std::fs::remove_dir_all(&temp_root);
}
