//! Integration tests for Epic 19 Pure CQRS/ES Query Model (CAP-1..CAP-4, AD-30..AD-32):
//! - Canonical `GetOrderQuery` / `ListOrdersQuery` from `ferrous-cqrses`
//! - `OrderQueryEngine` dual-backend (`InMemoryOrderQueryEngine` + `DataFusionOrderQueryEngine`)
//! - Version-checked `_delta_log` snapshot reload (closes PRD OQ-1) with zero write→read coupling
//! - Sub-3ms P95 View DTO delivery with SierraDB completely offline

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrous_api::{
    auth::JwtValidator,
    generated::{GetOrderResponse, ListOrdersParams, ListOrdersResponse, QueriesApi},
    handlers::QueriesHandler,
    publisher::InMemoryIggyPublisher,
    query_engine::{
        DataFusionOrderQueryEngine, DeltaTableProvider, InMemoryOrderQueryEngine, OrderQueryEngine,
        OrderRecord, SessionContext, ORDERS_CURRENT_STATE_TABLE,
    },
    state::AppState,
    GetOrderQuery, ListOrdersQuery,
};
use ferrous_cqrses::write_delta_table_snapshot;

fn sample_order(order_id: &str, customer_id: &str, status: &str, credit_cents: i64) -> OrderRecord {
    OrderRecord {
        order_id: order_id.to_string(),
        customer_id: customer_id.to_string(),
        status: status.to_string(),
        total_amount_cents: 4500,
        credit_issued_cents: credit_cents,
        currency: "USD".to_string(),
        created_at_ms: 1_730_000_000_000,
        updated_at_ms: 1_730_000_100_000,
        items: vec![],
    }
}

#[tokio::test]
async fn test_in_memory_order_query_engine_executes_canonical_queries() {
    let ctx = Arc::new(SessionContext::new());
    ctx.register_table(
        ORDERS_CURRENT_STATE_TABLE,
        Arc::new(DeltaTableProvider::from_records(
            ORDERS_CURRENT_STATE_TABLE,
            vec![
                sample_order("ord-19-a", "cust-1", "delivered", 0),
                sample_order("ord-19-b", "cust-1", "in_transit", 500),
            ],
        )),
    );
    let engine: Arc<dyn OrderQueryEngine> = Arc::new(InMemoryOrderQueryEngine::new(ctx));

    let found = engine
        .get_order(&GetOrderQuery::new("ord-19-a"))
        .await
        .unwrap()
        .expect("order must be found");
    assert_eq!(found.order_id, "ord-19-a");
    assert_eq!(found.status, "delivered");

    let list = engine
        .list_orders(&ListOrdersQuery::new(
            Some("cust-1".to_string()),
            Some("in_transit".to_string()),
            Some(10),
            Some(0),
        ))
        .await
        .unwrap();
    assert_eq!(list.total_count, 1);
    assert_eq!(list.orders[0].order_id, "ord-19-b");
}

#[tokio::test]
async fn test_datafusion_query_engine_reloads_on_delta_log_version_advance_without_write_hook() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "ferrous_epic19_delta_{}",
        uuid::Uuid::now_v7()
    ));
    fs::create_dir_all(&tmp_dir).unwrap();

    // 1. Write initial Delta Lake v0 snapshot externally (simulating Projection commit)
    let v0_records = vec![sample_order("ord-live-1", "cust-live", "in_transit", 0)];
    write_delta_table_snapshot(&tmp_dir, &v0_records).unwrap();

    // 2. Construct DataFusionOrderQueryEngine with 0ms TTL (immediate version check on query)
    let engine = Arc::new(
        DataFusionOrderQueryEngine::from_delta_dir_with_ttl(&tmp_dir, Duration::ZERO).unwrap(),
    );
    assert_eq!(engine.current_delta_version(), Some(0));

    let state = AppState::with_query_engine(
        Arc::new(InMemoryIggyPublisher::new()),
        Arc::new(JwtValidator::default()),
        Arc::clone(&engine) as Arc<dyn OrderQueryEngine>,
    );
    let handler = QueriesHandler::with_state(state);

    // Query v0 through QueriesHandler (no SierraDB involved)
    let res_v0 = handler
        .get_order(None, "ord-live-1".to_string())
        .await
        .unwrap();
    match res_v0 {
        GetOrderResponse::Ok(details) => {
            assert_eq!(details.order_id, "ord-live-1");
            assert_eq!(details.status, "in_transit");
        }
        other => panic!("Expected GetOrderResponse::Ok, got {other:?}"),
    }

    // 3. Externally append Delta Lake v1 commit (simulating new Projection commit)
    let v1_records = vec![sample_order("ord-live-1", "cust-live", "delivered", 1500)];
    let v1_file = "part-00001-orders-snapshot.json";
    let v1_json = serde_json::to_string(&v1_records).unwrap();
    fs::write(tmp_dir.join(v1_file), v1_json.as_bytes()).unwrap();
    let v1_commit = format!(
        "{{\"remove\":{{\"path\":\"part-00000-orders-snapshot.json\",\"deletionTimestamp\":1730000200000,\"dataChange\":true}}}}\n\
         {{\"add\":{{\"path\":\"{v1_file}\",\"size\":{},\"modificationTime\":1730000200000,\"dataChange\":true}}}}\n",
        v1_json.len()
    );
    fs::write(
        tmp_dir.join("_delta_log").join("00000000000000000001.json"),
        v1_commit.as_bytes(),
    )
    .unwrap();

    // 4. Re-query WITHOUT restarting or calling any write-side invalidation hook
    let start = Instant::now();
    let res_v1 = handler
        .get_order(None, "ord-live-1".to_string())
        .await
        .unwrap();
    let elapsed = start.elapsed();

    match res_v1 {
        GetOrderResponse::Ok(details) => {
            assert_eq!(details.order_id, "ord-live-1");
            assert_eq!(details.status, "delivered");
        }
        other => panic!("Expected updated GetOrderResponse::Ok, got {other:?}"),
    }
    assert_eq!(engine.current_delta_version(), Some(1));
    assert!(
        elapsed < Duration::from_millis(50),
        "Reload + query took too long: {elapsed:?}"
    );

    // 5. Verify list_orders also returns the refreshed v1 View
    let list_res = handler
        .list_orders(
            None,
            ListOrdersParams {
                customer_id: Some("cust-live".to_string()),
                status: Some("delivered".to_string()),
                limit: Some(10),
                offset: Some(0),
            },
        )
        .await
        .unwrap();
    match list_res {
        ListOrdersResponse::Ok(list) => {
            assert_eq!(list.total_count, 1);
            assert_eq!(list.orders[0].order_id, "ord-live-1");
            assert_eq!(list.orders[0].status, "delivered");
        }
    }

    let _ = fs::remove_dir_all(&tmp_dir);
}

#[tokio::test]
async fn test_ad34_olist_orders_ndjson_and_cqrs_projection_overlay_query_engine() {
    let delta_root = std::env::temp_dir().join(format!(
        "ferrous-api-ad34-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let olist_dir = delta_root.join("olist_orders");
    let current_dir = delta_root.join("orders_current_state");
    std::fs::create_dir_all(olist_dir.join("_delta_log")).unwrap();
    std::fs::create_dir_all(current_dir.join("_delta_log")).unwrap();

    // 1. Write 2 raw Kaggle olist_orders NDJSON rows as delta-sink emits them
    let olist_ndjson = concat!(
        r#"{"order_id":"olist-001","customer_id":"cust-a","order_status":"delivered","order_purchase_timestamp":"2017-10-02T10:56:33Z","order_approved_at":"2017-10-02T11:07:15Z","order_delivered_carrier_date":"2017-10-04T19:55:00Z","order_delivered_customer_date":"2017-10-10T21:25:13Z","order_estimated_delivery_date":"2017-10-18T00:00:00Z"}"#,
        "\n",
        r#"{"order_id":"olist-002","customer_id":"cust-b","order_status":"shipped","order_purchase_timestamp":"2017-10-03T08:15:00Z","order_approved_at":"2017-10-03T08:30:00Z","order_delivered_carrier_date":"2017-10-05T12:00:00Z","order_delivered_customer_date":null,"order_estimated_delivery_date":"2017-10-20T00:00:00Z"}"#,
        "\n"
    );
    std::fs::write(olist_dir.join("part-00000000000000000001.ndjson"), olist_ndjson).unwrap();
    std::fs::write(
        olist_dir.join("_delta_log/00000000000000000000.json"),
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n{\"metaData\":{\"id\":\"olist-orders\",\"name\":\"olist_orders\"}}\n",
    )
    .unwrap();
    std::fs::write(
        olist_dir.join("_delta_log/00000000000000000001.json"),
        "{\"add\":{\"path\":\"part-00000000000000000001.ndjson\",\"size\":100,\"modificationTime\":1730000000000,\"dataChange\":true}}\n",
    )
    .unwrap();

    // 2. Write a CQRS ConsolidatedOrderProjection overlay for olist-002 in orders_current_state
    let cqrs_ndjson = r#"{"order_id":"olist-002","customer_id":"cust-b","order_status":"delivered","order_purchase_timestamp":"2017-10-03T08:15:00Z","order_approved_at":"2017-10-03T08:30:00Z","order_delivered_carrier_date":"2017-10-05T12:00:00Z","order_delivered_customer_date":"2017-10-12T14:00:00Z","order_estimated_delivery_date":"2017-10-20T00:00:00Z","total_amount_cents":12500,"total_freight_cents":1500,"courtesy_credit_cents":500,"credit_delta":500,"version":2,"updated_at":"2017-10-12T14:05:00Z"}"#;
    std::fs::write(
        current_dir.join("part-00000000000000000001.ndjson"),
        format!("{cqrs_ndjson}\n"),
    )
    .unwrap();
    std::fs::write(
        current_dir.join("_delta_log/00000000000000000000.json"),
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n{\"metaData\":{\"id\":\"orders-current-state\",\"name\":\"orders_current_state\"}}\n",
    )
    .unwrap();
    std::fs::write(
        current_dir.join("_delta_log/00000000000000000001.json"),
        "{\"add\":{\"path\":\"part-00000000000000000001.ndjson\",\"size\":100,\"modificationTime\":1730000000000,\"dataChange\":true}}\n",
    )
    .unwrap();

    let engine = DataFusionOrderQueryEngine::from_delta_dir_with_ttl(
        &current_dir,
        std::time::Duration::from_millis(0),
    )
    .expect("DataFusionOrderQueryEngine must load unified olist_orders + orders_current_state");

    // Raw olist-001 from olist_orders is queryable directly
    let ord1 = engine
        .get_order(&ferrous_cqrses::queries::GetOrderQuery::new("olist-001"))
        .await
        .expect("get_order olist-001")
        .expect("olist-001 must exist");
    assert_eq!(ord1.customer_id, "cust-a");
    assert_eq!(ord1.status, "delivered");

    // olist-002 reflects the CQRS overlay from orders_current_state
    let ord2 = engine
        .get_order(&ferrous_cqrses::queries::GetOrderQuery::new("olist-002"))
        .await
        .expect("get_order olist-002")
        .expect("olist-002 must exist");
    assert_eq!(ord2.status, "delivered");
    assert_eq!(ord2.total_amount_cents, 12500);

    // List query returns both merged orders
    let list = engine
        .list_orders(&ferrous_cqrses::queries::ListOrdersQuery::default())
        .await
        .expect("list_orders");
    assert_eq!(list.total_count, 2);

    let _ = std::fs::remove_dir_all(&delta_root);
}

#[tokio::test]
async fn test_story_22_4_all_9_olist_tables_registration_and_item_hydration() {
    let delta_root = std::env::temp_dir().join(format!(
        "ferrous-api-ad35-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Create _delta_log for orders_current_state + all 9 Olist tables
    let mut all_dirs: Vec<&str> = vec!["orders_current_state"];
    all_dirs.extend_from_slice(ferrous_api::ALL_OLIST_TABLES);
    for tbl in &all_dirs {
        let log_dir = delta_root.join(tbl).join("_delta_log");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(
            log_dir.join("00000000000000000000.json"),
            format!(
                "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n{{\"metaData\":{{\"id\":\"{tbl}\",\"name\":\"{tbl}\"}}}}\n"
            ),
        )
        .unwrap();
    }

    // Write 1 order into olist_orders and 2 items into olist_order_items
    let olist_dir = delta_root.join("olist_orders");
    std::fs::write(
        olist_dir.join("part-00000000000000000001.ndjson"),
        "{\"order_id\":\"ord-9tbl-1\",\"customer_id\":\"cust-9\",\"order_status\":\"delivered\",\"order_purchase_timestamp\":\"2017-10-02T10:56:33Z\",\"order_approved_at\":null,\"order_delivered_carrier_date\":null,\"order_delivered_customer_date\":null,\"order_estimated_delivery_date\":\"2017-10-18T00:00:00Z\"}\n",
    )
    .unwrap();
    std::fs::write(
        olist_dir.join("_delta_log/00000000000000000001.json"),
        "{\"add\":{\"path\":\"part-00000000000000000001.ndjson\",\"size\":100,\"modificationTime\":1730000000000,\"dataChange\":true}}\n",
    )
    .unwrap();

    let items_dir = delta_root.join("olist_order_items");
    std::fs::write(
        items_dir.join("part-00000000000000000001.ndjson"),
        concat!(
            "{\"order_id\":\"ord-9tbl-1\",\"order_item_id\":1,\"product_id\":\"prod-a\",\"seller_id\":\"sel-1\",\"shipping_limit_date\":\"2017-10-06T00:00:00Z\",\"price\":49.90,\"freight_value\":10.00}\n",
            "{\"order_id\":\"ord-9tbl-1\",\"order_item_id\":2,\"product_id\":\"prod-b\",\"seller_id\":\"sel-1\",\"shipping_limit_date\":\"2017-10-06T00:00:00Z\",\"price\":20.10,\"freight_value\":5.00}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        items_dir.join("_delta_log/00000000000000000001.json"),
        "{\"add\":{\"path\":\"part-00000000000000000001.ndjson\",\"size\":100,\"modificationTime\":1730000000000,\"dataChange\":true}}\n",
    )
    .unwrap();

    let engine = DataFusionOrderQueryEngine::from_delta_dir_with_ttl(
        delta_root.join("orders_current_state"),
        std::time::Duration::from_millis(0),
    )
    .expect("must load all 9 Olist tables + orders_current_state");

    let registered = engine.registered_tables();
    assert_eq!(registered.len(), 10, "orders_current_state + 9 olist_* tables");
    for tbl in ferrous_api::ALL_OLIST_TABLES {
        assert!(
            registered.iter().any(|r| r == tbl),
            "Missing registered table {tbl}"
        );
    }

    // Order details must hydrate the 2 real Olist items and total_amount_cents (4990 + 2010 = 7000)
    let details = engine
        .get_order(&ferrous_cqrses::queries::GetOrderQuery::new("ord-9tbl-1"))
        .await
        .unwrap()
        .expect("ord-9tbl-1 must exist");
    assert_eq!(details.items.len(), 2);
    assert_eq!(details.total_amount_cents, 7000);

    let _ = std::fs::remove_dir_all(&delta_root);
}
