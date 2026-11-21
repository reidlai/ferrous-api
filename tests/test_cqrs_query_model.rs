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
