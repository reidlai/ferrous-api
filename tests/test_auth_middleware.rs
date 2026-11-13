//! Integration tests for Kanidm Policy Enforcement Point (PEP) JWT validation middleware,
//! in-memory JWKS cache, scope enforcement (`401`/`403`), and <0.2ms validation SLA.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    routing::post,
    Extension, Json, Router,
};
use chrono::Utc;
use ferrous_api::{
    auth::{
        JwtValidator, RequireScope, DEFAULT_KANIDM_KID, DEFAULT_KANIDM_SECRET, SCOPE_API_READ,
        SCOPE_API_WRITE,
    },
    create_router_with_app_state,
    generated::models::{Claims, CommandAcceptedResponse, ProblemDetails},
    state::AppState,
};
use http_body_util::BodyExt;
use jsonwebtoken::{encode, jwk::JwkSet, Algorithm, DecodingKey, EncodingKey, Header};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn test_missing_authorization_header_returns_401_rfc7807() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": 1500,
        "reason": "Missing auth header test"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 401);
    assert_eq!(problem.r#type, "urn:ferrous:error:unauthorized");
    assert!(
        publisher.is_empty(),
        "Unauthenticated request must never publish to Iggy"
    );
}

#[tokio::test]
async fn test_malformed_authorization_headers_return_401_invalid_token() {
    let (state, publisher, _validator) = AppState::with_in_memory();

    let malformed_headers = [
        "Basic dXNlcjpwYXNz",
        "Bearer",
        "Bearer    ",
        "Token some-random-token",
    ];

    for bad_header in malformed_headers {
        let app = create_router_with_app_state(state.clone());
        let payload = json!({
            "order_id": Uuid::now_v7().to_string(),
            "reason": "Cancel order test"
        });

        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/commands/orders/cancel")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, bad_header)
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Header '{bad_header}' should be rejected with 401"
        );

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(problem.status, 401);
        assert_eq!(problem.r#type, "urn:ferrous:error:invalid-token");
    }

    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_expired_jwt_token_returns_401_in_under_200_micros() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    let expired_claims = Claims::new("svc_ferrous_api")
        .with_scope(SCOPE_API_WRITE)
        .with_exp(Utc::now().timestamp() - 120);
    let expired_token = validator.issue_token(&expired_claims).unwrap();

    // Warm up and measure pure validation latency (< 0.2ms / 200us)
    for _ in 0..5 {
        let _ = validator.validate_token(&expired_token);
    }
    let start = Instant::now();
    let result = validator.validate_token(&expired_token);
    let elapsed = start.elapsed();
    assert!(result.is_err());
    assert!(
        elapsed < Duration::from_micros(200),
        "Expired token validation took {:?}, exceeding 0.2ms SLA",
        elapsed
    );

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": 2000,
        "reason": "Expired token test"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {expired_token}"))
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 401);
    assert_eq!(problem.r#type, "urn:ferrous:error:invalid-token");
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_tampered_jwt_signature_returns_401_invalid_token() {
    let (state, publisher, _validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    // Issue token with a different secret key
    let attacker_validator =
        JwtValidator::new_with_secret(DEFAULT_KANIDM_KID, b"wrong-signing-key-attacker-secret");
    let claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_WRITE);
    let forged_token = attacker_validator.issue_token(&claims).unwrap();

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": 1000,
        "reason": "Forged token attempt"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {forged_token}"))
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 401);
    assert_eq!(problem.r#type, "urn:ferrous:error:invalid-token");
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_insufficient_scope_returns_403_forbidden_rfc7807() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    // Token only has `api:read`, attempting a write command
    let read_only_claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_READ);
    let read_only_token = validator.issue_token(&read_only_claims).unwrap();

    let payload = json!({
        "order_id": Uuid::now_v7().to_string(),
        "amount_cents": 1500,
        "reason": "Read-only token attempting write"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {read_only_token}"))
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.status, 403);
    assert_eq!(problem.r#type, "urn:ferrous:error:forbidden");
    let detail = problem.detail.unwrap_or_default();
    assert!(
        detail.contains("api:write"),
        "403 ProblemDetails detail must note required 'api:write' scope, got: {detail}"
    );
    assert!(publisher.is_empty());
}

#[tokio::test]
async fn test_valid_jwt_injects_claims_extension_and_succeeds() {
    let validator = Arc::new(JwtValidator::default());

    let app = Router::new()
        .route(
            "/protected",
            post(|Extension(claims): Extension<Claims>| async move { Json(claims) }),
        )
        .layer(RequireScope::new(Arc::clone(&validator), SCOPE_API_WRITE));

    let expected_claims = Claims::new("svc_ferrous_api")
        .with_scopes(vec![
            SCOPE_API_READ.to_string(),
            SCOPE_API_WRITE.to_string(),
        ])
        .with_group("kanidm_service_accounts")
        .with_iss("https://idm.ferrouscx.internal");

    let token = validator.issue_token(&expected_claims).unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/protected")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let extracted: Claims = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(extracted.sub, "svc_ferrous_api");
    assert!(extracted.has_scope(SCOPE_API_WRITE));
    assert!(extracted.has_scope(SCOPE_API_READ));
    assert!(extracted.has_group("kanidm_service_accounts"));
    assert_eq!(
        extracted.iss.as_deref(),
        Some("https://idm.ferrouscx.internal")
    );
}

#[tokio::test]
async fn test_jwks_in_memory_cache_lookup_latency_and_ttl_refresh() {
    // Construct RFC 7517 JwkSet with an oct HS256 key
    let jwks_json = json!({
        "keys": [
            {
                "kty": "oct",
                "alg": "HS256",
                "kid": "kanidm-key-2026-q4",
                "k": "ZmVycm91c2N4LWthbmlkbS1qd2tzLXRlc3Qtc2VjcmV0LWtleS0zMmJ5dGVz"
            }
        ]
    });
    let jwks: JwkSet = serde_json::from_value(jwks_json).unwrap();

    // Configure short TTL (10ms) to verify automatic cache refresh
    let validator = JwtValidator::from_jwks(&jwks, Duration::from_millis(10)).unwrap();
    assert!(validator.has_key("kanidm-key-2026-q4"));
    assert_eq!(validator.cached_key_count(), 1);
    assert_eq!(validator.refresh_count(), 0);

    // Also insert a secondary key directly into the Arc<RwLock<HashMap<String, DecodingKey>>>
    let custom_secret = b"secondary-rotated-kanidm-signing-secret-key";
    validator.insert_key(
        "kanidm-rotated-key",
        DecodingKey::from_secret(custom_secret),
    );
    assert_eq!(validator.cached_key_count(), 2);

    let claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_WRITE);
    let encoding_key = EncodingKey::from_secret(custom_secret);
    let token = validator
        .issue_token_with_kid(
            "kanidm-rotated-key",
            &claims,
            &encoding_key,
            Algorithm::HS256,
        )
        .unwrap();

    // Warm up and verify validation latency < 0.2ms (200 microseconds)
    let _ = validator.validate_token(&token).unwrap();
    let start = Instant::now();
    let verified = validator.validate_token(&token).unwrap();
    let elapsed = start.elapsed();

    assert_eq!(verified.sub, "svc_ferrous_api");
    assert!(
        elapsed < Duration::from_micros(200),
        "JWT validation latency {:?} exceeded 0.2ms SLA",
        elapsed
    );

    // Remove the JWK key before TTL expiry to verify refresh_cache restores it from source_jwks
    assert!(validator.remove_key("kanidm-key-2026-q4").is_some());
    assert!(!validator.has_key("kanidm-key-2026-q4"));

    // Wait for TTL to expire and verify automatic refresh restores "kanidm-key-2026-q4" and validates a token signed with it
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(validator.is_cache_expired());

    let jwk_encoding_key =
        EncodingKey::from_secret(b"ferrouscx-kanidm-jwks-test-secret-key-32bytes");
    let jwk_token = validator
        .issue_token_with_kid(
            "kanidm-key-2026-q4",
            &claims,
            &jwk_encoding_key,
            Algorithm::HS256,
        )
        .unwrap();

    let verified_after_refresh = validator.validate_token(&jwk_token).unwrap();
    assert_eq!(verified_after_refresh.sub, "svc_ferrous_api");
    assert!(
        validator.has_key("kanidm-key-2026-q4"),
        "refresh_cache must restore 'kanidm-key-2026-q4' from source_jwks"
    );
    assert!(
        validator.refresh_count() >= 1,
        "JWKS cache should have automatically refreshed after TTL expiration"
    );
}

#[tokio::test]
async fn test_non_prefix_route_validates_optional_bearer_token() {
    let (state, _publisher, validator) = AppState::with_in_memory();

    // 1. Invalid Bearer token on GET /api/v1/orders/ord-98234-x returns 401 ProblemDetails
    let app_invalid = create_router_with_app_state(state.clone());
    let req_invalid = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/ord-98234-x")
        .header(header::AUTHORIZATION, "Bearer tampered.invalid.token")
        .body(Body::empty())
        .unwrap();

    let res_invalid = app_invalid.oneshot(req_invalid).await.unwrap();
    assert_eq!(res_invalid.status(), StatusCode::UNAUTHORIZED);
    let problem_bytes = res_invalid.into_body().collect().await.unwrap().to_bytes();
    let problem: ProblemDetails = serde_json::from_slice(&problem_bytes).unwrap();
    assert_eq!(problem.status, 401);
    assert_eq!(problem.r#type, "urn:ferrous:error:invalid-token");

    // 2. Valid Bearer token on GET /api/v1/orders/ord-98234-x returns 200 OK
    let valid_claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_READ);
    let valid_token = validator.issue_token(&valid_claims).unwrap();

    let app_valid = create_router_with_app_state(state);
    let req_valid = Request::builder()
        .method("GET")
        .uri("/api/v1/orders/ord-98234-x")
        .header(header::AUTHORIZATION, format!("Bearer {valid_token}"))
        .body(Body::empty())
        .unwrap();

    let res_valid = app_valid.oneshot(req_valid).await.unwrap();
    assert_eq!(res_valid.status(), StatusCode::OK);
}

#[test]
fn test_oauth2_space_delimited_scope_claim_is_normalized() {
    let validator = JwtValidator::default();
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(DEFAULT_KANIDM_KID.to_string());

    let oauth2_payload = json!({
        "sub": "svc_ferrous_api",
        "scope": "api:read api:write",
        "exp": Utc::now().timestamp() + 3600
    });

    let token = encode(
        &header,
        &oauth2_payload,
        &EncodingKey::from_secret(DEFAULT_KANIDM_SECRET),
    )
    .unwrap();

    let auth_header = format!("BEARER {token}");
    let claims = validator
        .validate_and_require_scope(Some(&auth_header), SCOPE_API_WRITE)
        .unwrap();
    assert_eq!(claims.sub, "svc_ferrous_api");
    assert!(claims.has_scope(SCOPE_API_READ));
    assert!(claims.has_scope(SCOPE_API_WRITE));

    // Also verify "BearerXYZ" without whitespace separator is rejected as malformed
    assert!(validator
        .validate_authorization_header(Some(&format!("BearerXYZ{token}")))
        .is_err());
}

#[tokio::test]
async fn test_end_to_end_authenticated_credit_command_passes_pep() {
    let (state, publisher, validator) = AppState::with_in_memory();
    let app = create_router_with_app_state(state);

    let claims = Claims::new("svc_ferrous_api").with_scope(SCOPE_API_WRITE);
    let token = validator.issue_token(&claims).unwrap();
    let order_id = Uuid::now_v7().to_string();

    let payload = json!({
        "order_id": order_id,
        "amount_cents": 1200,
        "reason": "Authorized courtesy credit"
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/commands/orders/issue-credit")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(payload.to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let accepted: CommandAcceptedResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(accepted.status, "accepted");
    assert_eq!(publisher.len(), 1);
}
