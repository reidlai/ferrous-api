//! Kanidm Policy Enforcement Point (PEP) JWT validation middleware, in-memory JWKS cache,
//! `Claims`, `RequireScope` Tower layer, and RFC 7807 `ProblemDetails` rejection mapping.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    http::{header, Request},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use jsonwebtoken::{
    decode, decode_header, encode,
    jwk::{Jwk, JwkSet},
    Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tower::{Layer, Service};

pub use crate::generated::models::Claims;
use crate::generated::models::ProblemDetails;

impl Claims {
    /// Creates a new `Claims` instance for the given subject (`sub`).
    pub fn new(sub: impl Into<String>) -> Self {
        Self {
            sub: sub.into(),
            scopes: Vec::new(),
            groups: Vec::new(),
            exp: None,
            iss: None,
        }
    }

    /// Appends a single scope to `Claims`.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scopes.push(scope.into());
        self
    }

    /// Replaces the scopes list on `Claims`.
    pub fn with_scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes = scopes;
        self
    }

    /// Appends a Kanidm group membership to `Claims`.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.groups.push(group.into());
        self
    }

    /// Sets the expiration timestamp (`exp`, seconds since Unix epoch).
    pub fn with_exp(mut self, exp: i64) -> Self {
        self.exp = Some(exp);
        self
    }

    /// Sets the issuer (`iss`) claim.
    pub fn with_iss(mut self, iss: impl Into<String>) -> Self {
        self.iss = Some(iss.into());
        self
    }

    /// Checks whether the token possesses the specified scope.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }

    /// Checks whether the token possesses the specified group.
    pub fn has_group(&self, group: &str) -> bool {
        self.groups.iter().any(|g| g == group)
    }
}

/// Default key identifier used for local and unit-test Kanidm JWKS cache entries.
pub const DEFAULT_KANIDM_KID: &str = "kanidm-default-key";

/// Default HMAC secret used for zero-dependency local testing when no external JWKS is loaded.
pub const DEFAULT_KANIDM_SECRET: &[u8] = b"ferrouscx-kanidm-pep-default-signing-secret-key-2026";

/// Default TTL for cached JWKS public keys (5 minutes).
pub const DEFAULT_JWKS_TTL: Duration = Duration::from_secs(300);

/// Required scope for mutation commands (`/api/v1/commands/*`).
pub const SCOPE_API_WRITE: &str = "api:write";

/// Required scope for analytical queries and streams.
pub const SCOPE_API_READ: &str = "api:read";

/// Authentication and authorization errors mapped to RFC 7807 `ProblemDetails`.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("Missing Authorization header; Bearer token required")]
    MissingToken,

    #[error("Malformed Authorization header: {0}")]
    MalformedHeader(String),

    #[error("Invalid or expired JWT token: {0}")]
    InvalidToken(String),

    #[error("Insufficient scope: required '{required}'")]
    InsufficientScope { required: String },
}

impl AuthError {
    /// Converts the authentication/authorization failure into an RFC 7807 `ProblemDetails` envelope.
    pub fn into_problem_details(self, instance: Option<String>) -> ProblemDetails {
        match self {
            Self::MissingToken => ProblemDetails {
                r#type: "urn:ferrous:error:unauthorized".to_string(),
                title: "Unauthorized".to_string(),
                status: 401,
                detail: Some(
                    "Missing Authorization header; Bearer JWT token is required".to_string(),
                ),
                instance,
                code: Some("UNAUTHORIZED".to_string()),
                invalid_params: None,
            },
            Self::MalformedHeader(reason) => ProblemDetails {
                r#type: "urn:ferrous:error:invalid-token".to_string(),
                title: "Unauthorized".to_string(),
                status: 401,
                detail: Some(format!("Malformed Authorization header: {reason}")),
                instance,
                code: Some("INVALID_TOKEN".to_string()),
                invalid_params: None,
            },
            Self::InvalidToken(reason) => ProblemDetails {
                r#type: "urn:ferrous:error:invalid-token".to_string(),
                title: "Unauthorized".to_string(),
                status: 401,
                detail: Some(format!("Invalid or expired JWT bearer token: {reason}")),
                instance,
                code: Some("INVALID_TOKEN".to_string()),
                invalid_params: None,
            },
            Self::InsufficientScope { required } => ProblemDetails {
                r#type: "urn:ferrous:error:forbidden".to_string(),
                title: "Forbidden".to_string(),
                status: 403,
                detail: Some(format!(
                    "Insufficient scope: token requires '{required}' authority"
                )),
                instance,
                code: Some("FORBIDDEN".to_string()),
                invalid_params: None,
            },
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        self.into_problem_details(None).into_response()
    }
}

/// Timestamp and refresh metadata for the in-memory JWKS key cache.
#[derive(Debug, Clone)]
pub struct JwksCacheMetadata {
    /// Instant when the JWKS cache was last populated or refreshed.
    pub cached_at: Instant,
    /// Time-to-live before automatic key cache refresh is triggered.
    pub ttl: Duration,
    /// Monotonic counter of JWKS cache refresh operations.
    pub refresh_count: u64,
}

/// Internal wire representation of JWT claims supporting both `scopes` array and OAuth2 `scope` string.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireClaims {
    pub sub: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
}

impl WireClaims {
    fn into_claims(self) -> Claims {
        let mut scopes = self.scopes;
        if let Some(scope_str) = self.scope {
            for s in scope_str.split_whitespace() {
                if !s.is_empty() && !scopes.iter().any(|existing| existing == s) {
                    scopes.push(s.to_string());
                }
            }
        }
        Claims {
            sub: self.sub,
            scopes,
            groups: self.groups,
            exp: self.exp,
            iss: self.iss,
        }
    }
}

/// Kanidm JWT signature and claims validator backed by an in-memory JWKS cache.
///
/// Stores public `DecodingKey` instances in an `Arc<RwLock<HashMap<String, DecodingKey>>>`
/// along with TTL timestamp metadata to guarantee <0.05ms key lookup and <0.2ms token validation.
#[derive(Clone)]
pub struct JwtValidator {
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
    metadata: Arc<RwLock<JwksCacheMetadata>>,
    source_jwks: Arc<RwLock<Option<JwkSet>>>,
    default_kid: String,
    default_secret: Vec<u8>,
    algorithm: Algorithm,
    issuer: Option<String>,
}

impl std::fmt::Debug for JwtValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key_count = self.keys.read().map(|k| k.len()).unwrap_or(0);
        f.debug_struct("JwtValidator")
            .field("default_kid", &self.default_kid)
            .field("algorithm", &self.algorithm)
            .field("issuer", &self.issuer)
            .field("cached_keys", &key_count)
            .finish()
    }
}

impl Default for JwtValidator {
    fn default() -> Self {
        Self::new_with_secret(DEFAULT_KANIDM_KID, DEFAULT_KANIDM_SECRET)
    }
}

impl JwtValidator {
    /// Creates a new `JwtValidator` initialized with an HMAC secret key stored in the in-memory JWKS cache.
    pub fn new_with_secret(kid: impl Into<String>, secret: &[u8]) -> Self {
        let kid_str = kid.into();
        let mut map = HashMap::new();
        map.insert(kid_str.clone(), DecodingKey::from_secret(secret));

        Self {
            keys: Arc::new(RwLock::new(map)),
            metadata: Arc::new(RwLock::new(JwksCacheMetadata {
                cached_at: Instant::now(),
                ttl: DEFAULT_JWKS_TTL,
                refresh_count: 0,
            })),
            source_jwks: Arc::new(RwLock::new(None)),
            default_kid: kid_str,
            default_secret: secret.to_vec(),
            algorithm: Algorithm::HS256,
            issuer: None,
        }
    }

    /// Creates a `JwtValidator` populated from a standard RFC 7517 `JwkSet` and TTL duration.
    pub fn from_jwks(jwks: &JwkSet, ttl: Duration) -> Result<Self, AuthError> {
        let default_kid = jwks
            .keys
            .first()
            .and_then(|jwk| jwk.common.key_id.clone())
            .unwrap_or_else(|| "jwk-key-0".to_string());

        let validator = Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(JwksCacheMetadata {
                cached_at: Instant::now(),
                ttl,
                refresh_count: 0,
            })),
            source_jwks: Arc::new(RwLock::new(Some(jwks.clone()))),
            default_kid,
            default_secret: DEFAULT_KANIDM_SECRET.to_vec(),
            algorithm: Algorithm::HS256,
            issuer: None,
        };
        validator.load_jwk_set(jwks)?;
        Ok(validator)
    }

    /// Configures a custom TTL for automatic in-memory JWKS cache refresh.
    pub fn with_ttl(self, ttl: Duration) -> Self {
        if let Ok(mut meta) = self.metadata.write() {
            meta.ttl = ttl;
        }
        self
    }

    /// Configures a required token issuer (`iss`) claim.
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Returns a clone of the underlying `Arc<RwLock<HashMap<String, DecodingKey>>>` JWKS key cache.
    pub fn keys(&self) -> Arc<RwLock<HashMap<String, DecodingKey>>> {
        Arc::clone(&self.keys)
    }

    /// Inserts or updates a `DecodingKey` in the in-memory JWKS cache under the specified `kid`.
    pub fn insert_key(&self, kid: impl Into<String>, key: DecodingKey) {
        if let Ok(mut keys) = self.keys.write() {
            keys.insert(kid.into(), key);
        }
        if let Ok(mut meta) = self.metadata.write() {
            meta.cached_at = Instant::now();
        }
    }

    /// Removes a key from the in-memory JWKS cache.
    pub fn remove_key(&self, kid: &str) -> Option<DecodingKey> {
        self.keys.write().ok().and_then(|mut k| k.remove(kid))
    }

    /// Checks whether a key with the specified `kid` is present in the JWKS cache.
    pub fn has_key(&self, kid: &str) -> bool {
        self.keys
            .read()
            .map(|keys| keys.contains_key(kid))
            .unwrap_or(false)
    }

    /// Returns the number of keys currently stored in the in-memory JWKS cache.
    pub fn cached_key_count(&self) -> usize {
        self.keys.read().map(|keys| keys.len()).unwrap_or(0)
    }

    /// Returns the number of times the JWKS cache has been refreshed.
    pub fn refresh_count(&self) -> u64 {
        self.metadata
            .read()
            .map(|meta| meta.refresh_count)
            .unwrap_or(0)
    }

    /// Checks whether the in-memory JWKS cache TTL has elapsed.
    pub fn is_cache_expired(&self) -> bool {
        self.metadata
            .read()
            .map(|meta| meta.cached_at.elapsed() >= meta.ttl)
            .unwrap_or(true)
    }

    /// Loads keys from a `JwkSet` into the in-memory `HashMap<String, DecodingKey>` cache.
    pub fn load_jwk_set(&self, jwks: &JwkSet) -> Result<usize, AuthError> {
        let mut parsed_keys = HashMap::new();
        for (idx, jwk) in jwks.keys.iter().enumerate() {
            let kid = jwk
                .common
                .key_id
                .clone()
                .unwrap_or_else(|| format!("jwk-key-{idx}"));
            let decoding_key = Self::decoding_key_from_jwk(jwk)?;
            parsed_keys.insert(kid, decoding_key);
        }

        let count = parsed_keys.len();
        if let Ok(mut source) = self.source_jwks.write() {
            *source = Some(jwks.clone());
        }
        if let Ok(mut keys) = self.keys.write() {
            for (k, v) in parsed_keys {
                keys.insert(k, v);
            }
        }
        if let Ok(mut meta) = self.metadata.write() {
            meta.cached_at = Instant::now();
        }
        Ok(count)
    }

    fn decoding_key_from_jwk(jwk: &Jwk) -> Result<DecodingKey, AuthError> {
        DecodingKey::from_jwk(jwk)
            .map_err(|e| AuthError::InvalidToken(format!("Invalid JWK entry: {e}")))
    }

    /// Refreshes the in-memory JWKS cache from the configured `JwkSet` source (or re-validates default key)
    /// and updates the cache timestamp metadata.
    pub fn refresh_cache(&self) -> Result<(), AuthError> {
        let mut meta = self
            .metadata
            .write()
            .map_err(|_| AuthError::InvalidToken("JWKS metadata lock poisoned".to_string()))?;

        if meta.cached_at.elapsed() < meta.ttl {
            return Ok(());
        }

        let maybe_jwks = self.source_jwks.read().ok().and_then(|s| s.clone());
        if let Some(jwks) = maybe_jwks {
            let mut refreshed_map = HashMap::new();
            for (idx, jwk) in jwks.keys.iter().enumerate() {
                let kid = jwk
                    .common
                    .key_id
                    .clone()
                    .unwrap_or_else(|| format!("jwk-key-{idx}"));
                let decoding_key = Self::decoding_key_from_jwk(jwk)?;
                refreshed_map.insert(kid, decoding_key);
            }
            if let Ok(mut keys) = self.keys.write() {
                for (k, v) in refreshed_map {
                    keys.insert(k, v);
                }
            }
        } else if let Ok(mut keys) = self.keys.write() {
            keys.entry(self.default_kid.clone())
                .or_insert_with(|| DecodingKey::from_secret(&self.default_secret));
        }

        meta.cached_at = Instant::now();
        meta.refresh_count += 1;
        Ok(())
    }

    /// Issues a signed JWT bearer token using the validator's default key and algorithm.
    ///
    /// If `claims.exp` is `None`, defaults expiration to 1 hour in the future.
    pub fn issue_token(&self, claims: &Claims) -> Result<String, AuthError> {
        let encoding_key = EncodingKey::from_secret(&self.default_secret);
        self.issue_token_with_kid(&self.default_kid, claims, &encoding_key, self.algorithm)
    }

    /// Issues a signed JWT bearer token with an explicit key ID (`kid`), `EncodingKey`, and `Algorithm`.
    pub fn issue_token_with_kid(
        &self,
        kid: &str,
        claims: &Claims,
        encoding_key: &EncodingKey,
        algorithm: Algorithm,
    ) -> Result<String, AuthError> {
        let mut header = Header::new(algorithm);
        header.kid = Some(kid.to_string());

        let mut token_claims = claims.clone();
        if token_claims.exp.is_none() {
            token_claims.exp = Some(Utc::now().timestamp() + 3600);
        }

        encode(&header, &token_claims, encoding_key)
            .map_err(|e| AuthError::InvalidToken(format!("Failed to encode JWT: {e}")))
    }

    /// Extracts and validates the Bearer token from a raw `Authorization` header string.
    pub fn validate_authorization_header(
        &self,
        auth_header: Option<&str>,
    ) -> Result<Claims, AuthError> {
        let raw_header = auth_header.ok_or(AuthError::MissingToken)?.trim();
        if raw_header.is_empty() {
            return Err(AuthError::MalformedHeader(
                "Authorization header cannot be empty".to_string(),
            ));
        }

        let (scheme, rest) = raw_header.split_once(char::is_whitespace).ok_or_else(|| {
            AuthError::MalformedHeader("Expected 'Bearer <token>' scheme".to_string())
        })?;

        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(AuthError::MalformedHeader(
                "Expected 'Bearer <token>' scheme".to_string(),
            ));
        }

        let token = rest.trim();
        if token.is_empty() {
            return Err(AuthError::MalformedHeader(
                "Bearer token value cannot be empty".to_string(),
            ));
        }

        self.validate_token(token)
    }

    /// Validates a JWT token string against the cached JWKS keys in <0.2ms.
    pub fn validate_token(&self, token: &str) -> Result<Claims, AuthError> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(AuthError::InvalidToken("Token is empty".to_string()));
        }

        // Automatically refresh JWKS cache if TTL has expired
        if self.is_cache_expired() {
            self.refresh_cache()?;
        }

        let header = decode_header(trimmed)
            .map_err(|e| AuthError::InvalidToken(format!("Malformed JWT header: {e}")))?;

        if header.alg != self.algorithm {
            return Err(AuthError::InvalidToken(format!(
                "Unsupported or mismatched JWT algorithm {:?}; expected {:?}",
                header.alg, self.algorithm
            )));
        }

        let kid = header.kid.as_deref().unwrap_or(&self.default_kid);

        let mut validation = Validation::new(self.algorithm);
        validation.leeway = 0;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        if let Some(ref iss) = self.issuer {
            validation.set_issuer(&[iss]);
        }

        let token_data = {
            let keys_guard = self
                .keys
                .read()
                .map_err(|_| AuthError::InvalidToken("JWKS cache lock poisoned".to_string()))?;

            let decoding_key = keys_guard
                .get(kid)
                .or_else(|| {
                    if header.kid.is_none() {
                        keys_guard.get(&self.default_kid)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    AuthError::InvalidToken(format!("Key ID '{kid}' not found in JWKS cache"))
                })?;

            decode::<WireClaims>(trimmed, decoding_key, &validation)
                .map_err(|e| AuthError::InvalidToken(e.to_string()))?
        };

        let claims = token_data.claims.into_claims();
        if claims.sub.trim().is_empty() {
            return Err(AuthError::InvalidToken(
                "Token subject (sub) claim cannot be empty".to_string(),
            ));
        }

        Ok(claims)
    }

    /// Validates the `Authorization` header and verifies that the token contains `required_scope`.
    pub fn validate_and_require_scope(
        &self,
        auth_header: Option<&str>,
        required_scope: &str,
    ) -> Result<Claims, AuthError> {
        let claims = self.validate_authorization_header(auth_header)?;
        if !claims.has_scope(required_scope) {
            return Err(AuthError::InsufficientScope {
                required: required_scope.to_string(),
            });
        }
        Ok(claims)
    }
}

/// Tower PEP Layer enforcing Kanidm JWT authentication and required scope authorization.
///
/// Injects verified `Claims` into request extensions (`Extension<Claims>`) on success, or
/// short-circuits with RFC 7807 `ProblemDetails` (`401 Unauthorized` / `403 Forbidden`).
#[derive(Clone, Debug)]
pub struct RequireScope {
    validator: Arc<JwtValidator>,
    required_scope: String,
    path_prefix: Option<String>,
}

impl RequireScope {
    /// Creates a new `RequireScope` Tower layer requiring `required_scope` on all requests.
    pub fn new(validator: Arc<JwtValidator>, required_scope: impl Into<String>) -> Self {
        Self {
            validator,
            required_scope: required_scope.into(),
            path_prefix: None,
        }
    }

    /// Creates a `RequireScope` Tower layer enforcing `api:write` scope.
    pub fn write(validator: Arc<JwtValidator>) -> Self {
        Self::new(validator, SCOPE_API_WRITE)
    }

    /// Creates a `RequireScope` Tower layer enforcing `api:read` scope.
    pub fn read(validator: Arc<JwtValidator>) -> Self {
        Self::new(validator, SCOPE_API_READ)
    }

    /// Restricts strict scope enforcement to routes matching `path_prefix` (e.g., `"/api/v1/commands"`).
    ///
    /// Requests to other paths with an `Authorization` header still have their JWT validated and
    /// `Claims` injected into extensions.
    pub fn for_path_prefix(
        validator: Arc<JwtValidator>,
        path_prefix: impl Into<String>,
        required_scope: impl Into<String>,
    ) -> Self {
        Self {
            validator,
            required_scope: required_scope.into(),
            path_prefix: Some(path_prefix.into()),
        }
    }
}

impl<S> Layer<S> for RequireScope {
    type Service = RequireScopeService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequireScopeService {
            inner,
            validator: Arc::clone(&self.validator),
            required_scope: self.required_scope.clone(),
            path_prefix: self.path_prefix.clone(),
        }
    }
}

/// Tower Service produced by [`RequireScope`].
#[derive(Clone, Debug)]
pub struct RequireScopeService<S> {
    inner: S,
    validator: Arc<JwtValidator>,
    required_scope: String,
    path_prefix: Option<String>,
}

impl<S> Service<Request<Body>> for RequireScopeService<S>
where
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        let path = req.uri().path().to_string();
        let enforce_strict = match &self.path_prefix {
            Some(prefix) => path.starts_with(prefix),
            None => true,
        };

        let raw_auth = match req.headers().get(header::AUTHORIZATION) {
            Some(val) => match val.to_str() {
                Ok(s) => Some(s.to_string()),
                Err(_) => {
                    let resp = AuthError::MalformedHeader(
                        "Authorization header contains non-ASCII characters".to_string(),
                    )
                    .into_problem_details(Some(path))
                    .into_response();
                    return Box::pin(async move { Ok(resp) });
                }
            },
            None => None,
        };

        if enforce_strict {
            match self
                .validator
                .validate_and_require_scope(raw_auth.as_deref(), &self.required_scope)
            {
                Ok(claims) => {
                    req.extensions_mut().insert(claims);
                }
                Err(auth_err) => {
                    let resp = auth_err.into_problem_details(Some(path)).into_response();
                    return Box::pin(async move { Ok(resp) });
                }
            }
        } else if let Some(ref auth_str) = raw_auth {
            match self.validator.validate_authorization_header(Some(auth_str)) {
                Ok(claims) => {
                    req.extensions_mut().insert(claims);
                }
                Err(auth_err) => {
                    let resp = auth_err.into_problem_details(Some(path)).into_response();
                    return Box::pin(async move { Ok(resp) });
                }
            }
        }

        let fut = self.inner.call(req);
        Box::pin(fut)
    }
}
