# ferrous-api

[← Back to FerrousCX Root Architecture](https://github.com/reidlai/ferrous-cx)

> **Contract-First Rust/Axum API Service for FerrousCX Platform**  
> Driven by [Goa Design v3](https://goa.design/) as the declarative API specification DSL, generating OpenAPI v3 specifications, compiling them into strongly typed Rust traits via `rust-axum` OpenAPI generators, and streaming real-time events to the browser client via native **Axum SSE**.

---

## 🎯 Architectural Role

`ferrous-api` is the unified contract-first HTTP API Service and real-time streaming hub for FerrousCX:
1. **Declarative Contract Modeling (`design/`):** API routes, request/response models, validation constraints, and Kanidm JWT security scopes are declared in Goa Design DSL (`design/design.go`).
2. **OpenAPI v3 Generation (`gen/openapi/`):** Goa compiles the DSL into a canonical OpenAPI v3 specification.
3. **Rust-Axum Codegen (`src/generated/`):** `openapi-generator -g rust-axum` compiles the OpenAPI specification into strongly typed Rust traits, request decoders, and data transfer objects.
4. **Native Axum Runtime (`src/handlers/`):** Handlers implement the generated Rust traits in pure Rust (`axum` 0.7+, `tokio` 1.40+).
5. **Real-Time Browser Sync (Axum SSE):** Exposes Server-Sent Events (`axum::response::sse::Sse`) with automatic keep-alive and cooperative cancellation (`CancellationToken`) when Svelte 5 clients abort or switch context.
6. **In-Process Analytical Querying:** Directly embeds `Apache DataFusion` (`Arc<SessionContext>`) to scan Delta Lake Parquet tables in **<3ms** with zero IPC overhead.

```
┌───────────────────────────────────────────────┐
│     design/design.go (Goa v3 Design DSL)      │
└───────────────────────┬───────────────────────┘
                        │ goa gen
                        ▼
┌───────────────────────────────────────────────┐
│   gen/openapi/openapi3.json (OpenAPI v3)      │
└───────────────────────┬───────────────────────┘
                        │ openapi-generator -g rust-axum
                        ▼
┌───────────────────────────────────────────────┐
│  src/generated/ (Rust Models & Axum Traits)   │
└───────────────────────┬───────────────────────┘
                        │ implements traits
                        ▼
┌───────────────────────────────────────────────┐
│      src/handlers/ (Pure Rust / Axum)         │
│  • In-Process Apache DataFusion (<3ms SQL)    │
│  • Direct Apache Iggy fsync (<1ms WAL)        │
│  • Kanidm JWT Signature Verification (<0.2ms) │
│  • Native Axum SSE (Browser Sync)             │
└───────────────────────┬───────────────────────┘
                        │ Server-Sent Events / HTTP
                        ▼
┌───────────────────────────────────────────────┐
│   ferrous-frontend (Svelte 5 + RxJS Runes)    │
└───────────────────────────────────────────────┘
```

---

## 🚀 Key Responsibilities

| Domain Service | Description | Protocol / Backend |
| :--- | :--- | :--- |
| **Authentication & Authorization (PEP)** | Operates strictly as Policy Enforcement Point (PEP) for Kanidm (`ferrous-iam`, the authoritative PAP/PDP). Intercepts requests via Axum Tower middleware, validates JWT signatures against cached JWKS (<0.2ms), and enforces authorities, roles, and policies defined and configured exclusively in Kanidm. | HTTP Bearer, JWKS from `ferrous-iam` |
| **CQRS-ES Command Ingress** | Dispatches command mutations (`IssueCredit`, `CancelOrder`) to `ferrous-cqrses` and Apache Iggy `orders/intake` WAL, returning HTTP 202 upon `fsync` ACK. | Direct TCP / Iggy Wire Protocol |
| **Axum SSE Streaming** | Streams live ticket updates, agent thoughts, and SLA breach warnings directly to `ferrous-frontend` via native Axum SSE with `CancellationToken` cancellation. | Server-Sent Events (`axum::response::sse::Sse`) |
| **Historical Analytics Ingress** | In-process Apache DataFusion (`Arc<SessionContext>`) scanning Delta Lake historical partitions in <3ms. | In-Process Vectorized SQL |
| **Replay Orchestration** | Triggers, pauses, and monitors historical dataset replay sessions managed by `stream-replay-rs`. | HTTP Control API |

---

## 📁 Repository Layout

```
ferrous-api/
├── .github/workflows/
│   └── devsecops-ci.yml        # Tier 1 Rust CI (clippy, cargo-audit, test)
├── .pre-commit-config.yaml      # Pre-commit hooks (cargo fmt, gitleaks)
├── design/                      # Goa v3 API Design DSL (Single Source of Truth)
│   ├── design.go                # API root, security schemes & middleware
│   ├── orders.go                # Orders, credits, and dispute command endpoints
│   ├── queries.go               # Analytical query endpoints (DataFusion lakehouse)
│   └── streams.go               # SSE stream definitions
├── gen/                         # Generated OpenAPI Artifacts
│   └── openapi/
│       └── openapi3.json        # Compiled OpenAPI v3 specification
├── scripts/
│   ├── generate-openapi.sh      # Executes 'goa gen' to produce openapi3.json
│   └── generate-axum.sh         # Executes 'openapi-generator -g rust-axum'
├── src/
│   ├── generated/               # Generated Rust models, traits, and router (DO NOT EDIT)
│   │   ├── apis/                # Generated Axum API server traits
│   │   ├── models/              # Generated Serde payload structs
│   │   └── mod.rs
│   ├── handlers/                # Domain implementations of generated Axum traits
│   │   ├── orders.rs            # Implements OrdersApi (Iggy WAL dispatch)
│   │   ├── queries.rs           # Implements QueriesApi (DataFusion scans)
│   │   └── sse.rs               # Axum SSE streaming implementations
│   ├── state.rs                 # Shared AppState (Arc<SessionContext>, IggyClient)
│   ├── auth.rs                  # Kanidm JWT validation middleware & JWKS cache
│   └── main.rs                  # Axum router setup & server bootstrap (:8080)
├── Cargo.toml                   # Axum, Tokio, DataFusion, Serde, JsonWebToken
├── Dockerfile                   # Multi-stage optimized Rust build (distroless cc-debian12)
└── README.md                    # Submodule specification & quickstart
```

---

## 🛠️ Developer Workflow

### 1. Update API Contract
Edit declarative endpoints, schemas, or security scopes in `design/`:
```go
// design/orders.go
var _ = Service("orders", func() {
    Method("issue_credit", func() {
        Payload(IssueCreditPayload)
        Result(IssueCreditResult)
        HTTP(func() {
            POST("/api/v1/commands/orders/issue-credit")
            Response(StatusAccepted)
        })
    })
})
```

### 2. Generate OpenAPI Spec & Axum Traits
```bash
# 1. Generate OpenAPI v3 JSON using Goa
goa gen ferrous-api/design -o gen/

# 2. Generate Axum traits and models using openapi-generator
openapi-generator-cli generate \
    -i gen/http/openapi3.json \
    -g rust-axum \
    -o src/generated
```

### 3. Implement Handlers in Pure Rust
Implement the generated server traits in `src/handlers/`:
```rust
#[async_trait]
impl OrdersApi for OrdersHandler {
    async fn issue_credit(
        &self,
        claims: Claims,
        body: IssueCreditPayload,
    ) -> Result<IssueCreditResponse, String> {
        // Dispatches command to Apache Iggy WAL in <1ms
        self.state.iggy_client.send_command(&body).await?;
        Ok(IssueCreditResponse::Accepted)
    }
}
```

### 4. Run & Test Locally
```bash
cargo run --bin ferrous-api
```
- **HTTP & SSE Service Endpoint**: `http://localhost:8080`
- **Health Check**: `http://localhost:8080/healthz`

---

## 🛡️ DevSecOps & Quality Assurance (Tier 1 Rust)

`ferrous-api` adheres to strict Rust engineering and security baselines enforced via GitHub Actions:
- **Contract Drift Gate:** CI executes `goa gen` and asserts `git diff --exit-code gen/openapi/openapi3.json` to verify checked-in OpenAPI specs and generated Rust traits match the design DSL.
- **Linting:** `cargo clippy -- -D warnings` enforcing zero warnings.
- **Vulnerability Audit:** `cargo audit` and `cargo deny check` scanning crate dependencies.
- **Testing:** `cargo test --all-targets` enforcing full unit coverage on handlers and SSE cancel semantics.
