#!/usr/bin/env bash
set -euo pipefail

# SYNOPSIS:
#   Compiles or validates the canonical OpenAPI 3.0 specification from Goa v3 DSL.
# USAGE:
#   ./scripts/generate-openapi.sh [--check]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

OPENAPI_FILE="${PROJECT_DIR}/gen/http/openapi3.json"
DESIGN_DIR="${PROJECT_DIR}/design"

echo "==> [Goa v3 OpenAPI Generator] Working Directory: ${PROJECT_DIR}"

# Validate required Goa DSL files
REQUIRED_FILES=("design.go" "orders.go" "queries.go" "streams.go")
for f in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "${DESIGN_DIR}/${f}" ]]; then
        echo "Error: Required Goa DSL file missing: ${DESIGN_DIR}/${f}" >&2
        exit 1
    fi
done

validate_spec() {
    local target="$1"
    if [[ ! -f "${target}" ]]; then
        echo "Error: OpenAPI specification not found at: ${target}" >&2
        exit 1
    fi

    # Basic JSON syntax and key presence check
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "
import json, sys
with open('${target}', 'r', encoding='utf-8') as f:
    d = json.load(f)
assert d.get('openapi', '').startswith('3.0'), 'Invalid openapi version'
assert d.get('info', {}).get('title') == 'FerrousCX Contract-First Axum API Service', 'Invalid title'
required_paths = [
    '/api/v1/commands/orders/issue-credit',
    '/api/v1/commands/orders/cancel',
    '/api/v1/orders/{order_id}',
    '/api/v1/orders',
    '/api/v1/events/stream',
    '/api/v1/benchmarks/{run_id}/stream',
    '/api/v1/telemetry/pipeline/stream'
]
for p in required_paths:
    assert p in d.get('paths', {}), f'Missing path {p}'
schemas = d.get('components', {}).get('schemas', {})
for s in ['IssueCreditRequest', 'CancelOrderRequest', 'CommandAcceptedResponse', 'ProblemDetails']:
    assert s in schemas, f'Missing schema {s}'
assert 'jwt_bearer' in d.get('components', {}).get('securitySchemes', {}), 'Missing jwt_bearer'
" || { echo "Error: Schema validation failed on ${target}" >&2; exit 1; }
    elif command -v jq >/dev/null 2>&1; then
        jq -e '.openapi | startswith("3.0")' "${target}" >/dev/null
        jq -e '.paths["/api/v1/commands/orders/issue-credit"]' "${target}" >/dev/null
    else
        grep -q '"openapi": "3.0' "${target}" || { echo "Error: Invalid openapi string" >&2; exit 1; }
    fi

    echo "[OK] OpenAPI specification passes schema validation: ${target}"
}

CHECK_MODE=0
for arg in "$@"; do
    case "$arg" in
        --check|-Check|-c)
            CHECK_MODE=1
            ;;
    esac
done

if [[ "${CHECK_MODE}" -eq 1 ]]; then
    echo "==> Checking canonical OpenAPI 3.0 specification conformance..."
    validate_spec "${OPENAPI_FILE}"
    exit 0
fi

if command -v goa >/dev/null 2>&1; then
    echo "==> Executing 'goa gen ferrous-api/design -o gen'..."
    goa gen ferrous-api/design -o gen
else
    echo "[INFO] Goa CLI not detected in PATH."
    echo "[INFO] Validating existing pre-compiled canonical specification fallback..."
fi

validate_spec "${OPENAPI_FILE}"
echo "==> OpenAPI generation / verification complete."
exit 0
