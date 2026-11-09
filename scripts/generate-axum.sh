#!/usr/bin/env bash
set -euo pipefail

# SYNOPSIS:
#   Generates or verifies strongly typed Rust server traits and models from OpenAPI 3.0.
# USAGE:
#   ./scripts/generate-axum.sh [--check]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

OPENAPI_FILE="${PROJECT_DIR}/gen/http/openapi3.json"
GENERATED_DIR="${PROJECT_DIR}/src/generated"

echo "==> [Rust-Axum Trait Generator] Working Directory: ${PROJECT_DIR}"

if [[ ! -f "${OPENAPI_FILE}" ]]; then
    echo "Error: OpenAPI 3.0 specification not found at: ${OPENAPI_FILE}" >&2
    exit 1
fi

validate_rust_files() {
    local target="$1"
    local required_files=("mod.rs" "models.rs" "apis.rs")
    for f in "${required_files[@]}"; do
        if [[ ! -f "${target}/${f}" ]]; then
            echo "Error: Required generated Rust source file missing: ${target}/${f}" >&2
            exit 1
        fi
    done

    grep -q "pub trait OrdersApi" "${target}/apis.rs" || { echo "Error: Missing OrdersApi" >&2; exit 1; }
    grep -q "pub trait QueriesApi" "${target}/apis.rs" || { echo "Error: Missing QueriesApi" >&2; exit 1; }
    grep -q "pub trait StreamsApi" "${target}/apis.rs" || { echo "Error: Missing StreamsApi" >&2; exit 1; }

    grep -q "pub struct ProblemDetails" "${target}/models.rs" || { echo "Error: Missing ProblemDetails" >&2; exit 1; }
    grep -q "pub struct CommandAcceptedResponse" "${target}/models.rs" || { echo "Error: Missing CommandAcceptedResponse" >&2; exit 1; }
    grep -q "pub struct IssueCreditRequest" "${target}/models.rs" || { echo "Error: Missing IssueCreditRequest" >&2; exit 1; }
    grep -q "pub struct CancelOrderRequest" "${target}/models.rs" || { echo "Error: Missing CancelOrderRequest" >&2; exit 1; }
    grep -q "pub struct OrderDetails" "${target}/models.rs" || { echo "Error: Missing OrderDetails" >&2; exit 1; }

    echo "[OK] Rust-Axum generated contracts pass validation: ${target}"
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
    echo "==> Verifying Rust-Axum server traits and models..."
    validate_rust_files "${GENERATED_DIR}"
    exit 0
fi

if command -v openapi-generator-cli >/dev/null 2>&1; then
    echo "==> Executing openapi-generator-cli for rust-axum..."
    openapi-generator-cli generate -i "${OPENAPI_FILE}" -g rust-axum -o "${GENERATED_DIR}"
else
    echo "[INFO] openapi-generator-cli not detected in PATH."
    echo "[INFO] Validating existing generated server traits and models..."
fi

validate_rust_files "${GENERATED_DIR}"
echo "==> Rust-Axum generation / verification complete."
exit 0
