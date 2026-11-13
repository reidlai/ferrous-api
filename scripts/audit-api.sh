#!/usr/bin/env bash
# ==============================================================================
# ferrous-api Tier 1 DevSecOps Local Audit & Scan Gate (POSIX/Bash)
# Enforces AD-13, NFR-10..12 shift-left standards and verification in < 3 minutes
# ==============================================================================
set -euo pipefail
export RUSTFLAGS="-D warnings"
SKIPPED_CHECKS=()

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${CRATE_DIR}"

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}======================================================================${NC}"
echo -e "${CYAN} [Tier 1 Security Gate] ferrous-api: Code Quality & Security Scan     ${NC}"
echo -e "${CYAN}======================================================================${NC}"

# 1. Format Gate
echo -e "${YELLOW}--> [1/6] Checking code formatting (cargo fmt)...${NC}"
if ! cargo fmt --all -- --check; then
    echo -e "${RED}ERROR: cargo fmt failed. Please run 'cargo fmt --all' to reformat code.${NC}" >&2
    exit 1
fi

# 2. Clippy Linter Gate
echo -e "${YELLOW}--> [2/6] Running static analysis (cargo clippy)...${NC}"
if ! cargo clippy --all-targets -- -D warnings; then
    echo -e "${RED}ERROR: cargo clippy detected warnings or errors. Build halted.${NC}" >&2
    exit 1
fi

# 3. Unit & Integration Test Gate
echo -e "${YELLOW}--> [3/6] Running unit and integration tests (cargo test)...${NC}"
if ! cargo test --all-targets; then
    echo -e "${RED}ERROR: cargo test failed. Fix failing tests before committing.${NC}" >&2
    exit 1
fi

# 4. Dependency Vulnerability Audit
echo -e "${YELLOW}--> [4/6] Checking dependency vulnerabilities (cargo audit)...${NC}"
if command -v cargo-audit >/dev/null 2>&1; then
    if ! cargo audit; then
        echo -e "${RED}ERROR: cargo audit found security vulnerabilities in dependencies.${NC}" >&2
        exit 1
    fi
else
    echo -e "${YELLOW}    [WARN] cargo-audit is not installed. Run 'cargo install cargo-audit' to enable.${NC}"
    SKIPPED_CHECKS+=("cargo-audit")
fi

# 5. Supply-Chain & License Assurance
echo -e "${YELLOW}--> [5/6] Checking license and supply chain compliance (cargo deny)...${NC}"
if command -v cargo-deny >/dev/null 2>&1; then
    if ! cargo deny check; then
        echo -e "${RED}ERROR: cargo deny detected banned licenses or advisories.${NC}" >&2
        exit 1
    fi
else
    echo -e "${YELLOW}    [WARN] cargo-deny is not installed. Run 'cargo install cargo-deny' to enable.${NC}"
    SKIPPED_CHECKS+=("cargo-deny")
fi

# 6. Static Analysis / Semgrep Scan
echo -e "${YELLOW}--> [6/6] Running static application security testing (semgrep)...${NC}"
if command -v semgrep >/dev/null 2>&1; then
    if ! semgrep scan --config auto --error --quiet .; then
        echo -e "${RED}ERROR: semgrep detected security issues or anti-patterns.${NC}" >&2
        exit 1
    fi
else
    echo -e "${YELLOW}    [WARN] semgrep is not installed. Skipping local semgrep scan.${NC}"
    SKIPPED_CHECKS+=("semgrep")
fi

echo -e "${GREEN}======================================================================${NC}"
if [ "${#SKIPPED_CHECKS[@]}" -gt 0 ]; then
    echo -e "${YELLOW} [PASS WITH WARNINGS] ferrous-api passed executed gates; skipped: ${SKIPPED_CHECKS[*]}${NC}"
else
    echo -e "${GREEN} [SUCCESS] ferrous-api passed all Tier 1 DevSecOps quality gates!     ${NC}"
fi
echo -e "${GREEN}======================================================================${NC}"
exit 0
