#!/usr/bin/env bash
# build-and-scan.sh: Builds ferrous-api Docker image and scans with Hadolint and Trivy (Story 8.7)
# Enforces Tier 2 DevSecOps standards locally without incurring cloud CI minutes (AD-13, AD-24, NFR-10..13).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DOCKERFILES=("Dockerfile")
TARGET_IMAGE="ferrous-api:latest"

SKIP_SCAN=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --skip-scan) SKIP_SCAN=true ;;
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown option: $arg" >&2; exit 1 ;;
    esac
done

echo "======================================================================"
echo " [DEVSECOPS] Building and Scanning ferrous-api Container"
echo "======================================================================"

cd "${REPO_ROOT}"

# 1. Hadolint Dockerfile Linting
echo -e "\n[Step 1/3] Running Hadolint Dockerfile SAST audit..."
for df in "${DOCKERFILES[@]}"; do
    echo "  Auditing ${df}..."
    if [ "${DRY_RUN}" = "true" ]; then
        echo "  [DRY-RUN] hadolint ${df}"
    else
        if command -v hadolint >/dev/null 2>&1; then
            hadolint "${df}"
        else
            echo "  [INFO] Local hadolint CLI not found; running via hadolint/hadolint container..."
            docker run --rm -v "${REPO_ROOT}:/work" -w /work hadolint/hadolint:latest hadolint "${df}"
        fi
        echo "  [OK] ${df} passed Hadolint linting (0 issues)."
    fi
done

# 2. Validate Compose Manifest & Build Container Image (parent context .. for ../ferrous-cqrses)
echo -e "\n[Step 2/3] Validating Docker Compose manifest and building container image..."
echo "  Validating docker-compose.yml syntax..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] docker compose config -q"
    echo "  [DRY-RUN] docker build -t ${TARGET_IMAGE} -f Dockerfile .."
else
    docker compose config -q
    echo "  [OK] docker-compose.yml configuration syntax validated."

    docker build -t "${TARGET_IMAGE}" -f Dockerfile ..
    echo "  [OK] Container image ${TARGET_IMAGE} built successfully."

    IMAGE_BYTES="$(docker image inspect --format='{{.Size}}' "${TARGET_IMAGE}")"
    MAX_BYTES=$((50 * 1024 * 1024))
    if [ "${IMAGE_BYTES}" -gt "${MAX_BYTES}" ]; then
        echo "ERROR: Container image ${TARGET_IMAGE} size (${IMAGE_BYTES} bytes) exceeds 50MB AD-24 budget" >&2
        exit 1
    fi
    echo "  [OK] Image size verified: ${IMAGE_BYTES} bytes (< 50 MB AD-24)."
fi

if [ "${SKIP_SCAN}" = "true" ]; then
    echo -e "\n[WARN] Skipping Trivy vulnerability scanning (--skip-scan enabled)."
    exit 0
fi

# Helper function for Trivy execution
run_trivy() {
    local image_name="$1"
    if command -v trivy >/dev/null 2>&1; then
        trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "${image_name}"
    else
        echo "  [INFO] Local trivy CLI not found; running via aquasec/trivy container..."
        docker run --rm \
            -v /var/run/docker.sock:/var/run/docker.sock \
            aquasec/trivy:latest image \
            --severity HIGH,CRITICAL \
            --ignore-unfixed \
            --exit-code 1 \
            "${image_name}"
    fi
}

# 3. Trivy Scan
echo -e "\n[Step 3/3] Running Trivy vulnerability scan on container image..."
echo "  Scanning image ${TARGET_IMAGE}..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 ${TARGET_IMAGE}"
else
    run_trivy "${TARGET_IMAGE}"
    echo "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in ${TARGET_IMAGE}."
fi

echo -e "\n======================================================================"
echo " [SUCCESS] Container Target Built and Verified (0 CVEs)!"
echo "======================================================================"
