<#
.SYNOPSIS
    Compiles or validates the canonical OpenAPI 3.0 specification from Goa v3 DSL.
.PARAMETER Check
    Performs contract validation and schema conformance checks without regenerating.
#>
param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectDir = Split-Path -Parent $ScriptDir
Set-Location $ProjectDir

$OpenApiFile = Join-Path $ProjectDir "gen\http\openapi3.json"
$DesignDir = Join-Path $ProjectDir "design"

Write-Host "==> [Goa v3 OpenAPI Generator] Working Directory: $ProjectDir"

# Validate that Goa DSL files exist
$RequiredDesignFiles = @("design.go", "orders.go", "queries.go", "streams.go")
foreach ($file in $RequiredDesignFiles) {
    $filePath = Join-Path $DesignDir $file
    if (-not (Test-Path $filePath)) {
        Write-Error "Required Goa DSL file missing: $filePath"
        exit 1
    }
}

function Validate-OpenApiSpec {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        Write-Error "OpenAPI specification not found at: $Path"
        exit 1
    }

    try {
        $content = Get-Content -Raw -Path $Path -Encoding UTF8
        $spec = $content | ConvertFrom-Json
    } catch {
        Write-Error "OpenAPI specification is not valid JSON: $_"
        exit 1
    }

    if (-not $spec.openapi -or -not $spec.openapi.StartsWith("3.0")) {
        Write-Error "Invalid or missing openapi version (expected 3.0.x): $($spec.openapi)"
        exit 1
    }

    if (-not $spec.info -or $spec.info.title -ne "FerrousCX Contract-First Axum API Service") {
        Write-Error "Invalid or missing info.title: $($spec.info.title)"
        exit 1
    }

    $requiredPaths = @(
        "/api/v1/commands/orders/issue-credit",
        "/api/v1/commands/orders/cancel",
        "/api/v1/orders/{order_id}",
        "/api/v1/orders",
        "/api/v1/events/stream",
        "/api/v1/benchmarks/{run_id}/stream",
        "/api/v1/telemetry/pipeline/stream"
    )

    foreach ($p in $requiredPaths) {
        if (-not $spec.paths.PSObject.Properties[$p]) {
            Write-Error "Missing required API path: $p"
            exit 1
        }
    }

    $requiredSchemas = @(
        "IssueCreditRequest",
        "CancelOrderRequest",
        "CommandAcceptedResponse",
        "ProblemDetails",
        "InvalidParam",
        "OrderItem",
        "OrderDetails",
        "OrderSummary",
        "OrderListResponse",
        "DomainEventStreamItem",
        "BenchmarkTelemetryItem",
        "PipelineTelemetryItem"
    )

    foreach ($s in $requiredSchemas) {
        if (-not $spec.components.schemas.PSObject.Properties[$s]) {
            Write-Error "Missing required component schema: $s"
            exit 1
        }
    }

    if (-not $spec.components.securitySchemes.jwt_bearer) {
        Write-Error "Missing required security scheme: jwt_bearer"
        exit 1
    }

    Write-Host "[OK] OpenAPI specification passes schema validation: $Path"
}

if ($Check) {
    Write-Host "==> Checking canonical OpenAPI 3.0 specification conformance..."
    Validate-OpenApiSpec -Path $OpenApiFile
    exit 0
}

# Generation workflow with fallback
$hasGo = [bool](Get-Command go -ErrorAction SilentlyContinue)
$hasGoa = [bool](Get-Command goa -ErrorAction SilentlyContinue)

if ($hasGo -and $hasGoa) {
    Write-Host "==> Executing 'goa gen ferrous-api/design -o gen'..."
    goa gen ferrous-api/design -o gen
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Goa code generation failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
} else {
    Write-Host "[INFO] Go and/or Goa CLI not installed in current environment."
    Write-Host "[INFO] Validating existing pre-compiled canonical specification fallback..."
}

Validate-OpenApiSpec -Path $OpenApiFile
Write-Host "==> OpenAPI generation / verification complete."
exit 0
