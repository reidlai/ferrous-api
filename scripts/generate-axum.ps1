<#
.SYNOPSIS
    Generates or verifies strongly typed Rust server traits and models from OpenAPI 3.0.
.PARAMETER Check
    Performs contract and file conformance checks without regenerating.
#>
param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectDir = Split-Path -Parent $ScriptDir
Set-Location $ProjectDir

$OpenApiFile = Join-Path $ProjectDir "gen\http\openapi3.json"
$GeneratedDir = Join-Path $ProjectDir "src\generated"

Write-Host "==> [Rust-Axum Trait Generator] Working Directory: $ProjectDir"

if (-not (Test-Path $OpenApiFile)) {
    Write-Error "OpenAPI 3.0 specification not found at: $OpenApiFile"
    exit 1
}

function Validate-GeneratedRustFiles {
    param([string]$Path)

    $requiredFiles = @("mod.rs", "models.rs", "apis.rs")
    foreach ($f in $requiredFiles) {
        $fullPath = Join-Path $Path $f
        if (-not (Test-Path $fullPath)) {
            Write-Error "Required generated Rust source file missing: $fullPath"
            exit 1
        }
    }

    # Verify traits are declared
    $apisContent = Get-Content -Raw -Path (Join-Path $Path "apis.rs") -Encoding UTF8
    $requiredTraits = @("pub trait OrdersApi", "pub trait QueriesApi", "pub trait StreamsApi")
    foreach ($t in $requiredTraits) {
        if (-not $apisContent.Contains($t)) {
            Write-Error "Generated apis.rs missing trait: $t"
            exit 1
        }
    }

    # Verify models are declared
    $modelsContent = Get-Content -Raw -Path (Join-Path $Path "models.rs") -Encoding UTF8
    $requiredStructs = @(
        "pub struct ProblemDetails",
        "pub struct CommandAcceptedResponse",
        "pub struct IssueCreditRequest",
        "pub struct CancelOrderRequest",
        "pub struct OrderDetails",
        "pub struct OrderListResponse",
        "pub struct DomainEventStreamItem",
        "pub struct BenchmarkTelemetryItem",
        "pub struct PipelineTelemetryItem"
    )
    foreach ($s in $requiredStructs) {
        if (-not $modelsContent.Contains($s)) {
            Write-Error "Generated models.rs missing struct: $s"
            exit 1
        }
    }

    Write-Host "[OK] Rust-Axum generated contracts pass validation: $Path"
}

if ($Check) {
    Write-Host "==> Verifying Rust-Axum server traits and models..."
    Validate-GeneratedRustFiles -Path $GeneratedDir
    exit 0
}

$hasGen = [bool](Get-Command openapi-generator-cli -ErrorAction SilentlyContinue)
if ($hasGen) {
    Write-Host "==> Executing openapi-generator-cli for rust-axum..."
    openapi-generator-cli generate -i $OpenApiFile -g rust-axum -o $GeneratedDir
} else {
    Write-Host "[INFO] openapi-generator-cli not detected in PATH."
    Write-Host "[INFO] Validating existing generated server traits and models..."
}

Validate-GeneratedRustFiles -Path $GeneratedDir
Write-Host "==> Rust-Axum generation / verification complete."
exit 0
